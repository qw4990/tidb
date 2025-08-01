// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtsummary

import (
	"bytes"
	"cmp"
	"container/list"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ppcpuusage"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
)

// StmtDigestKeyPool is the pool for StmtDigestKey.
var StmtDigestKeyPool = sync.Pool{
	New: func() any {
		return &StmtDigestKey{}
	},
}

// StmtDigestKey defines key for stmtSummaryByDigestMap.summaryMap.
type StmtDigestKey struct {
	// `hash` is the hash value of this object.
	hash []byte
}

// Init initialize the hash key.
func (key *StmtDigestKey) Init(schemaName, digest, prevDigest, planDigest, resourceGroupName string) {
	length := len(schemaName) + len(digest) + len(prevDigest) + len(planDigest) + len(resourceGroupName)
	if cap(key.hash) < length {
		key.hash = make([]byte, 0, length)
	} else {
		key.hash = key.hash[:0]
	}
	key.hash = append(key.hash, hack.Slice(digest)...)
	key.hash = append(key.hash, hack.Slice(schemaName)...)
	key.hash = append(key.hash, hack.Slice(prevDigest)...)
	key.hash = append(key.hash, hack.Slice(planDigest)...)
	key.hash = append(key.hash, hack.Slice(resourceGroupName)...)
}

// Hash implements SimpleLRUCache.Key.
// Only when current SQL is `commit` do we record `prevSQL`. Otherwise, `prevSQL` is empty.
// `prevSQL` is included in the key To distinguish different transactions.
func (key *StmtDigestKey) Hash() []byte {
	return key.hash
}

// stmtSummaryByDigestMap is a LRU cache that stores statement summaries.
type stmtSummaryByDigestMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *kvcache.SimpleLRUCache
	// beginTimeForCurInterval is the begin time for current summary.
	beginTimeForCurInterval int64

	// These options are set by global system variables and are accessed concurrently.
	optEnabled             *atomic2.Bool
	optEnableInternalQuery *atomic2.Bool
	optHistoryEnabled      *atomic2.Bool
	optMaxStmtCount        *atomic2.Uint32
	optRefreshInterval     *atomic2.Int64
	optHistorySize         *atomic2.Int32
	optMaxSQLLength        *atomic2.Int32

	// other stores summary of evicted data.
	other *stmtSummaryByDigestEvicted
}

// StmtSummaryByDigestMap is a global map containing all statement summaries.
var StmtSummaryByDigestMap = newStmtSummaryByDigestMap()

// stmtSummaryByDigest is the summary for each type of statements.
type stmtSummaryByDigest struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	// Mutex is only used to lock `history`.
	sync.Mutex
	initialized bool
	cumulative  stmtSummaryStats
	// Each element in history is a summary in one interval.
	history *list.List
	// Following fields are common for each summary element.
	// They won't change once this object is created, so locking is not needed.
	schemaName    string
	digest        string
	planDigest    string
	stmtType      string
	normalizedSQL string
	tableNames    string
	isInternal    bool
	bindingSQL    string
	bindingDigest string
}

// stmtSummaryByDigestElement is the summary for each type of statements in current interval.
type stmtSummaryByDigestElement struct {
	sync.Mutex
	// Each summary is summarized between [beginTime, endTime).
	beginTime int64
	endTime   int64
	stmtSummaryStats
}

// stmtSummaryStats is the collection of statistics tracked for each statement summary,
// both cumulatively and for each interval.
type stmtSummaryStats struct {
	// basic
	sampleSQL        string
	charset          string
	collation        string
	prevSQL          string
	samplePlan       string
	sampleBinaryPlan string
	planHint         string
	indexNames       []string
	execCount        int64
	sumErrors        int
	sumWarnings      int
	// latency
	sumLatency        time.Duration
	maxLatency        time.Duration
	minLatency        time.Duration
	sumParseLatency   time.Duration
	maxParseLatency   time.Duration
	sumCompileLatency time.Duration
	maxCompileLatency time.Duration
	// coprocessor
	sumNumCopTasks       int64
	sumCopProcessTime    time.Duration
	maxCopProcessTime    time.Duration
	maxCopProcessAddress string
	sumCopWaitTime       time.Duration
	maxCopWaitTime       time.Duration
	maxCopWaitAddress    string
	// TiKV
	sumProcessTime               time.Duration
	maxProcessTime               time.Duration
	sumWaitTime                  time.Duration
	maxWaitTime                  time.Duration
	sumBackoffTime               time.Duration
	maxBackoffTime               time.Duration
	sumTotalKeys                 int64
	maxTotalKeys                 int64
	sumProcessedKeys             int64
	maxProcessedKeys             int64
	sumRocksdbDeleteSkippedCount uint64
	maxRocksdbDeleteSkippedCount uint64
	sumRocksdbKeySkippedCount    uint64
	maxRocksdbKeySkippedCount    uint64
	sumRocksdbBlockCacheHitCount uint64
	maxRocksdbBlockCacheHitCount uint64
	sumRocksdbBlockReadCount     uint64
	maxRocksdbBlockReadCount     uint64
	sumRocksdbBlockReadByte      uint64
	maxRocksdbBlockReadByte      uint64
	// txn
	commitCount          int64
	sumGetCommitTsTime   time.Duration
	maxGetCommitTsTime   time.Duration
	sumPrewriteTime      time.Duration
	maxPrewriteTime      time.Duration
	sumCommitTime        time.Duration
	maxCommitTime        time.Duration
	sumLocalLatchTime    time.Duration
	maxLocalLatchTime    time.Duration
	sumCommitBackoffTime int64
	maxCommitBackoffTime int64
	sumResolveLockTime   int64
	maxResolveLockTime   int64
	sumWriteKeys         int64
	maxWriteKeys         int
	sumWriteSize         int64
	maxWriteSize         int
	sumPrewriteRegionNum int64
	maxPrewriteRegionNum int32
	sumTxnRetry          int64
	maxTxnRetry          int
	sumBackoffTimes      int64
	backoffTypes         map[string]int
	authUsers            map[string]struct{}
	// other
	sumMem               int64
	maxMem               int64
	sumDisk              int64
	maxDisk              int64
	sumAffectedRows      uint64
	sumKVTotal           time.Duration
	sumPDTotal           time.Duration
	sumBackoffTotal      time.Duration
	sumWriteSQLRespTotal time.Duration
	sumTidbCPU           time.Duration
	sumTikvCPU           time.Duration
	sumResultRows        int64
	maxResultRows        int64
	minResultRows        int64
	prepared             bool
	// The first time this type of SQL executes.
	firstSeen time.Time
	// The last time this type of SQL executes.
	lastSeen time.Time
	// plan cache
	planInCache   bool
	planCacheHits int64
	planInBinding bool
	// pessimistic execution retry information.
	execRetryCount uint
	execRetryTime  time.Duration
	// request-units
	resourceGroupName string
	StmtRUSummary
	StmtNetworkTrafficSummary

	planCacheUnqualifiedCount int64
	lastPlanCacheUnqualified  string // the reason why this query is unqualified for the plan cache

	storageKV  bool // query read from TiKV
	storageMPP bool // query read from TiFlash
}

// StmtExecInfo records execution information of each statement.
type StmtExecInfo struct {
	SchemaName     string
	Charset        string
	Collation      string
	NormalizedSQL  string
	Digest         string
	PrevSQL        string
	PrevSQLDigest  string
	PlanDigest     string
	User           string
	TotalLatency   time.Duration
	ParseLatency   time.Duration
	CompileLatency time.Duration
	StmtCtx        *stmtctx.StatementContext
	CopTasks       *execdetails.CopTasksSummary
	ExecDetail     execdetails.ExecDetails
	MemMax         int64
	DiskMax        int64
	StartTime      time.Time
	IsInternal     bool
	Succeed        bool
	PlanInCache    bool
	PlanInBinding  bool
	ExecRetryCount uint
	ExecRetryTime  time.Duration
	execdetails.StmtExecDetails
	ResultRows        int64
	TiKVExecDetails   *util.ExecDetails
	Prepared          bool
	KeyspaceName      string
	KeyspaceID        uint32
	ResourceGroupName string
	RUDetail          *util.RUDetails
	CPUUsages         ppcpuusage.CPUUsages

	PlanCacheUnqualified string

	LazyInfo StmtExecLazyInfo
}

// StmtExecLazyInfo is the interface about getting lazy information for StmtExecInfo.
type StmtExecLazyInfo interface {
	GetOriginalSQL() string
	GetEncodedPlan() (string, string, any)
	GetBinaryPlan() string
	GetPlanDigest() string
	GetBindingSQLAndDigest() (string, string)
}

// newStmtSummaryByDigestMap creates an empty stmtSummaryByDigestMap.
func newStmtSummaryByDigestMap() *stmtSummaryByDigestMap {
	ssbde := newStmtSummaryByDigestEvicted()

	// This initializes the stmtSummaryByDigestMap with "compiled defaults"
	// (which are regrettably duplicated from sessionctx/variable/tidb_vars.go).
	// Unfortunately we need to do this to avoid circular dependencies, but the correct
	// values will be applied on startup as soon as domain.LoadSysVarCacheLoop() is called,
	// which in turn calls func domain.checkEnableServerGlobalVar(name, sVal string) for each sysvar.
	// Currently this is early enough in the startup sequence.
	maxStmtCount := uint(3000)
	newSsMap := &stmtSummaryByDigestMap{
		summaryMap:             kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
		optMaxStmtCount:        atomic2.NewUint32(uint32(maxStmtCount)),
		optEnabled:             atomic2.NewBool(true),
		optEnableInternalQuery: atomic2.NewBool(false),
		optHistoryEnabled:      atomic2.NewBool(true),
		optRefreshInterval:     atomic2.NewInt64(1800),
		optHistorySize:         atomic2.NewInt32(24),
		optMaxSQLLength:        atomic2.NewInt32(4096),
		other:                  ssbde,
	}
	newSsMap.summaryMap.SetOnEvict(func(k kvcache.Key, v kvcache.Value) {
		historySize := newSsMap.historySize()
		newSsMap.other.AddEvicted(k.(*StmtDigestKey), v.(*stmtSummaryByDigest), historySize)
	})
	return newSsMap
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtExecInfo) {
	// All times are counted in seconds.
	now := time.Now().Unix()

	failpoint.Inject("mockTimeForStatementsSummary", func(val failpoint.Value) {
		// mockTimeForStatementsSummary takes string of Unix timestamp
		if unixTimeStr, ok := val.(string); ok {
			unixTime, err := strconv.ParseInt(unixTimeStr, 10, 64)
			if err != nil {
				panic(err.Error())
			}
			now = unixTime
		}
	})

	intervalSeconds := ssMap.refreshInterval()
	historySize := 0
	if ssMap.historyEnabled() {
		historySize = ssMap.historySize()
	}

	key := StmtDigestKeyPool.Get().(*StmtDigestKey)
	// Init hash value in advance, to reduce the time holding the lock.
	key.Init(sei.SchemaName, sei.Digest, sei.PrevSQLDigest, sei.PlanDigest, sei.ResourceGroupName)

	var exist bool

	// Using a global lock here instead of fine-grained locks because:
	// 1. The critical sections are very short, making lock overhead potentially higher
	// than the protected code execution time.
	// 2. Previous implementation with layered locks reveals significant contention and
	// poorer performance in benchmarks.
	// A single coarse-grained lock reduces overall contention and may provide better
	// performance in this specific case.
	ssMap.Lock()
	defer ssMap.Unlock()

	// Check again. Statements could be added before disabling the flag and after Clear().
	if !ssMap.Enabled() {
		return
	}
	if sei.IsInternal && !ssMap.EnabledInternal() {
		return
	}

	if ssMap.beginTimeForCurInterval+intervalSeconds <= now {
		// `beginTimeForCurInterval` is a multiple of intervalSeconds, so that when the interval is a multiple
		// of 60 (or 600, 1800, 3600, etc), begin time shows 'XX:XX:00', not 'XX:XX:01'~'XX:XX:59'.
		ssMap.beginTimeForCurInterval = now / intervalSeconds * intervalSeconds
	}

	beginTime := ssMap.beginTimeForCurInterval
	var value kvcache.Value
	value, exist = ssMap.summaryMap.Get(key)
	var summary *stmtSummaryByDigest
	if !exist {
		// Lazy initialize it to release ssMap.mutex ASAP.
		summary = new(stmtSummaryByDigest)
		ssMap.summaryMap.Put(key, summary)
	} else {
		summary = value.(*stmtSummaryByDigest)
	}
	summary.isInternal = summary.isInternal && sei.IsInternal
	if summary != nil {
		summary.add(sei, beginTime, intervalSeconds, historySize)
	}
	if exist {
		StmtDigestKeyPool.Put(key)
	}
}

// Clear removes all statement summaries.
func (ssMap *stmtSummaryByDigestMap) Clear() {
	ssMap.Lock()
	defer ssMap.Unlock()

	ssMap.summaryMap.DeleteAll()
	ssMap.other.Clear()
	ssMap.beginTimeForCurInterval = 0
}

// clearInternal removes all statement summaries which are internal summaries.
func (ssMap *stmtSummaryByDigestMap) clearInternal() {
	ssMap.Lock()
	defer ssMap.Unlock()

	for _, key := range ssMap.summaryMap.Keys() {
		summary, ok := ssMap.summaryMap.Get(key)
		if !ok {
			continue
		}
		if summary.(*stmtSummaryByDigest).isInternal {
			ssMap.summaryMap.Delete(key)
		}
	}
}

// clearHistory removes history for all statement summaries, leaving only the current interval.
func (ssMap *stmtSummaryByDigestMap) clearHistory() {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	for _, value := range values {
		ssbd := value.(*stmtSummaryByDigest)
		ssbd.Lock()
		newHistory := list.New()
		newHistory.PushFront(ssbd.history.Front().Value)
		ssbd.history = newHistory
		ssbd.Unlock()
	}
}

// SetEnabled enables or disables statement summary
func (ssMap *stmtSummaryByDigestMap) SetEnabled(value bool) error {
	// `optEnabled` and `ssMap` don't need to be strictly atomically updated.
	ssMap.optEnabled.Store(value)
	if !value {
		ssMap.Clear()
	}
	return nil
}

// Enabled returns whether statement summary is enabled.
func (ssMap *stmtSummaryByDigestMap) Enabled() bool {
	return ssMap.optEnabled.Load()
}

// SetEnabledInternalQuery enables or disables internal statement summary
func (ssMap *stmtSummaryByDigestMap) SetEnabledInternalQuery(value bool) error {
	// `optEnableInternalQuery` and `ssMap` don't need to be strictly atomically updated.
	ssMap.optEnableInternalQuery.Store(value)
	if !value {
		ssMap.clearInternal()
	}
	return nil
}

// EnabledInternal returns whether internal statement summary is enabled.
func (ssMap *stmtSummaryByDigestMap) EnabledInternal() bool {
	return ssMap.optEnableInternalQuery.Load()
}

// SetHistoryEnabled enables or disables maintaining the history of statement summary intervals.
// When history is disabled, any existing history is cleared.
func (ssMap *stmtSummaryByDigestMap) SetHistoryEnabled(value bool) error {
	ssMap.optHistoryEnabled.Store(value)
	if !value {
		ssMap.clearHistory()
	}
	return nil
}

// historyEnabled returns whether the history of statement summary intervals is maintained.
func (ssMap *stmtSummaryByDigestMap) historyEnabled() bool {
	return ssMap.optHistoryEnabled.Load()
}

// SetRefreshInterval sets refreshing interval in ssMap.sysVars.
func (ssMap *stmtSummaryByDigestMap) SetRefreshInterval(value int64) error {
	ssMap.optRefreshInterval.Store(value)
	return nil
}

// refreshInterval gets the refresh interval for summaries.
func (ssMap *stmtSummaryByDigestMap) refreshInterval() int64 {
	return ssMap.optRefreshInterval.Load()
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetHistorySize(value int) error {
	ssMap.optHistorySize.Store(int32(value))
	return nil
}

// historySize gets the history size for summaries.
func (ssMap *stmtSummaryByDigestMap) historySize() int {
	return int(ssMap.optHistorySize.Load())
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetMaxStmtCount(value uint) error {
	// `optMaxStmtCount` and `ssMap` don't need to be strictly atomically updated.
	ssMap.optMaxStmtCount.Store(uint32(value))

	ssMap.Lock()
	defer ssMap.Unlock()
	return ssMap.summaryMap.SetCapacity(value)
}

// Used by tests
// nolint: unused
func (ssMap *stmtSummaryByDigestMap) maxStmtCount() int {
	return int(ssMap.optMaxStmtCount.Load())
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetMaxSQLLength(value int) error {
	ssMap.optMaxSQLLength.Store(int32(value))
	return nil
}

func (ssMap *stmtSummaryByDigestMap) maxSQLLength() int {
	return int(ssMap.optMaxSQLLength.Load())
}

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtExecInfo.
func (ssbd *stmtSummaryByDigest) init(sei *StmtExecInfo, _ int64, _ int64, _ int) {
	// Use "," to separate table names to support FIND_IN_SET.
	var buffer bytes.Buffer
	for i, value := range sei.StmtCtx.Tables {
		// In `create database` statement, DB name is not empty but table name is empty.
		if len(value.Table) == 0 {
			continue
		}
		buffer.WriteString(strings.ToLower(value.DB))
		buffer.WriteString(".")
		buffer.WriteString(strings.ToLower(value.Table))
		if i < len(sei.StmtCtx.Tables)-1 {
			buffer.WriteString(",")
		}
	}
	tableNames := buffer.String()

	ssbd.cumulative = *newStmtSummaryStats(sei)

	planDigest := sei.PlanDigest
	if len(planDigest) == 0 {
		// It comes here only when the plan is 'Point_Get'.
		planDigest = sei.LazyInfo.GetPlanDigest()
	}
	ssbd.schemaName = sei.SchemaName
	ssbd.digest = sei.Digest
	ssbd.planDigest = planDigest
	ssbd.stmtType = sei.StmtCtx.StmtType
	ssbd.normalizedSQL = formatSQL(sei.NormalizedSQL)
	ssbd.tableNames = tableNames
	ssbd.history = list.New()
	ssbd.initialized = true
	ssbd.bindingSQL, ssbd.bindingDigest = sei.LazyInfo.GetBindingSQLAndDigest()
}

func (ssbd *stmtSummaryByDigest) add(sei *StmtExecInfo, beginTime int64, intervalSeconds int64, historySize int) {
	// Enclose this block in a function to ensure the lock will always be released.
	warningCount := int(sei.StmtCtx.WarningCount())
	affectedRows := sei.StmtCtx.AffectedRows()
	ssElement, isElementNew := func() (*stmtSummaryByDigestElement, bool) {
		ssbd.Lock()
		defer ssbd.Unlock()

		if !ssbd.initialized {
			ssbd.init(sei, beginTime, intervalSeconds, historySize)
		}
		ssbd.cumulative.add(sei, warningCount, affectedRows)

		var ssElement *stmtSummaryByDigestElement
		isElementNew := true
		if ssbd.history.Len() > 0 {
			lastElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			if lastElement.beginTime >= beginTime {
				ssElement = lastElement
				isElementNew = false
			} else {
				// The last elements expires to the history.
				lastElement.onExpire(intervalSeconds)
			}
		}
		if isElementNew {
			// If the element is new created, `ssElement.add(sei)` should be done inside the lock of `ssbd`.
			ssElement = newStmtSummaryByDigestElement(sei, beginTime, intervalSeconds, warningCount, affectedRows)
			if ssElement == nil {
				return nil, isElementNew
			}
			ssbd.history.PushBack(ssElement)
		}

		// `historySize` might be modified anytime, so check expiration every time.
		// Even if history is set to 0, current summary is still needed.
		for ssbd.history.Len() > historySize && ssbd.history.Len() > 1 {
			ssbd.history.Remove(ssbd.history.Front())
		}

		return ssElement, isElementNew
	}()

	// Lock a single entry, not the whole `ssbd`.
	if !isElementNew {
		ssElement.add(sei, intervalSeconds, warningCount, affectedRows)
	}
}

// collectHistorySummaries puts at most `historySize` summaries to an array.
func (ssbd *stmtSummaryByDigest) collectHistorySummaries(checker *stmtSummaryChecker, historySize int) []*stmtSummaryByDigestElement {
	ssbd.Lock()
	defer ssbd.Unlock()

	if !ssbd.initialized {
		return nil
	}
	if checker != nil && !checker.isDigestValid(ssbd.digest) {
		return nil
	}

	ssElements := make([]*stmtSummaryByDigestElement, 0, ssbd.history.Len())
	for listElement := ssbd.history.Front(); listElement != nil && len(ssElements) < historySize; listElement = listElement.Next() {
		ssElement := listElement.Value.(*stmtSummaryByDigestElement)
		ssElements = append(ssElements, ssElement)
	}
	return ssElements
}

// MaxEncodedPlanSizeInBytes is the upper limit of the size of the plan and the binary plan in the stmt summary.
var MaxEncodedPlanSizeInBytes = 1024 * 1024

func newStmtSummaryStats(sei *StmtExecInfo) *stmtSummaryStats {
	// sampleSQL / authUsers(sampleUser) / samplePlan / prevSQL / indexNames store the values shown at the first time,
	// because it compacts performance to update every time.
	samplePlan, planHint, e := sei.LazyInfo.GetEncodedPlan()
	if e != nil {
		return nil
	}
	if len(samplePlan) > MaxEncodedPlanSizeInBytes {
		samplePlan = plancodec.PlanDiscardedEncoded
	}
	binPlan := sei.LazyInfo.GetBinaryPlan()
	if len(binPlan) > MaxEncodedPlanSizeInBytes {
		binPlan = plancodec.BinaryPlanDiscardedEncoded
	}
	return &stmtSummaryStats{
		sampleSQL: formatSQL(sei.LazyInfo.GetOriginalSQL()),
		charset:   sei.Charset,
		collation: sei.Collation,
		// PrevSQL is already truncated to cfg.Log.QueryLogMaxLen.
		prevSQL: sei.PrevSQL,
		// samplePlan needs to be decoded so it can't be truncated.
		samplePlan:        samplePlan,
		sampleBinaryPlan:  binPlan,
		planHint:          planHint,
		indexNames:        sei.StmtCtx.IndexNames,
		minLatency:        sei.TotalLatency,
		firstSeen:         sei.StartTime,
		lastSeen:          sei.StartTime,
		backoffTypes:      make(map[string]int),
		authUsers:         make(map[string]struct{}),
		planInCache:       false,
		planCacheHits:     0,
		planInBinding:     false,
		prepared:          sei.Prepared,
		minResultRows:     math.MaxInt64,
		resourceGroupName: sei.ResourceGroupName,
	}
}

func newStmtSummaryByDigestElement(sei *StmtExecInfo, beginTime int64, intervalSeconds int64, warningCount int, affectedRows uint64) *stmtSummaryByDigestElement {
	ssElement := &stmtSummaryByDigestElement{
		beginTime:        beginTime,
		stmtSummaryStats: *newStmtSummaryStats(sei),
	}
	ssElement.add(sei, intervalSeconds, warningCount, affectedRows)
	return ssElement
}

// onExpire is called when this element expires to history.
func (ssElement *stmtSummaryByDigestElement) onExpire(intervalSeconds int64) {
	ssElement.Lock()
	defer ssElement.Unlock()

	// refreshInterval may change anytime, so we need to update endTime.
	if ssElement.beginTime+intervalSeconds > ssElement.endTime {
		// // If interval changes to a bigger value, update endTime to beginTime + interval.
		ssElement.endTime = ssElement.beginTime + intervalSeconds
	} else if ssElement.beginTime+intervalSeconds < ssElement.endTime {
		now := time.Now().Unix()
		// If interval changes to a smaller value and now > beginTime + interval, update endTime to current time.
		if now > ssElement.beginTime+intervalSeconds {
			ssElement.endTime = now
		}
	}
}

func (ssStats *stmtSummaryStats) add(sei *StmtExecInfo, warningCount int, affectedRows uint64) {
	// add user to auth users set
	if len(sei.User) > 0 {
		ssStats.authUsers[sei.User] = struct{}{}
	}

	ssStats.execCount++
	if !sei.Succeed {
		ssStats.sumErrors++
	}
	ssStats.sumWarnings += warningCount

	// latency
	ssStats.sumLatency += sei.TotalLatency
	if sei.TotalLatency > ssStats.maxLatency {
		ssStats.maxLatency = sei.TotalLatency
	}
	if sei.TotalLatency < ssStats.minLatency {
		ssStats.minLatency = sei.TotalLatency
	}
	ssStats.sumParseLatency += sei.ParseLatency
	if sei.ParseLatency > ssStats.maxParseLatency {
		ssStats.maxParseLatency = sei.ParseLatency
	}
	ssStats.sumCompileLatency += sei.CompileLatency
	if sei.CompileLatency > ssStats.maxCompileLatency {
		ssStats.maxCompileLatency = sei.CompileLatency
	}

	// coprocessor
	if sei.CopTasks != nil {
		ssStats.sumNumCopTasks += int64(sei.CopTasks.NumCopTasks)
		ssStats.sumCopProcessTime += sei.CopTasks.TotProcessTime
		if sei.CopTasks.MaxProcessTime > ssStats.maxCopProcessTime {
			ssStats.maxCopProcessTime = sei.CopTasks.MaxProcessTime
			ssStats.maxCopProcessAddress = sei.CopTasks.MaxProcessAddress
		}
		ssStats.sumCopWaitTime += sei.CopTasks.TotWaitTime
		if sei.CopTasks.MaxWaitTime > ssStats.maxCopWaitTime {
			ssStats.maxCopWaitTime = sei.CopTasks.MaxWaitTime
			ssStats.maxCopWaitAddress = sei.CopTasks.MaxWaitAddress
		}
	}

	// TiKV
	ssStats.sumProcessTime += sei.ExecDetail.TimeDetail.ProcessTime
	if sei.ExecDetail.TimeDetail.ProcessTime > ssStats.maxProcessTime {
		ssStats.maxProcessTime = sei.ExecDetail.TimeDetail.ProcessTime
	}
	ssStats.sumWaitTime += sei.ExecDetail.TimeDetail.WaitTime
	if sei.ExecDetail.TimeDetail.WaitTime > ssStats.maxWaitTime {
		ssStats.maxWaitTime = sei.ExecDetail.TimeDetail.WaitTime
	}
	ssStats.sumBackoffTime += sei.ExecDetail.BackoffTime
	if sei.ExecDetail.BackoffTime > ssStats.maxBackoffTime {
		ssStats.maxBackoffTime = sei.ExecDetail.BackoffTime
	}

	if sei.ExecDetail.ScanDetail != nil {
		ssStats.sumTotalKeys += sei.ExecDetail.ScanDetail.TotalKeys
		if sei.ExecDetail.ScanDetail.TotalKeys > ssStats.maxTotalKeys {
			ssStats.maxTotalKeys = sei.ExecDetail.ScanDetail.TotalKeys
		}
		ssStats.sumProcessedKeys += sei.ExecDetail.ScanDetail.ProcessedKeys
		if sei.ExecDetail.ScanDetail.ProcessedKeys > ssStats.maxProcessedKeys {
			ssStats.maxProcessedKeys = sei.ExecDetail.ScanDetail.ProcessedKeys
		}
		ssStats.sumRocksdbDeleteSkippedCount += sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		if sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount > ssStats.maxRocksdbDeleteSkippedCount {
			ssStats.maxRocksdbDeleteSkippedCount = sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		}
		ssStats.sumRocksdbKeySkippedCount += sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		if sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount > ssStats.maxRocksdbKeySkippedCount {
			ssStats.maxRocksdbKeySkippedCount = sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		}
		ssStats.sumRocksdbBlockCacheHitCount += sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		if sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount > ssStats.maxRocksdbBlockCacheHitCount {
			ssStats.maxRocksdbBlockCacheHitCount = sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		}
		ssStats.sumRocksdbBlockReadCount += sei.ExecDetail.ScanDetail.RocksdbBlockReadCount
		if sei.ExecDetail.ScanDetail.RocksdbBlockReadCount > ssStats.maxRocksdbBlockReadCount {
			ssStats.maxRocksdbBlockReadCount = sei.ExecDetail.ScanDetail.RocksdbBlockReadCount
		}
		ssStats.sumRocksdbBlockReadByte += sei.ExecDetail.ScanDetail.RocksdbBlockReadByte
		if sei.ExecDetail.ScanDetail.RocksdbBlockReadByte > ssStats.maxRocksdbBlockReadByte {
			ssStats.maxRocksdbBlockReadByte = sei.ExecDetail.ScanDetail.RocksdbBlockReadByte
		}
	}

	// txn
	commitDetails := sei.ExecDetail.CommitDetail
	if commitDetails != nil {
		ssStats.commitCount++
		ssStats.sumPrewriteTime += commitDetails.PrewriteTime
		if commitDetails.PrewriteTime > ssStats.maxPrewriteTime {
			ssStats.maxPrewriteTime = commitDetails.PrewriteTime
		}
		ssStats.sumCommitTime += commitDetails.CommitTime
		if commitDetails.CommitTime > ssStats.maxCommitTime {
			ssStats.maxCommitTime = commitDetails.CommitTime
		}
		ssStats.sumGetCommitTsTime += commitDetails.GetCommitTsTime
		if commitDetails.GetCommitTsTime > ssStats.maxGetCommitTsTime {
			ssStats.maxGetCommitTsTime = commitDetails.GetCommitTsTime
		}
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLock.ResolveLockTime)
		ssStats.sumResolveLockTime += resolveLockTime
		if resolveLockTime > ssStats.maxResolveLockTime {
			ssStats.maxResolveLockTime = resolveLockTime
		}
		ssStats.sumLocalLatchTime += commitDetails.LocalLatchTime
		if commitDetails.LocalLatchTime > ssStats.maxLocalLatchTime {
			ssStats.maxLocalLatchTime = commitDetails.LocalLatchTime
		}
		ssStats.sumWriteKeys += int64(commitDetails.WriteKeys)
		if commitDetails.WriteKeys > ssStats.maxWriteKeys {
			ssStats.maxWriteKeys = commitDetails.WriteKeys
		}
		ssStats.sumWriteSize += int64(commitDetails.WriteSize)
		if commitDetails.WriteSize > ssStats.maxWriteSize {
			ssStats.maxWriteSize = commitDetails.WriteSize
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		ssStats.sumPrewriteRegionNum += int64(prewriteRegionNum)
		if prewriteRegionNum > ssStats.maxPrewriteRegionNum {
			ssStats.maxPrewriteRegionNum = prewriteRegionNum
		}
		ssStats.sumTxnRetry += int64(commitDetails.TxnRetry)
		if commitDetails.TxnRetry > ssStats.maxTxnRetry {
			ssStats.maxTxnRetry = commitDetails.TxnRetry
		}
		commitDetails.Mu.Lock()
		commitBackoffTime := commitDetails.Mu.CommitBackoffTime
		ssStats.sumCommitBackoffTime += commitBackoffTime
		if commitBackoffTime > ssStats.maxCommitBackoffTime {
			ssStats.maxCommitBackoffTime = commitBackoffTime
		}
		ssStats.sumBackoffTimes += int64(len(commitDetails.Mu.PrewriteBackoffTypes))
		for _, backoffType := range commitDetails.Mu.PrewriteBackoffTypes {
			ssStats.backoffTypes[backoffType]++
		}
		ssStats.sumBackoffTimes += int64(len(commitDetails.Mu.CommitBackoffTypes))
		for _, backoffType := range commitDetails.Mu.CommitBackoffTypes {
			ssStats.backoffTypes[backoffType]++
		}
		commitDetails.Mu.Unlock()
	}

	// plan cache
	if sei.PlanInCache {
		ssStats.planInCache = true
		ssStats.planCacheHits++
	} else {
		ssStats.planInCache = false
	}
	if sei.PlanCacheUnqualified != "" {
		ssStats.planCacheUnqualifiedCount++
		ssStats.lastPlanCacheUnqualified = sei.PlanCacheUnqualified
	}

	// SPM
	if sei.PlanInBinding {
		ssStats.planInBinding = true
	} else {
		ssStats.planInBinding = false
	}

	// other
	ssStats.sumAffectedRows += affectedRows
	ssStats.sumMem += sei.MemMax
	if sei.MemMax > ssStats.maxMem {
		ssStats.maxMem = sei.MemMax
	}
	ssStats.sumDisk += sei.DiskMax
	if sei.DiskMax > ssStats.maxDisk {
		ssStats.maxDisk = sei.DiskMax
	}
	if sei.StartTime.Before(ssStats.firstSeen) {
		ssStats.firstSeen = sei.StartTime
	}
	if ssStats.lastSeen.Before(sei.StartTime) {
		ssStats.lastSeen = sei.StartTime
	}
	if sei.ExecRetryCount > 0 {
		ssStats.execRetryCount += sei.ExecRetryCount
		ssStats.execRetryTime += sei.ExecRetryTime
	}
	if sei.ResultRows > 0 {
		ssStats.sumResultRows += sei.ResultRows
		if ssStats.maxResultRows < sei.ResultRows {
			ssStats.maxResultRows = sei.ResultRows
		}
		if ssStats.minResultRows > sei.ResultRows {
			ssStats.minResultRows = sei.ResultRows
		}
	} else {
		ssStats.minResultRows = 0
	}
	ssStats.sumKVTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.WaitKVRespDuration))
	ssStats.sumPDTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.WaitPDRespDuration))
	ssStats.sumBackoffTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.BackoffDuration))
	ssStats.sumWriteSQLRespTotal += sei.StmtExecDetails.WriteSQLRespDuration
	ssStats.sumTidbCPU += sei.CPUUsages.TidbCPUTime
	ssStats.sumTikvCPU += sei.CPUUsages.TikvCPUTime

	// network traffic
	ssStats.StmtNetworkTrafficSummary.Add(sei.TiKVExecDetails)

	// request-units
	ssStats.StmtRUSummary.Add(sei.RUDetail)

	ssStats.storageKV = sei.StmtCtx.IsTiKV.Load()
	ssStats.storageMPP = sei.StmtCtx.IsTiFlash.Load()
}

func (ssElement *stmtSummaryByDigestElement) add(sei *StmtExecInfo, intervalSeconds int64, warningCount int, affectedRows uint64) {
	ssElement.Lock()
	defer ssElement.Unlock()

	// refreshInterval may change anytime, update endTime ASAP.
	ssElement.endTime = ssElement.beginTime + intervalSeconds
	ssElement.stmtSummaryStats.add(sei, warningCount, affectedRows)
}

// Truncate SQL to maxSQLLength.
func formatSQL(sql string) string {
	maxSQLLength := StmtSummaryByDigestMap.maxSQLLength()
	length := len(sql)
	if length > maxSQLLength {
		var result strings.Builder
		result.WriteString(sql[:maxSQLLength])
		fmt.Fprintf(&result, "(len:%d)", length)
		return result.String()
	}
	return sql
}

// Format the backoffType map to a string or nil.
func formatBackoffTypes(backoffMap map[string]int) any {
	type backoffStat struct {
		backoffType string
		count       int
	}

	size := len(backoffMap)
	if size == 0 {
		return nil
	}

	backoffArray := make([]backoffStat, 0, len(backoffMap))
	for backoffType, count := range backoffMap {
		backoffArray = append(backoffArray, backoffStat{backoffType, count})
	}
	slices.SortFunc(backoffArray, func(i, j backoffStat) int {
		return cmp.Compare(j.count, i.count)
	})

	var buffer bytes.Buffer
	for index, stat := range backoffArray {
		if _, err := fmt.Fprintf(&buffer, "%v:%d", stat.backoffType, stat.count); err != nil {
			return "FORMAT ERROR"
		}
		if index < len(backoffArray)-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
}

func avgInt(sum int64, count int64) int64 {
	if count > 0 {
		return sum / count
	}
	return 0
}

func avgFloat(sum int64, count int64) float64 {
	if count > 0 {
		return float64(sum) / float64(count)
	}
	return 0
}

func avgSumFloat(sum float64, count int64) float64 {
	if count > 0 {
		return sum / float64(count)
	}
	return 0
}

func convertEmptyToNil(str string) any {
	if str == "" {
		return nil
	}
	return str
}

// StmtRUSummary is the request-units summary for each type of statements.
type StmtRUSummary struct {
	SumRRU            float64       `json:"sum_rru"`
	SumWRU            float64       `json:"sum_wru"`
	SumRUWaitDuration time.Duration `json:"sum_ru_wait_duration"`
	MaxRRU            float64       `json:"max_rru"`
	MaxWRU            float64       `json:"max_wru"`
	MaxRUWaitDuration time.Duration `json:"max_ru_wait_duration"`
}

// Add add a new sample value to the ru summary record.
func (s *StmtRUSummary) Add(info *util.RUDetails) {
	if info != nil {
		rru := info.RRU()
		s.SumRRU += rru
		if s.MaxRRU < rru {
			s.MaxRRU = rru
		}
		wru := info.WRU()
		s.SumWRU += wru
		if s.MaxWRU < wru {
			s.MaxWRU = wru
		}
		ruWaitDur := info.RUWaitDuration()
		s.SumRUWaitDuration += ruWaitDur
		if s.MaxRUWaitDuration < ruWaitDur {
			s.MaxRUWaitDuration = ruWaitDur
		}
	}
}

// Merge merges the value of 2 ru summary records.
func (s *StmtRUSummary) Merge(other *StmtRUSummary) {
	s.SumRRU += other.SumRRU
	s.SumWRU += other.SumWRU
	s.SumRUWaitDuration += other.SumRUWaitDuration
	if s.MaxRRU < other.MaxRRU {
		s.MaxRRU = other.MaxRRU
	}
	if s.MaxWRU < other.MaxWRU {
		s.MaxWRU = other.MaxWRU
	}
	if s.MaxRUWaitDuration < other.MaxRUWaitDuration {
		s.MaxRUWaitDuration = other.MaxRUWaitDuration
	}
}

// StmtNetworkTrafficSummary is the network traffic summary for each type of statements.
type StmtNetworkTrafficSummary struct {
	UnpackedBytesSentTiKVTotal            int64 `json:"unpacked_bytes_send_tikv_total"`
	UnpackedBytesReceivedTiKVTotal        int64 `json:"unpacked_bytes_received_tikv_total"`
	UnpackedBytesSentTiKVCrossZone        int64 `json:"unpacked_bytes_send_tikv_cross_zone"`
	UnpackedBytesReceivedTiKVCrossZone    int64 `json:"unpacked_bytes_received_tikv_cross_zone"`
	UnpackedBytesSentTiFlashTotal         int64 `json:"unpacked_bytes_send_tiflash_total"`
	UnpackedBytesReceivedTiFlashTotal     int64 `json:"unpacked_bytes_received_tiflash_total"`
	UnpackedBytesSentTiFlashCrossZone     int64 `json:"unpacked_bytes_send_tiflash_cross_zone"`
	UnpackedBytesReceivedTiFlashCrossZone int64 `json:"unpacked_bytes_received_tiflash_cross_zone"`
}

// Merge merges the value of 2 network traffic summary records.
func (s *StmtNetworkTrafficSummary) Merge(other *StmtNetworkTrafficSummary) {
	if other == nil {
		return
	}
	s.UnpackedBytesSentTiKVTotal += other.UnpackedBytesSentTiKVTotal
	s.UnpackedBytesReceivedTiKVTotal += other.UnpackedBytesReceivedTiKVTotal
	s.UnpackedBytesSentTiKVCrossZone += other.UnpackedBytesSentTiKVCrossZone
	s.UnpackedBytesReceivedTiKVCrossZone += other.UnpackedBytesReceivedTiKVCrossZone
	s.UnpackedBytesSentTiFlashTotal += other.UnpackedBytesSentTiFlashTotal
	s.UnpackedBytesReceivedTiFlashTotal += other.UnpackedBytesReceivedTiFlashTotal
	s.UnpackedBytesSentTiFlashCrossZone += other.UnpackedBytesSentTiFlashCrossZone
	s.UnpackedBytesReceivedTiFlashCrossZone += other.UnpackedBytesReceivedTiFlashCrossZone
}

// Add add a new sample value to the ru summary record.
func (s *StmtNetworkTrafficSummary) Add(info *util.ExecDetails) {
	if info != nil {
		s.UnpackedBytesSentTiKVTotal += info.UnpackedBytesSentKVTotal
		s.UnpackedBytesReceivedTiKVTotal += info.UnpackedBytesReceivedKVTotal
		s.UnpackedBytesSentTiKVCrossZone += info.UnpackedBytesSentKVCrossZone
		s.UnpackedBytesReceivedTiKVCrossZone += info.UnpackedBytesReceivedKVCrossZone
		s.UnpackedBytesSentTiFlashTotal += info.UnpackedBytesSentMPPTotal
		s.UnpackedBytesReceivedTiFlashTotal += info.UnpackedBytesReceivedMPPTotal
		s.UnpackedBytesSentTiFlashCrossZone += info.UnpackedBytesSentMPPCrossZone
		s.UnpackedBytesReceivedTiFlashCrossZone += info.UnpackedBytesReceivedMPPCrossZone
	}
}
