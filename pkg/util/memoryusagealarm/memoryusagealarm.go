// Copyright 2020 PingCAP, Inc.
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

package memoryusagealarm

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	rpprof "runtime/pprof"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ConfigProvider provides memory usage alarm configuration values
type ConfigProvider interface {
	// GetMemoryUsageAlarmRatio returns the ratio of memory usage that triggers an alarm
	GetMemoryUsageAlarmRatio() float64
	// GetMemoryUsageAlarmKeepRecordNum returns the number of alarm records to keep
	GetMemoryUsageAlarmKeepRecordNum() int64
	// GetLogDir returns the directory for storing logs
	GetLogDir() string
	// GetComponentName returns the name of the component (e.g. "tidb-server" or "br")
	GetComponentName() string
}

// TiDBConfigProvider implements ConfigProvider using TiDB's vardef variables
type TiDBConfigProvider struct{}

// GetMemoryUsageAlarmRatio returns the ratio of memory usage that triggers an alarm.
// When memory usage exceeds this ratio of the total memory limit (or system memory if no limit),
// the memory monitor will dump profiles and trigger OOM-related actions.
func (*TiDBConfigProvider) GetMemoryUsageAlarmRatio() float64 {
	return vardef.MemoryUsageAlarmRatio.Load()
}

// GetMemoryUsageAlarmKeepRecordNum returns the number of alarm records to keep.
// When the number of records exceeds this limit, older records will be deleted.
func (*TiDBConfigProvider) GetMemoryUsageAlarmKeepRecordNum() int64 {
	return vardef.MemoryUsageAlarmKeepRecordNum.Load()
}

// GetLogDir returns the directory for storing memory profiles and alarm records.
func (*TiDBConfigProvider) GetLogDir() string {
	logDir, _ := filepath.Split(config.GetGlobalConfig().Log.File.Filename)
	return logDir
}

// GetComponentName returns the name of the component for logging and metrics.
// This helps identify which component triggered the memory alarm.
func (*TiDBConfigProvider) GetComponentName() string {
	return "tidb-server"
}

// Handle is the handler for memory usage alarm.
type Handle struct {
	exitCh         chan struct{}
	sm             atomic.Pointer[sessmgr.Manager]
	configProvider ConfigProvider
}

// NewMemoryUsageAlarmHandle builds a memory usage alarm handler.
func NewMemoryUsageAlarmHandle(exitCh chan struct{}, provider ConfigProvider) *Handle {
	return &Handle{
		exitCh:         exitCh,
		configProvider: provider,
	}
}

// SetSessionManager sets the Manager which is used to fetching the info
// of all active sessions.
func (eqh *Handle) SetSessionManager(sm sessmgr.Manager) *Handle {
	eqh.sm.Store(&sm)
	return eqh
}

// Run starts a memory usage alarm goroutine at the start time of the server.
func (eqh *Handle) Run() {
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load()
	record := &memoryUsageAlarm{
		configProvider: eqh.configProvider,
	}
	for {
		select {
		case <-ticker.C:
			record.alarm4ExcessiveMemUsage(*sm)
		case <-eqh.exitCh:
			return
		}
	}
}

type memoryUsageAlarm struct {
	lastCheckTime                 time.Time
	lastUpdateVariableTime        time.Time
	err                           error
	baseRecordDir                 string
	lastRecordDirName             []string
	lastRecordMemUsed             uint64
	memoryUsageAlarmRatio         float64
	memoryUsageAlarmKeepRecordNum int64
	serverMemoryLimit             uint64
	isServerMemoryLimitSet        bool
	initialized                   bool
	configProvider                ConfigProvider
}

func (record *memoryUsageAlarm) updateVariable() {
	if time.Since(record.lastUpdateVariableTime) < 60*time.Second {
		return
	}
	record.memoryUsageAlarmRatio = record.configProvider.GetMemoryUsageAlarmRatio()
	record.memoryUsageAlarmKeepRecordNum = record.configProvider.GetMemoryUsageAlarmKeepRecordNum()
	record.serverMemoryLimit = memory.ServerMemoryLimit.Load()
	if record.serverMemoryLimit != 0 {
		record.isServerMemoryLimitSet = true
	} else {
		record.serverMemoryLimit, record.err = memory.MemTotal()
		if record.err != nil {
			logutil.BgLogger().Error("get system total memory fail", zap.Error(record.err))
			return
		}
		record.isServerMemoryLimitSet = false
	}
	record.lastUpdateVariableTime = time.Now()
}

func (record *memoryUsageAlarm) initMemoryUsageAlarmRecord() {
	record.lastCheckTime = time.Time{}
	record.lastUpdateVariableTime = time.Time{}
	record.updateVariable()
	tidbLogDir := record.configProvider.GetLogDir()
	record.baseRecordDir = filepath.Join(tidbLogDir, "oom_record")
	if record.err = disk.CheckAndCreateDir(record.baseRecordDir); record.err != nil {
		return
	}
	// Read last records
	recordDirs, err := os.ReadDir(record.baseRecordDir)
	if err != nil {
		record.err = err
		return
	}
	for _, dir := range recordDirs {
		name := filepath.Join(record.baseRecordDir, dir.Name())
		if strings.Contains(dir.Name(), "record") {
			record.lastRecordDirName = append(record.lastRecordDirName, name)
		}
	}
	record.initialized = true
}

// If Performance.ServerMemoryQuota is set, use `ServerMemoryQuota * MemoryUsageAlarmRatio` to check oom risk.
// If Performance.ServerMemoryQuota is not set, use `system total memory size * MemoryUsageAlarmRatio` to check oom risk.
func (record *memoryUsageAlarm) alarm4ExcessiveMemUsage(sm sessmgr.Manager) {
	if !record.initialized {
		record.initMemoryUsageAlarmRecord()
		if record.err != nil {
			return
		}
	} else {
		record.updateVariable()
	}
	if record.memoryUsageAlarmRatio <= 0.0 || record.memoryUsageAlarmRatio >= 1.0 {
		return
	}
	var memoryUsage uint64
	instanceStats := memory.ReadMemStats()
	if record.isServerMemoryLimitSet {
		memoryUsage = instanceStats.HeapAlloc
	} else {
		memoryUsage, record.err = memory.MemUsed()
		if record.err != nil {
			logutil.BgLogger().Error("get system memory usage fail", zap.Error(record.err))
			return
		}
	}

	// TODO: Consider NextGC to record SQLs.
	if needRecord, reason := record.needRecord(memoryUsage); needRecord {
		record.lastCheckTime = time.Now()
		record.lastRecordMemUsed = memoryUsage
		record.doRecord(memoryUsage, instanceStats.HeapAlloc, sm, reason)
		record.tryRemoveRedundantRecords()
	}
}

// AlarmReason implements alarm reason.
type AlarmReason uint

const (
	// GrowTooFast is the reason that memory increasing too fast.
	GrowTooFast AlarmReason = iota
	// ExceedAlarmRatio is the reason that memory used exceed threshold.
	ExceedAlarmRatio
	// NoReason means no alarm
	NoReason
)

func (reason AlarmReason) String() string {
	return [...]string{"memory usage grows too fast", "memory usage exceeds alarm ratio", "no reason"}[reason]
}

func (record *memoryUsageAlarm) needRecord(memoryUsage uint64) (bool, AlarmReason) {
	// At least 60 seconds between two recordings that memory usage is less than threshold (default 70% system memory).
	// If the memory is still exceeded, only records once.
	// If the memory used ratio recorded this time is 0.1 higher than last time, we will force record this time.
	if float64(memoryUsage) <= float64(record.serverMemoryLimit)*record.memoryUsageAlarmRatio {
		return false, NoReason
	}

	interval := time.Since(record.lastCheckTime)
	memDiff := int64(memoryUsage) - int64(record.lastRecordMemUsed)
	if interval > 60*time.Second {
		return true, ExceedAlarmRatio
	}
	if float64(memDiff) > 0.1*float64(record.serverMemoryLimit) {
		return true, GrowTooFast
	}
	return false, NoReason
}

func (record *memoryUsageAlarm) doRecord(memUsage uint64, instanceMemoryUsage uint64, sm sessmgr.Manager, alarmReason AlarmReason) {
	fields := make([]zap.Field, 0, 6)
	componentName := record.configProvider.GetComponentName()
	fields = append(fields, zap.Bool(fmt.Sprintf("is %s_memory_limit set", componentName), record.isServerMemoryLimitSet))
	if record.isServerMemoryLimitSet {
		fields = append(fields, zap.Uint64(fmt.Sprintf("%s_memory_limit", componentName), record.serverMemoryLimit))
		fields = append(fields, zap.Uint64(fmt.Sprintf("%s memory usage", componentName), memUsage))
	} else {
		fields = append(fields, zap.Uint64("system memory total", record.serverMemoryLimit))
		fields = append(fields, zap.Uint64("system memory usage", memUsage))
		fields = append(fields, zap.Uint64(fmt.Sprintf("%s memory usage", componentName), instanceMemoryUsage))
	}
	fields = append(fields, zap.Float64("memory-usage-alarm-ratio", record.memoryUsageAlarmRatio))
	fields = append(fields, zap.String("record path", record.baseRecordDir))
	logutil.BgLogger().Warn(fmt.Sprintf("%s has the risk of OOM because of %s. Running profiles will be recorded in record path", componentName, alarmReason.String()), fields...)
	recordDir := filepath.Join(record.baseRecordDir, "record"+record.lastCheckTime.Format(time.RFC3339))
	if record.err = disk.CheckAndCreateDir(recordDir); record.err != nil {
		return
	}
	record.lastRecordDirName = append(record.lastRecordDirName, recordDir)
	if sm != nil {
		if record.err = record.recordSQL(sm, recordDir); record.err != nil {
			return
		}
	}
	if record.err = record.recordProfile(recordDir); record.err != nil {
		return
	}
}

func (record *memoryUsageAlarm) tryRemoveRedundantRecords() {
	filename := &record.lastRecordDirName
	for len(*filename) > int(record.memoryUsageAlarmKeepRecordNum) {
		err := os.RemoveAll((*filename)[0])
		if err != nil {
			logutil.BgLogger().Error("remove temp files failed", zap.Error(err))
		}
		*filename = (*filename)[1:]
	}
}

func getPlanString(info *sessmgr.ProcessInfo) string {
	var buf strings.Builder
	rows, _ := plancodec.DecodeBinaryPlan4Connection(info.BriefBinaryPlan, types.ExplainFormatROW, true)
	buf.WriteString(fmt.Sprintf("|%v|%v|%v|%v|%v|", "id", "estRows", "task", "access object", "operator info"))
	for _, row := range rows {
		buf.WriteString("\n|")
		for _, col := range row {
			buf.WriteString(fmt.Sprintf("%v|", col))
		}
	}
	return buf.String()
}

func (record *memoryUsageAlarm) printTop10SqlInfo(pinfo []*sessmgr.ProcessInfo, f *os.File) {
	if _, err := f.WriteString("The 10 SQLs with the most memory usage for OOM analysis\n"); err != nil {
		logutil.BgLogger().Error("write top 10 memory sql info fail", zap.Error(err))
	}
	memBuf := record.getTop10SqlInfoByMemoryUsage(pinfo)
	if _, err := f.WriteString(memBuf.String()); err != nil {
		logutil.BgLogger().Error("write top 10 memory sql info fail", zap.Error(err))
	}
	if _, err := f.WriteString("The 10 SQLs with the most time usage for OOM analysis\n"); err != nil {
		logutil.BgLogger().Error("write top 10 time cost sql info fail", zap.Error(err))
	}
	costBuf := record.getTop10SqlInfoByCostTime(pinfo)
	if _, err := f.WriteString(costBuf.String()); err != nil {
		logutil.BgLogger().Error("write top 10 time cost sql info fail", zap.Error(err))
	}
}

func (record *memoryUsageAlarm) getTop10SqlInfo(cmp func(i, j *sessmgr.ProcessInfo) int, pinfo []*sessmgr.ProcessInfo) strings.Builder {
	slices.SortFunc(pinfo, cmp)
	list := pinfo
	var buf strings.Builder
	oomAction := vardef.OOMAction.Load()
	serverMemoryLimit := memory.ServerMemoryLimit.Load()
	for i, totalCnt := 0, 10; i < len(list) && totalCnt > 0; i++ {
		info := list[i]
		buf.WriteString(fmt.Sprintf("SQL %v: \n", i))
		fields := util.GenLogFields(record.lastCheckTime.Sub(info.Time), info, false)
		if fields == nil {
			continue
		}
		fields = append(fields, zap.String("tidb_mem_oom_action", oomAction))
		fields = append(fields, zap.Uint64("tidb_server_memory_limit", serverMemoryLimit))
		fields = append(fields, zap.Int64("tidb_mem_quota_query", info.OOMAlarmVariablesInfo.SessionMemQuotaQuery))
		fields = append(fields, zap.Int("tidb_analyze_version", info.OOMAlarmVariablesInfo.SessionAnalyzeVersion))
		fields = append(fields, zap.Bool("tidb_enable_rate_limit_action", info.OOMAlarmVariablesInfo.SessionEnabledRateLimitAction))
		fields = append(fields, zap.String("current_analyze_plan", getPlanString(info)))
		for _, field := range fields {
			switch field.Type {
			case zapcore.StringType:
				fmt.Fprintf(&buf, "%v: %v", field.Key, field.String)
			case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
				fmt.Fprintf(&buf, "%v: %v", field.Key, uint64(field.Integer))
			case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type:
				fmt.Fprintf(&buf, "%v: %v", field.Key, field.Integer)
			case zapcore.BoolType:
				fmt.Fprintf(&buf, "%v: %v", field.Key, field.Integer == 1)
			}
			buf.WriteString("\n")
		}
		totalCnt--
	}
	buf.WriteString("\n")
	return buf
}

func (record *memoryUsageAlarm) getTop10SqlInfoByMemoryUsage(pinfo []*sessmgr.ProcessInfo) strings.Builder {
	return record.getTop10SqlInfo(func(i, j *sessmgr.ProcessInfo) int {
		return cmp.Compare(j.MemTracker.MaxConsumed(), i.MemTracker.MaxConsumed())
	}, pinfo)
}

func (record *memoryUsageAlarm) getTop10SqlInfoByCostTime(pinfo []*sessmgr.ProcessInfo) strings.Builder {
	return record.getTop10SqlInfo(func(i, j *sessmgr.ProcessInfo) int {
		return i.Time.Compare(j.Time)
	}, pinfo)
}

func (record *memoryUsageAlarm) recordSQL(sm sessmgr.Manager, recordDir string) error {
	processInfo := sm.ShowProcessList()
	pinfo := make([]*sessmgr.ProcessInfo, 0, len(processInfo))
	for _, info := range processInfo {
		if len(info.Info) != 0 {
			pinfo = append(pinfo, info)
		}
	}
	fileName := filepath.Join(recordDir, "running_sql")
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error("create oom record file fail", zap.Error(err))
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error("close oom record file fail", zap.Error(err))
		}
	}()
	record.printTop10SqlInfo(pinfo, f)
	return nil
}

type item struct {
	Name  string
	Debug int
}

func (*memoryUsageAlarm) recordProfile(recordDir string) error {
	items := []item{
		{Name: "heap"},
		{Name: "goroutine", Debug: 2},
	}
	for _, item := range items {
		if err := write(item, recordDir); err != nil {
			return err
		}
	}
	return nil
}

func write(item item, recordDir string) error {
	fileName := filepath.Join(recordDir, item.Name)
	f, err := os.Create(fileName)
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("create %v profile file fail", item.Name), zap.Error(err))
		return err
	}
	p := rpprof.Lookup(item.Name)
	err = p.WriteTo(f, item.Debug)
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("write %v profile file fail", item.Name), zap.Error(err))
		return err
	}

	//nolint: revive
	defer func() {
		err := f.Close()
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("close %v profile file fail", item.Name), zap.Error(err))
		}
	}()
	return nil
}
