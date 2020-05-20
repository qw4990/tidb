package executor

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"io/ioutil"
	"path"
	"sync"
	"sync/atomic"
)

type HashAggResultTable interface {
	Get(key string) ([]aggfuncs.PartialResult, bool, error)
	Put(key string, results []aggfuncs.PartialResult) error
	Foreach(callback func(key string, results []aggfuncs.PartialResult)) error
}

type hashAggResultTableImpl struct {
	// dm disk-based map
	sync.RWMutex
	state      int // 0: memory, 1: should spill, 2: spilled, 3: ignore spill
	memResult  map[string][]aggfuncs.PartialResult
	diskResult *leveldb.DB
	memTracker *memory.Tracker
	aggFuncs   []aggfuncs.AggFunc
}

func NewHashAggResultTable(ctx sessionctx.Context, aggFuncs []aggfuncs.AggFunc, useTmpStorage bool, memTracker *memory.Tracker) *hashAggResultTableImpl {
	t := &hashAggResultTableImpl{
		memResult:  make(map[string][]aggfuncs.PartialResult),
		memTracker: memTracker,
		aggFuncs:   aggFuncs,
	}
	if useTmpStorage {
		oomAction := newHashAggTableImplAction(t)
		ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(oomAction)
	}
	return t
}

func (t *hashAggResultTableImpl) Get(key string) ([]aggfuncs.PartialResult, bool, error) {
	t.RLock()
	defer t.RUnlock()
	if t.state != 2 {
		prs, ok := t.memResult[key]
		return prs, ok, nil
	}
	val, err := t.diskResult.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, errors.Trace(err)
	}
	prs, err := aggfuncs.DecodePartialResult(t.aggFuncs, val)
	return prs, true, err
}

func (t *hashAggResultTableImpl) Put(key string, prs []aggfuncs.PartialResult) error {
	t.Lock()
	if t.state != 2 {
		oldPrs, ok := t.memResult[key]
		oldMem := aggfuncs.PartialResultsMemory(t.aggFuncs, oldPrs)
		newMem := aggfuncs.PartialResultsMemory(t.aggFuncs, prs)
		t.memResult[key] = prs
		t.Unlock()
		delta := newMem - oldMem
		if !ok {
			delta += int64(len(key))
		}
		if delta != 0 && t.state == 0 {
			t.memTracker.Consume(delta)
		}
		return nil
	}

	defer t.Unlock()
	val, err := aggfuncs.EncodePartialResult(t.aggFuncs, prs)
	if err != nil {
		return err
	}
	return errors.Trace(t.diskResult.Put([]byte(key), val, nil))
}

func (t *hashAggResultTableImpl) Foreach(callback func(key string, results []aggfuncs.PartialResult)) error {
	t.RLock()
	defer t.RUnlock()
	if t.state != 2 {
		for key, prs := range t.memResult {
			callback(key, prs)
		}
		return nil
	}
	it := t.diskResult.NewIterator(nil, nil)
	for it.Next() {
		key, val := string(it.Key()), it.Value()
		prs, err := aggfuncs.DecodePartialResult(t.aggFuncs, val)
		if err != nil {
			return err
		}
		callback(key, prs)
	}
	return nil
}

func (t *hashAggResultTableImpl) spill() (err error) {
	fmt.Println(">>>>>>>>>>>>>>>> try to spill")
	if err := aggfuncs.SupportDisk(t.aggFuncs); err != nil {
		fmt.Println(">>>>>>>>>>>>>>>>>>>> ignore spill state=3 ", err)
		logutil.BgLogger().Info(err.Error())
		t.state = 3
		return nil
	}
	dir := config.GetGlobalConfig().TempStoragePath
	tmpFile, err := ioutil.TempFile(config.GetGlobalConfig().TempStoragePath, t.memTracker.Label().String())
	if err != nil {
		fmt.Println(">>>>>>>>>>>>>>>> cannot spill open file error ", err)
		return err
	}
	tmpPath := path.Join(dir, tmpFile.Name())
	if t.diskResult, err = leveldb.OpenFile(tmpPath, nil); err != nil {
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>> open levelDB error ", err)
		return
	}
	fmt.Println(">>>>>>>>>>>>>>>>>> spill path >> ", tmpPath)
	for key, prs := range t.memResult {
		mem := aggfuncs.PartialResultsMemory(t.aggFuncs, prs)
		t.memTracker.ConsumeNoAction(-(mem + int64(len(key))))
		val, err := aggfuncs.EncodePartialResult(t.aggFuncs, prs)
		if err != nil {
			return err
		}
		if err := t.diskResult.Put([]byte(key), val, nil); err != nil {
			return err
		}
	}
	t.memResult = nil
	t.state = 2
	fmt.Println(">>>>>>>>>>>>>>>>>>>> spill succ")
	return nil
}

func (t *hashAggResultTableImpl) oomAction() {
	t.Lock()
	defer t.Unlock()
	t.state = 1
	logutil.BgLogger().Info("Spill hash-agg result table to disk.")
	if err := t.spill(); err != nil {
		panic(err)
	}
}

type hashAggTableImplAction struct {
	t    *hashAggResultTableImpl
	next memory.ActionOnExceed
	done uint64
}

func newHashAggTableImplAction(t *hashAggResultTableImpl) *hashAggTableImplAction {
	return &hashAggTableImplAction{t: t}
}

func (act *hashAggTableImplAction) Action(t *memory.Tracker) {
	if atomic.CompareAndSwapUint64(&act.done, 0, 1) {
		act.t.oomAction()
		return
	}
	act.next.Action(t)
}

func (act *hashAggTableImplAction) SetLogHook(hook func(uint64)) {}

func (act *hashAggTableImplAction) SetFallback(a memory.ActionOnExceed) {
	act.next = a
}
