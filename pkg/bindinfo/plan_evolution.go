package bindinfo

import (
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
)

type EvolutionResult struct {
	BaselineDigest string        // the plan baseline digest
	EstPlanCost    float64       // the estimated plan cost
	ExecTime       time.Duration // the execution time of this plan
	EvolutionTime  time.Time     // the time when this plan evolved
	// TODO: SchemaVersion?
	SQLText  string // the SQL statement
	PlanText string // the execution plan

}

type EvolutionHandle interface {
	// EvolveAll evolves all plan baselines.
	EvolveAll() error

	// EvolveOne evolves the specified plan baseline.
	EvolveOne(sqlDigest string) error
}

type evolutionHandle struct {
	baselineHandle PlanBaselineHandle

	sPool SessionPool
}

func (h *evolutionHandle) EvolveAll() error {
	// TODO
	return nil
}

func (h *evolutionHandle) EvolveOne(sqlDigest string) error {
	return callWithSCtx(h.sPool, false, func(sctx sessionctx.Context) error {
		baselines, err := h.baselineHandle.GetBaseline("", sqlDigest, "", "")
		if err != nil {
			return nil
		}

		return nil
	})
}
