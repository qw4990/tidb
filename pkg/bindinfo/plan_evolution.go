package bindinfo

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
)

type EvolutionResult struct {
	BaselineDigest string        // the plan baseline digest
	EstPlanCost    string        // the estimated plan cost
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
	EvolveOne(sqlDigest string, autoAccept bool) error
}

type evolutionHandle struct {
	baselineHandle PlanBaselineHandle

	sPool SessionPool
}

func (h *evolutionHandle) EvolveAll() error {
	// TODO
	return nil
}

func (h *evolutionHandle) EvolveOne(sqlDigest string, autoAccept bool) (results []*EvolutionResult, err error) {
	baselines, err := h.baselineHandle.GetBaseline("", sqlDigest, "", "")
	if err != nil {
		return nil, err
	}

	if results, err = h.evolveBaselines(baselines); err != nil {
		return nil, err
	}

	if err := h.writeEvolutionResults(results); err != nil {
		return nil, err
	}

	if autoAccept {
		return results, h.autoAccept(baselines, results)
	}
	return
}

func (h *evolutionHandle) evolveBaselines(baselines []*PlanBaseline) (results []*EvolutionResult, err error) {
	err = callWithSCtx(h.sPool, false, func(sctx sessionctx.Context) error {
		for _, baseline := range baselines {
			hintedStmt := hintedStmtFromBaseline(baseline)
			rows, _, err := execRows(sctx, "explain format='verbose' "+hintedStmt) // TODO: what if it contains decorrelated sub-query?
			if err != nil {
				return err
			}
			estCost := rows[0].GetString(3)

			execBegin := time.Now()
			_, _, err = execRows(sctx, "explain analyze "+hintedStmt) // TODO: resource control
			if err != nil {
				return err
			}
			execTime := time.Since(execBegin)

			results = append(results, &EvolutionResult{
				BaselineDigest: baseline.Digest,
				EstPlanCost:    estCost,
				ExecTime:       execTime,
				EvolutionTime:  time.Now(),
				SQLText:        baseline.SQLText,
				PlanText:       "", // TODO
			})
		}
		return nil
	})
	return
}

func (h *evolutionHandle) writeEvolutionResults(results []*EvolutionResult) error {
	return callWithSCtx(h.sPool, true, func(sctx sessionctx.Context) error {
		for _, r := range results {
			if _, _, err := execRows(sctx, "insert into mysql.plan_evolution values (?,?,?,?,?,?)",
				r.BaselineDigest, r.EstPlanCost, r.ExecTime, r.EvolutionTime, r.SQLText, r.PlanText); err != nil {
				return err
			}
		}
		return nil
	})
}

func (h *evolutionHandle) autoAccept(baselines []*PlanBaseline, results []*EvolutionResult) error {
	return callWithSCtx(h.sPool, true, func(sctx sessionctx.Context) error {
		var bestAcceptedExecTime time.Duration
		for i := range baselines {
			if baselines[i].Status != StateAccepted {
				continue
			}
			if results[i].ExecTime < bestAcceptedExecTime {
				bestAcceptedExecTime = results[i].ExecTime
			}
		}

		for i := range baselines {
			if baselines[i].Status != StateUnverified {
				continue
			}
			if results[i].ExecTime < bestAcceptedExecTime {
				if err := h.baselineHandle.UpdateBaselineStatus(baselines[i].Digest, baselines[i].SQLDigest, baselines[i].PlanDigest, StateAccepted); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func hintedStmtFromBaseline(baseline *PlanBaseline) string {
	// TODO: change a more robust implementation.
	// simply assume the first 6 characters are 'select'
	sqlText := strings.TrimSpace(baseline.SQLText)
	return "select " + baseline.PlanHint + " " + sqlText[6:]
}
