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

func (h *evolutionHandle) EvolveOne(sqlDigest string) (results []*EvolutionResult, err error) {
	err = callWithSCtx(h.sPool, false, func(sctx sessionctx.Context) error {
		baselines, err := h.baselineHandle.GetBaseline("", sqlDigest, "", "")
		if err != nil {
			return nil
		}

		results, err = evolveBaselines(sctx, baselines)
		if err != nil {
			return err
		}

		return writeEvolutionResults(sctx, results)

		// TODO: automatically accept verified baselines?
	})
	return
}

func evolveBaselines(sctx sessionctx.Context, baselines []*PlanBaseline) (results []*EvolutionResult, err error) {
	for _, baseline := range baselines {
		hintedStmt := hintedStmtFromBaseline(baseline)
		rows, _, err := execRows(sctx, "explain format='verbose' "+hintedStmt) // TODO: what if it contains decorrelated sub-query?
		if err != nil {
			return nil, err
		}
		estCost := rows[0].GetString(3)

		execBegin := time.Now()
		_, _, err = execRows(sctx, "explain analyze "+hintedStmt) // TODO: resource control
		if err != nil {
			return nil, err
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
	return
}

func writeEvolutionResults(sctx sessionctx.Context, results []*EvolutionResult) error {
	for _, r := range results {
		if _, _, err := execRows(sctx, "insert into mysql.plan_evolution values (?,?,?,?,?,?)",
			r.BaselineDigest, r.EstPlanCost, r.ExecTime, r.EvolutionTime, r.SQLText, r.PlanText); err != nil {
			return err
		}
	}
	return nil
}

func hintedStmtFromBaseline(baseline *PlanBaseline) string {
	// TODO: change a more robust implementation.
	// simply assume the first 6 characters are 'select'
	sqlText := strings.TrimSpace(baseline.SQLText)
	return "select " + baseline.PlanHint + " " + sqlText[6:]
}
