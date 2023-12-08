package bindinfo

import "time"

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
	EvolveOne(baselineDigest string) error
}
