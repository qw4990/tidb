package bindinfo

import (
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"strings"
	"time"
)

const StateAccepted = "accepted"
const StatePreferred = "preferred"
const StateUnverified = "unverified"
const StateDisabled = "disabled"

type PlanBaseline struct {
	// core fields
	Digest     string // identifier of this plan baseline.
	SQLDigest  string // identifier of the SQL statement.
	PlanDigest string // identifier of the execution plan.
	Outline    string // a set of hints corresponding to the SQL to generate the execution plan.
	Status     string // the status of this plan baseline: accepted, preferred, unverified, disabled.
	//TODO: SchemaVer

	// meta information
	Creator      string    // the user who created this plan baseline.
	Source       string    // how the user create this plan baseline.
	Created      time.Time // when the user created this plan baseline.
	Modified     time.Time // the last modified time.
	LastActive   time.Time // the last time the plan baseline used.
	LastVerified time.Time // the last time the plan baseline verified.

	// others user-friendly fields
	NormSQLText string // a normalized SQL statement, `select * from t where a<?`.
	SQLText     string // a sample SQL statement will literals, `select * from t where a<10`.
	PlanText    string // the execution plan.
	Comment     string // the comment of this plan baseline
	Extras      string // the extra information of this plan baseline.
}

type PlanBaselineHandle interface {
	// GetBaseline returns baselines of the specified conditions.
	// All returned baselines are read-only.
	GetBaseline(digest, sqlDigest, planDigest, status string) ([]*PlanBaseline, error)

	// AddUnVerifiedBaseline adds an unverified plan baseline.
	AddUnVerifiedBaseline(sqlDigest, planDigest, Outline string) error

	// TODO: how to know whether a baseline is stale or invalid due to something like schema changes?

	// CreateBaselineByPlanDigest creates a plan baseline from the specified plan digest.
	// CREATE PLAN BASELINE FROM HISTORY PLAN DIGEST {PlanDigest}
	CreateBaselineByPlanDigest(planDigest string) error

	// TODO: more ways to create a baseline.

	// UpdateBaselineStatus updates the status of the specified plan baseline.
	UpdateBaselineStatus(digest, sqlDigest, planDigest, status string) error

	// DropBaseline drops the specified plan baseline.
	DropBaseline(digest, sqlDigest, planDigest, status string) error

	// Purge automatically purges useless plan baselines, whose LastActive < NOW()-tidb_plan_baseline_retention_days.
	Purge() error
}

type planBaselineHandle struct {
	sPool SessionPool
}

func NewPlanBaselineHandle(sPool SessionPool) PlanBaselineHandle {
	return &planBaselineHandle{sPool: sPool}
}

func (h *planBaselineHandle) GetBaseline(digest, sqlDigest, planDigest, status string) (baselines []*PlanBaseline, err error) {
	err = callWithSCtx(h.sPool, false, func(sctx sessionctx.Context) error {
		var predicates []string
		if digest != "" {
			predicates = append(predicates, "digest = "+digest)
		}
		if sqlDigest != "" {
			predicates = append(predicates, "sql_digest = "+sqlDigest)
		}
		if planDigest != "" {
			predicates = append(predicates, "plan_digest = "+planDigest)
		}
		if status != "" {
			predicates = append(predicates, "status = "+status)
		}
		rows, _, err := execRows(sctx, "select * from mysql.plan_baselines where "+strings.Join(predicates, " and "))
		if err != nil {
			return err
		}
		baselines = rows2Baselines(rows)
		return nil
	})
	return
}

func (h *planBaselineHandle) AddUnVerifiedBaseline(sqlDigest, planDigest, outline string) error {
	// TODO
	return nil
}

func (h *planBaselineHandle) CreateBaselineByPlanDigest(planDigest string) error {
	// TODO
	return nil
}

func (h *planBaselineHandle) UpdateBaselineStatus(digest, sqlDigest, planDigest, status string) error {
	// TODO
	return nil
}

func (h *planBaselineHandle) DropBaseline(digest, sqlDigest, planDigest, status string) error {
	// TODO
	return nil
}

func (h *planBaselineHandle) Purge() error {
	// TODO
	return nil
}

func rows2Baselines(rows []chunk.Row) []*PlanBaseline {
	// TODO
	return nil
}
