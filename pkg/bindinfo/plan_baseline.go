package bindinfo

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
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
	PlanHint   string // a set of hints corresponding to the SQL to generate the execution plan.
	Hint       *hint.HintsSet
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
	AddUnVerifiedBaseline(sqlDigest, planDigest, hintSet string) error

	// TODO: how to know whether a baseline is stale or invalid due to something like schema changes?

	// CreateBaselineByPlanDigest creates a plan baseline from the specified plan digest.
	// CREATE PLAN BASELINE FROM HISTORY PLAN DIGEST {PlanDigest}
	CreateBaselineByPlanDigest(planDigest string) error

	// TODO: more ways to create a baseline.

	// UpdateBaselineStatus updates the status of the specified plan baseline.
	UpdateBaselineStatus(digest, sqlDigest, planDigest, status string) error

	// DropBaseline drops the specified plan baseline.
	DropBaseline(digest, sqlDigest, planDigest, status string) error

	// UpdateCache updates the cache of plan baselines.
	UpdateCache() error

	// Purge automatically purges useless plan baselines, whose LastActive < NOW()-tidb_plan_baseline_retention_days.
	Purge() error
}

type baselineCache interface {
	Get(sqlDigest string) []*PlanBaseline
	Put(baseline []*PlanBaseline)
}

type planBaselineHandle struct {
	cache baselineCache

	sPool SessionPool
}

func NewPlanBaselineHandle(sPool SessionPool) PlanBaselineHandle {
	return &planBaselineHandle{sPool: sPool}
}

func (h *planBaselineHandle) GetBaseline(digest, sqlDigest, planDigest, status string) (baselines []*PlanBaseline, err error) {
	vals := h.cache.Get(sqlDigest)
	if vals != nil {
		return vals, nil
	}
	defer func() {
		if err != nil {
			h.cache.Put(baselines)
		}
	}()

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
	return callWithSCtx(h.sPool, false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx,
			`select plan_hint, digest, digest_text, query_sample_text, plan, charset, collation, schema_name
            from information_schema.cluster_statements_summary_history where plan_digest=?`, planDigest)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("not found")
		}
		planHint, sqlDigest, normSQLText, sqlText := rows[0].GetString(0), rows[0].GetString(1), rows[0].GetString(2), rows[0].GetString(3)
		planText, charset, collation, dbName := rows[0].GetString(4), rows[0].GetString(5), rows[0].GetString(6), rows[0].GetString(7)
		hintSet, err := planHintText2HintSet(sqlText, charset, collation, dbName, planHint)
		if err != nil {
			return err
		}

		b := &PlanBaseline{
			Digest:       planDigest, // always equal to planDigest ?
			SQLDigest:    sqlDigest,
			PlanDigest:   planDigest,
			PlanHint:     planHint,
			Hint:         hintSet,
			Status:       StateAccepted,
			Creator:      "user",
			Source:       "plan-digest",
			Created:      time.Now(),
			Modified:     time.Now(),
			LastActive:   time.Time{},
			LastVerified: time.Now(),
			NormSQLText:  normSQLText,
			SQLText:      sqlText,
			PlanText:     planText,
			Comment:      "",
			Extras:       "",
		}

		_, _, err = execRows(sctx, `insert into mysql.plan_baseline values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update`,
			b.Digest, b.SQLDigest, b.PlanDigest, b.PlanHint, b.Status, b.Creator, b.Source, b.Created,
			b.Modified, b.LastActive, b.LastVerified, b.NormSQLText, b.SQLText, b.PlanText, b.Comment, b.Extras)
		if err != nil {
			return err
		}

		h.cache.Put([]*PlanBaseline{b})
		return nil
	})
}

func (h *planBaselineHandle) UpdateBaselineStatus(digest, sqlDigest, planDigest, status string) error {
	// TODO
	return nil
}

func (h *planBaselineHandle) DropBaseline(digest, sqlDigest, planDigest, status string) error {
	// TODO
	return nil
}

func (h *planBaselineHandle) UpdateCache() error {
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

func planHintText2HintSet(sqlText, charset, collation, dbName, planHint string) (*hint.HintsSet, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sqlText, charset, collation)
	if err != nil {
		return nil, err
	}
	sqlWithHint := GenerateBindSQL(context.Background(), stmt, planHint, false, dbName)
	hintSet, _, _, err := hint.ParseHintsSet(p, sqlWithHint, charset, collation, dbName)
	return hintSet, err
}
