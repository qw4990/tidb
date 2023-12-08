package bindinfo

import "time"

type PlanBaseline struct {
	// core fields
	Digest     string // identifier of this plan baseline.
	SQLDigest  string // identifier of the SQL statement.
	PlanDigest string // identifier of the execution plan.
	Outline    string // a set of hints corresponding to the SQL to generate the execution plan.
	State      string // the state of this plan baseline: accepted, preferred, unverified, disabled.

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
	GetBaseline(sqlDigest, normalizedSQL string)
}
