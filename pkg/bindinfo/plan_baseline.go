package bindinfo

import "time"

type PlanBaseline struct {
	Digest        string    // identifier of this plan baseline
	SQLDigest     string    // identifier of the SQL statement
	PlanDigest    string    // identifier of the execution plan
	Creator       string    // the user who created this plan baseline
	Source        string    // how the user create this plan baseline
	Created       time.Time // when the user created this plan baseline
	Modified      time.Time // the last modified time
	LastActive    time.Time // the last time the plan baseline used
	LastVerified  time.Time // the last time the plan baseline verified
	State         string    // the state of this plan baseline: accepted, preferred, unverified, disabled
	Comment       string    // the comment of this plan baseline
	SampleSQLStmt string    // the sample SQL statement
	Plan          string    // the execution plan
	Extras        string    // the extra information of this plan baseline
}

type PlanBaselineHandle interface {
	GetBaseline(sqlDigest, normalizedSQL, db string)
}
