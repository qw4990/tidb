package instanceplancache

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func randomItem(items ...string) string {
	return items[rand.Intn(len(items))]
}

func randomItems(items ...string) []string {
	n := rand.Intn(len(items))
	res := make([]string, 0, n)
	for i := 0; i < n; i++ {
		res = append(res, randomItem(items...))
	}
	return res
}

func randomTables(n int) []string {
	nCols := 6
	sqls := make([]string, 0, n)
	for i := 0; i < n; i++ {
		cols := make([]string, 0, nCols)
		colNames := []string{"c0", "c1", "c2", "c3", "c4", "c5"}
		for j := 0; j < nCols; j++ {
			cols = append(cols, fmt.Sprintf("c%d %v", j, randomItem("int", "varchar(255)", "float", "double", "decimal(10,2)", "datetime")))
		}
		pkCols := randomItems(colNames...)
		idx1 := randomItems(colNames...)
		idx2 := randomItems(colNames...)
		sqls = append(sqls, fmt.Sprintf("create table t%d (%s, primary key (%s), index idx1 (%s), index idx2 (%s));",
			i, strings.Join(cols, ", "), strings.Join(pkCols, ", "), strings.Join(idx1, ", "), strings.Join(idx2, ", ")))
	}
	return sqls
}

func TestPlanStatsLoad(t *testing.T) {
	for _, s := range randomTables(10) {
		fmt.Println(s)
	}
}

var queryPattern []string = []string{
	`select * from {T} where c0 = ?`,
}

type CaseSchema struct {
}

type CaseSQL struct {
}

/*

select * from {T} t where col=?;

*/
