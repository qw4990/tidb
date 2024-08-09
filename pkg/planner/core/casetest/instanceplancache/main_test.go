package instanceplancache

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

var (
	typeInt      = "int"
	typeVarchar  = "varchar(255)"
	typeFloat    = "float"
	typeDouble   = "double"
	typeDecimal  = "decimal(10,2)"
	typeDatetime = "datetime"
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

func randomIntVal() string {
	switch rand.Intn(4) {
	case 0: // null
		return "null"
	case 1: // 0/positive/negative
		return randomItem("0", "-1", "1", "100000000", "-1000000000")
	case 2: // maxint32
		return randomItem("2147483648", "2147483647", "2147483646", "-2147483648", "-2147483647", "-2147483646")
	case 3: // maxint64
		return randomItem("9223372036854775807", "9223372036854775808", "9223372036854775806", "-9223372036854775807", "-9223372036854775808", "-9223372036854775806")
	default:
		return randomItem(fmt.Sprintf("%v", rand.Intn(10)), fmt.Sprintf("-%v", rand.Intn(10)),
			fmt.Sprintf("%v", rand.Intn(10)+1000000), fmt.Sprintf("-%v", rand.Intn(10)+1000000),
			fmt.Sprintf("%v", rand.Intn(10)+100000000000), fmt.Sprintf("-%v", rand.Intn(10)+100000000000),
			fmt.Sprintf("%v", rand.Intn(10)+1000000000000000), fmt.Sprintf("-%v", rand.Intn(10)+1000000000000000))
	}
}

func prepareTableData(t string, rows int, colTypes []string) []string {
	colValues := make([][]string, len(colTypes))
	for i, colType := range colTypes {
		colValues[i] = make([]string, 0, rows)
		switch colType {
		case typeInt:
			for j := 0; j < rows; j++ {
				colValues[i] = append(colValues[i], randomIntVal())
			}
		default:
			panic("not implemented")
		}
	}
	var inserts []string
	for i := 0; i < rows; i++ {
		vals := make([]string, 0, len(colTypes))
		for j := range colTypes {
			vals = append(vals, colValues[j][i])
		}
		inserts = append(inserts, fmt.Sprintf("insert ignore into %s values (%s);", t, strings.Join(vals, ", ")))
	}
	return inserts
}

func prepareTables(n int) []string {
	nCols := 6
	sqls := make([]string, 0, n)
	for i := 0; i < n; i++ {
		cols := make([]string, 0, nCols)
		colNames := []string{"c0", "c1", "c2", "c3", "c4", "c5"}
		var colTypes []string
		for j := 0; j < nCols; j++ {
			colType := randomItem(typeInt)
			colTypes = append(colTypes, colType)
			cols = append(cols, fmt.Sprintf("c%d %v", j, colType))
		}
		pkCols := randomItems(colNames...)
		idx1 := randomItems(colNames...)
		idx2 := randomItems(colNames...)
		sqls = append(sqls, fmt.Sprintf("create table t%d (%s, primary key (%s), index idx1 (%s), index idx2 (%s));",
			i, strings.Join(cols, ", "), strings.Join(pkCols, ", "), strings.Join(idx1, ", "), strings.Join(idx2, ", ")))

		sqls = append(sqls, prepareTableData(fmt.Sprintf("t%d", i), 100, colTypes)...)
	}
	return sqls
}

func TestPlanStatsLoad(t *testing.T) {
	for _, s := range prepareTables(10) {
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
