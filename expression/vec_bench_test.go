package expression

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"testing"
)

func genAbsCol(nullRate uint) ([]Expression, *chunk.Chunk) {
	ft := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := chunk.New(ft, 1024, 1024)
	for i := 0; i < 1024; i ++ {
		if nullRate > 0 && uint(i)%nullRate == 0 {
			chk.AppendNull(0)
			continue
		}
		if i%5 == 3 {
			chk.AppendInt64(0, int64(-i))
		} else {
			chk.AppendInt64(0, int64(i))
		}
	}

	c := &Column{
		RetType: ft[0],
		Index:   0,
		colData: chk.Column(0),
	}
	return []Expression{c}, chk
}

func BenchmarkAbsInt(b *testing.B) {
	exprs, chk := genAbsCol(0)
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := chunk.NewIterator4Chunk(chk)
		for r := it.Begin(); r != it.End(); r = it.Next() {
			f.EvalInt(ctx, r)
		}
	}
}

func BenchmarkAbsIntVec(b *testing.B) {
	exprs, _ := genAbsCol(0)
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	result := chunk.NewColumn(*f.GetType(), 1024, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.VecEval(ctx, nil, result)
	}
}
