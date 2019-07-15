package expression

import (
	"testing"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func genAbsCol(useVec bool) (*Column, *chunk.Chunk) {
	chunk.Vectorized = useVec
	ft := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := chunk.New(ft, 1024, 1024)
	for i := 0; i < 1024; i ++ {
		if i%5 == 3 {
			chk.AppendInt64(0, int64(-i))
		} else {
			chk.AppendInt64(0, int64(i))
		}
	}

	c := &Column{
		RetType: ft[0],
		Index:   0,
	}
	return c, chk
}

func BenchmarkAbsInt(b *testing.B) {
	col, chk := genAbsCol(false)
	exprs := []Expression{col}
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, col.RetType, exprs...)
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
	col, chk := genAbsCol(true)
	exprs := []Expression{col}
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, col.RetType, exprs...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.VecEvalInt(ctx, chk)
	}
}
