package expression

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
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
	}
	return []Expression{c}, chk
}

func genPlusCols() ([]Expression, *chunk.Chunk) {
	ft := []*types.FieldType{types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeDouble)}
	chk := chunk.New(ft, 1024, 1024)
	for i := 0; i < 1024; i++ {
		chk.AppendFloat64(0, float64(i))
		chk.AppendFloat64(1, float64(i+1))
	}

	exprs := []Expression{
		&Column{
			RetType: ft[0],
			Index:   0,
		}, &Column{
			RetType: ft[1],
			Index:   1,
		},
	}
	return exprs, chk
}

func BenchmarkAbsInt(b *testing.B) {
	chunk.Vectorized = false
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
	chunk.Vectorized = true
	exprs, chk := genAbsCol(0)
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	var buf *chunk.Vec
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = f.VecEvalInt(ctx, chk, buf)
	}
}

func BenchmarkAbsIntVecWithNull(b *testing.B) {
	chunk.Vectorized = true
	exprs, chk := genAbsCol(10)
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	var buf *chunk.Vec
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = f.VecEvalInt(ctx, chk, buf)
	}
}

func TestAbsIntVec(t *testing.T) {
	chunk.Vectorized = false
	exprs, chk := genAbsCol(0)
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}
	results := make([]int64, 0, 1024)
	nulls := make([]bool, 0, 1024)
	it := chunk.NewIterator4Chunk(chk)
	for r := it.Begin(); r != it.End(); r = it.Next() {
		v, isNull, err := f.EvalInt(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, v)
		nulls = append(nulls, isNull)
	}

	chunk.Vectorized = true
	exprs, chk = genAbsCol(0)
	f, err = NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.VecEvalInt(ctx, chk, nil)
	if err != nil {
		t.Fatal(err)
	}

	vResults := v.Int64()
	for i := range results {
		if vResults[i] != results[i] {
			t.Fatal(i)
		}
	}
}

func BenchmarkPlusReal(b *testing.B) {
	chunk.Vectorized = false
	exprs, chk := genPlusCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Plus, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := chunk.NewIterator4Chunk(chk)
		for r := it.Begin(); r != it.End(); r = it.Next() {
			f.EvalReal(ctx, r)
		}
	}
}

func BenchmarkPlusRealVecWithoutLoopOpt(b *testing.B) {
	chunk.Vectorized = true
	nullLoopOptimize = false
	exprs, chk := genPlusCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Plus, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	var buf *chunk.Vec
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = f.VecEvalReal(ctx, chk, buf)
	}
}

func BenchmarkPlusRealVec(b *testing.B) {
	chunk.Vectorized = true
	nullLoopOptimize = true
	exprs, chk := genPlusCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Plus, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	var buf *chunk.Vec
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = f.VecEvalReal(ctx, chk, buf)
	}
}

func genGTCols() ([]Expression, *chunk.Chunk) {
	ft := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)}
	chk := chunk.New(ft, 1024, 1024)
	for i := 0; i < 1024; i++ {
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, int64(1024-i))
	}

	exprs := []Expression{
		&Column{
			RetType: ft[0],
			Index:   0,
		}, &Column{
			RetType: ft[1],
			Index:   1,
		},
	}
	return exprs, chk
}

func BenchmarkGTInt(b *testing.B) {
	chunk.Vectorized = false
	exprs, chk := genGTCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.GT, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		it := chunk.NewIterator4Chunk(chk)
		for r := it.Begin(); r != it.End(); r = it.Next() {
			f.EvalInt(ctx, r)
		}
	}
}

func BenchmarkGTIntVec(b *testing.B) {
	chunk.Vectorized = true
	exprs, chk := genGTCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.GT, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		f.VecEvalInt(ctx, chk, nil)
	}
}

var (
	fixedSeeds []int
)

func init() {
	for i := 0; i < 10; i++ {
		fixedSeeds = append(fixedSeeds, rand.Intn(1024))
	}
}

/*
	4 columns, all values are in [0, 1024);
	4 filters, all are col[i] > fixedSeeds[i];
 */
func genVecFilterCols(ctx sessionctx.Context) ([]Expression, *chunk.Chunk) {
	tll := types.NewFieldType(mysql.TypeLonglong)
	nCol := 4
	var ft []*types.FieldType
	for i := 0; i < nCol; i++ {
		ft = append(ft, tll)
	}

	chk := chunk.New(ft, 1024, 1024)
	for i := 0; i < 1024; i++ {
		for c := 0; c < nCol; c++ {
			chk.AppendInt64(c, int64(uint(i*fixedSeeds[c])%1024))
		}
	}

	exprs := make([]Expression, 0, 4)
	for i := 0; i < nCol; i++ {
		col := &Column{
			RetType: tll,
			Index:   i,
		}
		constVal := &Constant{
			RetType: tll,
			Value:   types.NewIntDatum(int64(fixedSeeds[i])),
		}

		gt, err := NewFunction(ctx, ast.GT, tll, col, constVal)
		if err != nil {
			panic(err)
		}
		exprs = append(exprs, gt)
	}
	return exprs, chk
}

func BenchmarkVectorizedFilter(b *testing.B) {
	chunk.Vectorized = false
	ctx := mock.NewContext()
	filters, chk := genVecFilterCols(ctx)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sel := make([]bool, 1024)
		VectorizedFilter(ctx, filters, chunk.NewIterator4Chunk(chk), sel)
	}
}

func BenchmarkVectorizedFilter2(b *testing.B) {
	chunk.Vectorized = true
	ctx := mock.NewContext()
	filters, chk := genVecFilterCols(ctx)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VectorizedFilter2(ctx, filters, chk)
	}
}

func TestVectorizedFilter(t *testing.T) {
	chunk.Vectorized = false
	ctx := mock.NewContext()
	filters, chk := genVecFilterCols(ctx)
	sel := make([]bool, 1024)
	sel, _ = VectorizedFilter(ctx, filters, chunk.NewIterator4Chunk(chk), sel)
	rSel := make([]chunk.VecSize, 0, len(sel))
	for i, v := range sel {
		if v {
			rSel = append(rSel, chunk.VecSize(i))
		}
	}

	chunk.Vectorized = true
	filters, chk = genVecFilterCols(ctx)
	vSel, _ := VectorizedFilter2(ctx, filters, chk)

	for i := range vSel {
		if vSel[i] != rSel[i] {
			fmt.Println(rSel)
			fmt.Println(vSel)
			t.Fatal(i)
		}
	}
}

func BenchmarkChunkIter(b *testing.B) {
	chunk.Vectorized = false
	_, chk := genGTCols()
	for i := 0; i < b.N; i++ {
		it := chunk.NewIterator4Chunk(chk)
		var sum int64
		for r := it.Begin(); r != it.End(); r = it.Next() {
			sum += r.GetInt64(0)
		}
	}
}

func BenchmarkChunkIterRowOnVec(b *testing.B) {
	chunk.Vectorized = true
	_, chk := genGTCols()
	for i := 0; i < b.N; i++ {
		it := chunk.NewIterator4Chunk(chk)
		var sum int64
		for r := it.Begin(); r != it.End(); r = it.Next() {
			sum += r.GetInt64(0)
		}
	}
}
