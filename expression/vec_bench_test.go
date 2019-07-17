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
	for i := 0; i < 1024; i++ {
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

func BenchmarkAbsIntVecWithNull(b *testing.B) {
	exprs, _ := genAbsCol(10)
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

func TestAbsInt(t *testing.T) {
	exprs, chk := genAbsCol(0)
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}

	var results []int64
	it := chunk.NewIterator4Chunk(chk)
	for r := it.Begin(); r != it.End(); r = it.Next() {
		v, _, err := f.EvalInt(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, v)
	}

	exprs, _ = genAbsCol(0)
	f, err = NewFunction(ctx, ast.Abs, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}

	vecResult := chunk.NewColumn(*f.GetType(), 1024, 1024)
	if err := f.VecEval(ctx, nil, vecResult); err != nil {
		t.Fatal(err)
	}

	vecR := vecResult.Int64s()

	if len(vecR) != len(results) {
		t.Fatal()
	}
	for i := range vecR {
		if vecR[i] != results[i] {
			t.Fatal(i, vecR[i], results[i])
		}
	}
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
			colData: chk.Column(0),
		}, &Column{
			RetType: ft[1],
			Index:   1,
			colData: chk.Column(1),
		},
	}
	return exprs, chk
}

func BenchmarkPlusReal(b *testing.B) {
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
	nullLoopOptimize = false
	exprs, _ := genPlusCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Plus, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	result := chunk.NewColumn(*exprs[0].GetType(), 1024, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.VecEval(ctx, nil, result)
	}
}

func BenchmarkPlusRealVec(b *testing.B) {
	nullLoopOptimize = true
	exprs, _ := genPlusCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Plus, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	result := chunk.NewColumn(*exprs[0].GetType(), 1024, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.VecEval(ctx, nil, result)
	}
}

func TestPlusReal(t *testing.T) {
	exprs, chk := genPlusCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Plus, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}

	var results []float64
	it := chunk.NewIterator4Chunk(chk)
	for r := it.Begin(); r != it.End(); r = it.Next() {
		v, _, err := f.EvalReal(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, v)
	}

	nullLoopOptimize = true
	exprs, _ = genPlusCols()
	f, err = NewFunction(ctx, ast.Plus, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}

	vecResult := chunk.NewColumn(*exprs[0].GetType(), 1024, 1024)
	if err := f.VecEval(ctx, nil, vecResult); err != nil {
		t.Fatal(err)
	}

	vecR := vecResult.Float64s()
	if len(vecR) != len(results) {
		t.Fatal()
	}
	for i := range vecR {
		if vecR[i] != results[i] {
			t.Fatal(i, vecR[i], results[i])
		}
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
			colData: chk.Column(0),
		}, &Column{
			RetType: ft[1],
			Index:   1,
			colData: chk.Column(1),
		},
	}
	return exprs, chk
}

func BenchmarkGTInt(b *testing.B) {
	exprs, chk := genGTCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.GT, exprs[0].GetType(), exprs...)
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

func BenchmarkGTIntVec(b *testing.B) {
	exprs, _ := genGTCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.GT, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	result := chunk.NewColumn(*exprs[0].GetType(), 1024, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.VecEval(ctx, nil, result)
	}
}

func genConcatCols() ([]Expression, *chunk.Chunk) {
	st := types.NewFieldType(mysql.TypeVarString)
	nCols := 3
	str := "PingCAP"
	st.Flen = len(str)
	var ft []*types.FieldType
	for i := 0; i < nCols; i++ {
		ft = append(ft, st)
	}
	chk := chunk.New(ft, 1024, 1024)
	for i := 0; i < 1024; i++ {
		for c := 0; c < nCols; c++ {
			chk.AppendString(c, str)
		}
	}

	var exprs []Expression
	for c := 0; c < nCols; c++ {
		exprs = append(exprs, &Column{
			RetType: st,
			Index:   c,
			colData: chk.Column(c),
		})
	}
	return exprs, chk
}

func BenchmarkConcatStr(b *testing.B) {
	exprs, chk := genConcatCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Concat, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := chunk.NewIterator4Chunk(chk)
		for r := it.Begin(); r != it.End(); r = it.Next() {
			f.EvalString(ctx, r)
		}
	}
}

func BenchmarkConcatStrVec(b *testing.B) {
	exprs, _ := genConcatCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Concat, exprs[0].GetType(), exprs...)
	if err != nil {
		b.Fatal(err)
	}

	result := chunk.NewColumn(*exprs[0].GetType(), 1024, 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.VecEval(ctx, nil, result)
	}
}

func TestConcatStr(t *testing.T) {
	exprs, chk := genConcatCols()
	ctx := mock.NewContext()
	f, err := NewFunction(ctx, ast.Concat, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}

	var results []string
	it := chunk.NewIterator4Chunk(chk)
	for r := it.Begin(); r != it.End(); r = it.Next() {
		v, _, err := f.EvalString(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, v)
	}

	exprs, _ = genConcatCols()
	f, err = NewFunction(ctx, ast.Concat, exprs[0].GetType(), exprs...)
	if err != nil {
		t.Fatal(err)
	}

	vecResult := chunk.NewColumn(*exprs[0].GetType(), 1024, 1024)
	if err := f.VecEval(ctx, nil, vecResult); err != nil {
		t.Fatal(err)
	}

	if int(vecResult.Length()) != len(results) {
		t.Fatal()
	}
	for i := 0; i < len(results); i++ {
		if results[i] != vecResult.GetString(i) {
			t.Fatal()
		}
	}
}
