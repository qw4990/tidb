package chunk

import (
	"reflect"
	"testing"
	"unsafe"
)

const (
	Int64SizeBytes = int(unsafe.Sizeof(int64(0)))
)

type interfaceVec struct {
	data interface{}
}

func newInterfaceVec(n int) *interfaceVec {
	return &interfaceVec{make([]int64, n)}
}

func (iv *interfaceVec) Int64() []int64 {
	return iv.data.([]int64)
}

type bytesVec struct {
	data []byte
}

func newBytesVec(n int) *bytesVec {
	return &bytesVec{make([]byte, n*Int64SizeBytes)}
}

func (bv *bytesVec) Int64() []int64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&bv.data))
	var res []int64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int64SizeBytes
	s.Cap = h.Cap / Int64SizeBytes
	return res
}

type int64Vec struct {
	int64s []int64
}

func newBytesVecInt64(n int) *int64Vec {
	return &int64Vec{make([]int64, n)}
}

func (bvi *int64Vec) Int64() []int64 {
	return bvi.int64s
}

func BenchmarkVecImplInterfaceVec(b *testing.B) {
	iv := newInterfaceVec(1024)
	var sum int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += iv.Int64()[0]
	}
}

func BenchmarkVecImplBytesVec(b *testing.B) {
	bv := newBytesVec(1024)
	var sum int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += bv.Int64()[0]
	}
}

func BenchmarkVecImplInt64Vec(b *testing.B) {
	bvi := newBytesVecInt64(1024)
	var sum int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += bvi.Int64()[0]
	}
}
