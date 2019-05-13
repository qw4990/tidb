package chunk

import (
	"runtime"
	"sync/atomic"
	"testing"
)

func BenchmarkAllocMultiBuf(b *testing.B) {
	bufSize := []int{32, 64, 128, 256, 512, 1024, 4096}
	n := len(bufSize)
	m := NewMultiBufAllocator(8, 15, 32)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		var i uint32
		for p.Next() {
			c := bufSize[int(atomic.AddUint32(&i, 1))%n]
			buf := m.Alloc(0, c)
			buf = append(buf, '0')
			m.Free(buf)
		}
	})
}

func BenchmarkAllocGolangSlice(b *testing.B) {
	bufSize := []int{32, 64, 128, 256, 512, 1024, 4096}
	n := len(bufSize)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		var i uint32
		for p.Next() {
			c := bufSize[int(atomic.AddUint32(&i, 1))%n]
			buf := make([]byte, 0, c)
			buf = append(buf, '0')
		}
	})
}

func BenchmarkAllocMultiBufSingle(b *testing.B) {
	bufSize := []int{32, 64, 128, 256, 512, 1024, 4096}
	n := len(bufSize)
	m := NewMultiBufAllocator(8, 15, 32)
	runtime.GC()
	b.ResetTimer()
	var k uint32
	for i := 0; i < b.N; i++ {
		c := bufSize[int(atomic.AddUint32(&k, 1))%n]
		buf := m.Alloc(0, c)
		buf = append(buf, '0')
		m.Free(buf)
	}
}

func BenchmarkAllocGolangSliceSingle(b *testing.B) {
	bufSize := []int{32, 64, 128, 256, 512, 1024, 4096}
	n := len(bufSize)
	runtime.GC()
	b.ResetTimer()
	var k uint32
	for i := 0; i < b.N; i++ {
		c := bufSize[int(atomic.AddUint32(&k, 1))%n]
		buf := make([]byte, 0, c)
		buf = append(buf, '0')
	}
}
