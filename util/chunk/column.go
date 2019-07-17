// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"reflect"
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (c *Column) AppendDuration(dur types.Duration) {
	c.AppendInt64(int64(dur.Duration))
}

func (c *Column) AppendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *Column) appendNameValue(name string, val uint64) {
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

func (c *Column) AppendJSON(j json.BinaryJSON) {
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

type Sel []uint16

type Column struct {
	length     int
	nullCount  int
	nullBitmap []byte
	offsets    []int64
	data       []byte
	elemBuf    []byte

	ft types.FieldType
}

func NewColumn(ft types.FieldType, length, cap uint16) *Column {
	c := new(Column)
	c.length = int(length)
	c.nullCount = 0
	c.nullBitmap = make([]byte, (cap+7)>>3)
	c.ft = ft
	typeSize := getFixedLen(&ft)
	if typeSize == varElemLen {
		c.offsets = make([]int64, length, cap)
		c.data = make([]byte, 0, 8*cap)
		c.elemBuf = nil
	} else {
		c.offsets = nil
		c.data = make([]byte, int(length)*typeSize, int(cap)*typeSize)
		c.elemBuf = make([]byte, typeSize)
	}
	return c
}

func (c *Column) CopyTo(result *Column) {
	result.length = c.length
	result.nullCount = c.nullCount
	result.nullBitmap = append(result.nullBitmap[:0], c.nullBitmap...)
	result.offsets = append(result.offsets[:0], c.offsets...)
	result.data = append(result.data[:0], c.data...)
	result.elemBuf = append(result.elemBuf[:0], c.elemBuf...)
}

func (c *Column) Type() *types.FieldType {
	return &c.ft
}

func (c *Column) isFixed() bool {
	return c.elemBuf != nil
}

func (c *Column) reset() {
	c.length = 0
	c.nullCount = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

func (c *Column) isNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

func (c *Column) IsNull(row uint16) bool {
	return c.nullBitmap[row>>3]&(1<<(row&7)) == 0
}

func (c *Column) SetNull(row uint16) {
	c.nullBitmap[row>>3] &= 7 ^ (1 << (row & 7))
}

func (c *Column) HasNull() bool {
	return c.nullCount > 0
}

func (c *Column) copyConstruct() *Column {
	newCol := &Column{length: c.length, nullCount: c.nullCount}
	newCol.nullBitmap = append(newCol.nullBitmap, c.nullBitmap...)
	newCol.offsets = append(newCol.offsets, c.offsets...)
	newCol.data = append(newCol.data, c.data...)
	newCol.elemBuf = append(newCol.elemBuf, c.elemBuf...)
	return newCol
}

func (c *Column) appendNullBitmap(notNull bool) {
	idx := c.length >> 3
	if idx >= len(c.nullBitmap) {
		c.nullBitmap = append(c.nullBitmap, 0)
	}
	if notNull {
		pos := uint(c.length) & 7
		c.nullBitmap[idx] |= byte(1 << pos)
	} else {
		c.nullCount++
	}
}

// appendMultiSameNullBitmap appends multiple same bit value to `nullBitMap`.
// notNull means not null.
// num means the number of bits that should be appended.
func (c *Column) appendMultiSameNullBitmap(notNull bool, num int) {
	numNewBytes := ((c.length + num + 7) >> 3) - len(c.nullBitmap)
	b := byte(0)
	if notNull {
		b = 0xff
	}
	for i := 0; i < numNewBytes; i++ {
		c.nullBitmap = append(c.nullBitmap, b)
	}
	if !notNull {
		c.nullCount += num
		return
	}
	// 1. Set all the remaining bits in the last slot of old c.numBitMap to 1.
	numRemainingBits := uint(c.length % 8)
	bitMask := byte(^((1 << numRemainingBits) - 1))
	c.nullBitmap[c.length/8] |= bitMask
	// 2. Set all the redundant bits in the last slot of new c.numBitMap to 0.
	numRedundantBits := uint(len(c.nullBitmap)*8 - c.length - num)
	bitMask = byte(1<<(8-numRedundantBits)) - 1
	c.nullBitmap[len(c.nullBitmap)-1] &= bitMask
}

func (c *Column) AppendNull() {
	c.appendNullBitmap(false)
	if c.isFixed() {
		c.data = append(c.data, c.elemBuf...)
	} else {
		c.offsets = append(c.offsets, c.offsets[c.length])
	}
	c.length++
}

func (c *Column) finishAppendFixed() {
	c.data = append(c.data, c.elemBuf...)
	c.appendNullBitmap(true)
	c.length++
}

func (c *Column) AppendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i
	c.finishAppendFixed()
}

func (c *Column) Int64s() []int64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&c.data))
	var res []int64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = c.length
	s.Cap = h.Cap / 8
	return res
}

func (c *Column) Float64s() []float64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&c.data))
	var res []float64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = c.length
	s.Cap = h.Cap / 8
	return res
}

func (c *Column) Length() uint16 {
	return uint16(c.length)
}

func (c *Column) GetInt64(rowID int) int64 {
	return *(*int64)(unsafe.Pointer(&c.data[rowID*8]))
}

func (c *Column) AppendUint64(u uint64) {
	*(*uint64)(unsafe.Pointer(&c.elemBuf[0])) = u
	c.finishAppendFixed()
}

func (c *Column) AppendFloat32(f float32) {
	*(*float32)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *Column) AppendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *Column) finishAppendVar() {
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int64(len(c.data)))
	c.length++
}

func (c *Column) AppendString(str string) {
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

func (c *Column) AppendBytes(b []byte) {
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

func (c *Column) AppendTime(t types.Time) {
	writeTime(c.elemBuf, t)
	c.finishAppendFixed()
}
