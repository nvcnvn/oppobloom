// Copyright 2012 Jeff Hodges. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package oppobloom implements a filter data structure that may report false
// negatives but no false positives.
package oppobloom

import (
	"bytes"
	"crypto/md5"
	"errors"
	"hash"
	"math"
	"sync/atomic"
	"unsafe"
)

type Filter struct {
	array          []*[]byte
	sizeMask       uint32
	forgetedUnsafe unsafe.Pointer
}

var ErrSizeTooLarge = errors.New("oppobloom: size given too large to round to a power of 2")
var ErrSizeTooSmall = errors.New("oppobloom: filter cannot have a zero or negative size")
var MaxFilterSize = 1 << 30

func NewFilter(size int) (*Filter, error) {
	if size > MaxFilterSize {
		return nil, ErrSizeTooLarge
	}
	if size <= 0 {
		return nil, ErrSizeTooSmall
	}
	// round to the next largest power of two
	size = int(math.Pow(2, math.Ceil(math.Log2(float64(size)))))
	slice := make([]*[]byte, size)
	sizeMask := uint32(size - 1)

	forgetedHolder := []byte{}
	return &Filter{slice, sizeMask, unsafe.Pointer(&forgetedHolder)}, nil
}

// Contains adds id to the hashmap and then returns true if id already exist.
func (f *Filter) Contains(id []byte) bool {
	oldId := getAndSet(f.array, f.caculateIndex(id), id)
	return bytes.Equal(oldId, id)
}

// Forget removes id if it in the filter.
func (f *Filter) Forget(id []byte) {
	indexPtr := (*unsafe.Pointer)(unsafe.Pointer(&f.array[f.caculateIndex(id)]))
	oldIdUnsafe := atomic.LoadPointer(indexPtr)
	atomic.CompareAndSwapPointer(indexPtr, oldIdUnsafe, f.forgetedUnsafe)
}

func (f *Filter) caculateIndex(id []byte) int32 {
	h := md5UintHash{md5.New()}
	h.Write(id)
	uindex := h.Sum32() & f.sizeMask

	return int32(uindex)
}

// Size return the size of the hashmap
func (f *Filter) Size() int {
	return len(f.array)
}

type md5UintHash struct {
	hash.Hash // a hack with knowledge of how md5 works
}

func (m md5UintHash) Sum32() uint32 {
	sum := m.Sum(nil)
	x := uint32(sum[0])
	for _, val := range sum[1:3] {
		x = x << 3
		x += uint32(val)
	}
	return x
}

// Returns the id that was in the slice at the given index after putting the
// new id in the slice at that index, atomically.
func getAndSet(arr []*[]byte, index int32, id []byte) []byte {
	indexPtr := (*unsafe.Pointer)(unsafe.Pointer(&arr[index]))
	idUnsafe := unsafe.Pointer(&id)
	var oldId []byte
	for {
		oldIdUnsafe := atomic.LoadPointer(indexPtr)
		if atomic.CompareAndSwapPointer(indexPtr, oldIdUnsafe, idUnsafe) {
			oldIdPtr := (*[]byte)(oldIdUnsafe)
			if oldIdPtr != nil {
				oldId = *oldIdPtr
			}
			break
		}
	}
	return oldId
}
