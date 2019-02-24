// Copyright 2019 sch00lb0y.
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
package aran

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"hash/crc32"

	"github.com/AndreasBriese/bbloom"
)

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

type hashMap struct {
	buf           []byte
	currentOffset int
	minRange      uint32
	maxRange      uint32
	concurrentMap map[uint32]uint32
	size          int
	records       uint32
	sync.RWMutex
}

func newHashMap(size int) *hashMap {

	return &hashMap{
		buf:           make([]byte, size),
		currentOffset: 0,
		minRange:      0,
		maxRange:      0,
		concurrentMap: make(map[uint32]uint32, 0),
		size:          size,
		RWMutex:       sync.RWMutex{},
	}
}

func (h *hashMap) Set(key, value []byte) {
	h.Lock()
	c := crc32.New(CastagnoliCrcTable)
	c.Write(key)
	hash := c.Sum32()
	oldOffSet := h.currentOffset
	kl := len(key)
	vl := len(value)
	// each 4 byte is for storing key and value length
	binary.BigEndian.PutUint32(h.buf[h.currentOffset:], uint32(kl))
	h.currentOffset += 4
	binary.BigEndian.PutUint32(h.buf[h.currentOffset:], uint32(vl))
	h.currentOffset += 4
	copy(h.buf[h.currentOffset:h.currentOffset+kl], key)
	h.currentOffset += kl
	copy(h.buf[h.currentOffset:h.currentOffset+vl], value)
	h.currentOffset += vl
	h.concurrentMap[hash] = uint32(oldOffSet)
	h.Unlock()
	h.setRange(hash)
	atomic.AddUint32(&h.records, 1)
}

func (h *hashMap) Get(outkey []byte) ([]byte, bool) {
	h.RLock()
	defer h.RLock()
	c := crc32.New(CastagnoliCrcTable)
	c.Write(outkey)
	hash := c.Sum32()
	offset, ok := h.concurrentMap[hash]
	if !ok {
		return nil, ok
	}
	castedOffset := offset
	kl := binary.BigEndian.Uint32(h.buf[castedOffset : castedOffset+4])
	castedOffset += 4
	vl := binary.BigEndian.Uint32(h.buf[castedOffset : castedOffset+4])
	castedOffset += 4
	key := h.buf[castedOffset : castedOffset+kl]
	if bytes.Compare(key, outkey) != 0 {

		return nil, false
	}
	castedOffset += kl
	return h.buf[castedOffset : castedOffset+vl], true
}

func (h *hashMap) setRange(r uint32) {
	h.Lock()
	defer h.Unlock()
	h.setMinRage(r)
	h.setMaxRange(r)
}
func (h *hashMap) setMinRage(r uint32) {
	if h.minRange == 0 {
		h.minRange = r
		return
	}
	if h.minRange >= r {
		h.minRange = r
	}
}

func (h *hashMap) setMaxRange(r uint32) {
	if h.maxRange == 0 {
		h.maxRange = r
		return
	}
	if h.maxRange <= r {
		h.maxRange = r
	}
}

func (h *hashMap) isEnoughSpace(size int) bool {
	h.RLock()
	defer h.RUnlock()
	left := h.size - h.currentOffset
	if left < size {
		return false
	}
	return true
}

func (h *hashMap) occupiedSpace() int {
	return h.size - h.currentOffset
}

type fileInfo struct {
	metaOffset int
	entries    int
	minRange   uint32
	maxRange   uint32
	filterSize int
}

//TODO: avoid unnecessary converstion
func (fi *fileInfo) Decode(buf []byte) {
	_ = buf[31]
	fi.metaOffset = int(binary.BigEndian.Uint32(buf[0:4]))
	fi.entries = int(binary.BigEndian.Uint32(buf[4:8]))
	fi.minRange = binary.BigEndian.Uint32(buf[8:16])
	fi.maxRange = binary.BigEndian.Uint32(buf[16:24])
	fi.filterSize = int(binary.BigEndian.Uint32(buf[24:32]))
}

func (fi *fileInfo) Encode(buf []byte) {
	_ = buf[31]
	binary.BigEndian.PutUint32(buf[0:4], uint32(fi.metaOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(fi.entries))
	binary.BigEndian.PutUint32(buf[8:16], fi.minRange)
	binary.BigEndian.PutUint32(buf[16:24], fi.maxRange)
	binary.BigEndian.PutUint32(buf[24:32], uint32(fi.filterSize))

}

func (h *hashMap) toDisk(p string, idx uint32) {
	h.Lock()
	defer h.Unlock()
	filePath, err := filepath.Abs(p)
	if err != nil {
		panic("unable to form path for flushing the disk")
	}
	fp, err := os.Create(fmt.Sprintf("%s/%d.table", filePath, idx))
	if err != nil {
		panic(fmt.Sprintf("unable to flush the in-memory table %v", err))
	}
	fp.Write(h.buf[0:h.currentOffset])
	slots := h.Len()
	filter := bbloom.New(float64(slots), 0.01)

	for key, _ := range h.concurrentMap {
		// kl := binary.BigEndian.Uint32(h.buf[valueOffset : valueOffset+4])
		// valueOffset += 4
		// valueOffset += 4
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, key)
		filter.Add(buf) //h.buf[valueOffset : valueOffset+kl])
	}
	fib := make([]byte, 32)
	filterJSON := filter.JSONMarshal()
	fi := fileInfo{
		metaOffset: h.currentOffset,
		entries:    slots,
		minRange:   h.minRange,
		maxRange:   h.maxRange,
		filterSize: len(filterJSON),
	}
	fi.Encode(fib)
	metaBuf := new(bytes.Buffer)
	encoder := gob.NewEncoder(metaBuf)
	err = encoder.Encode(h.concurrentMap)
	if err != nil {
		panic("unable to create encoder")
	}
	fp.Write(metaBuf.Bytes())
	fp.Write(filterJSON)
	fp.Write(fib)
	fp.Close()
}

func (h *hashMap) Len() int {
	return len(h.concurrentMap)
}
