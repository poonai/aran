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
	"hash/crc32"
	"os"
	"sync"
	"syscall"

	"github.com/AndreasBriese/bbloom"
)

// read only
type table struct {
	data      []byte
	path      string
	fileInfo  *fileInfo
	size      int64
	fp        *os.File
	stat      os.FileInfo
	filter    *bbloom.Bloom
	offsetMap map[uint32]uint32
	sync.RWMutex
	idx uint32
}

func newTable(path string, idx uint32) *table {
	path = giveTablePath(path, idx)
	fp, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("unable to open level files %v", err))
	}
	stat, err := os.Stat(path)
	if err != nil {
		panic("unable to get the file state")
	}
	data, err := syscall.Mmap(int(fp.Fd()), int64(0), int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic("unable to mmap")
	}
	fi := &fileInfo{}
	// last 32 byte is file info
	//fiData := make([]byte, 32)
	//fp.ReadAt(fiData)
	fi.Decode(data[stat.Size()-32 : stat.Size()])

	filter := bbloom.JSONUnmarshal(data[stat.Size()-32-int64(fi.filterSize) : stat.Size()-32])
	metBuf := new(bytes.Buffer)
	metBuf.Write(data[fi.metaOffset : stat.Size()-32-int64(fi.filterSize)])
	offsetMap := make(map[uint32]uint32, 0)
	decoder := gob.NewDecoder(metBuf)
	err = decoder.Decode(&offsetMap)
	if err != nil {
		panic("unable to decode the map")
	}
	return &table{
		data:      data,
		path:      path,
		fileInfo:  fi,
		size:      stat.Size(),
		fp:        fp,
		stat:      stat,
		filter:    &filter,
		offsetMap: offsetMap,
		idx:       idx,
	}
}

func (t *table) SeekBegin() {
	t.fp.Seek(0, 0)
}

func (t *table) ID() uint32 {
	return t.idx
}

// only get is possible
func (t *table) Get(key []byte) ([]byte, bool) {
	c := crc32.New(CastagnoliCrcTable)
	c.Write(key)
	hash := c.Sum32()
	if !t.filterHas(hash) {
		return nil, false
	}
	valueOffset, ok := t.offsetMap[hash]
	if !ok {
		return nil, false
	}
	kl := binary.BigEndian.Uint32(t.data[valueOffset : valueOffset+4])
	valueOffset += 4
	vl := binary.BigEndian.Uint32(t.data[valueOffset : valueOffset+4])
	valueOffset += 4
	valueOffset += kl
	return t.data[valueOffset : valueOffset+vl], true
}

func (t *table) filterHas(hash uint32) bool {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, hash)
	return t.filter.Has(buf)
}
func (t *table) iter() *iterator {
	return newIterator(t.fp, t.fileInfo.metaOffset)
}
func (t *table) close() {
	t.fp.Close()
}

func (t *table) entries() []uint32 {
	entries := make([]uint32, 0)
	for key := range t.offsetMap {
		entries = append(entries, key)
	}
	return entries
}
