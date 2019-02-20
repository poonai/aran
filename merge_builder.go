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
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"

	"github.com/AndreasBriese/bbloom"
	"github.com/Sirupsen/logrus"
)

// It is used for merging table into buffer. usually meant to merge two table
type mergeTableBuilder struct {
	buf       *bytes.Buffer
	offsetMap map[uint32]uint32
	min       uint32
	max       uint32
}

func newTableMergeBuilder(size int) *mergeTableBuilder {
	buf := new(bytes.Buffer)
	buf.Grow(size)
	return &mergeTableBuilder{buf: buf, offsetMap: make(map[uint32]uint32), min: 0, max: 0}
}

func (m *mergeTableBuilder) Min() uint32 {
	return m.min
}

func (m *mergeTableBuilder) Max() uint32 {
	return m.max
}

// append data to the buffer
func (m *mergeTableBuilder) append(fp *os.File, limit int64) {
	writer := bufio.NewWriter(m.buf)
	n, err := io.CopyN(writer, fp, limit)
	if err != nil {
		logrus.Fatalf("merge builder: unable to append data while mering %s", err.Error())
	}
	if limit != n {
		logrus.Fatalf("merge builder: unable to append completely. expected %d but got %d", limit, n)
	}
}

func (m *mergeTableBuilder) add(kl, vl, key, val []byte, hash uint32) {
	offset := m.buf.Len()
	m.offsetMap[hash] = uint32(offset)
	m.setMax(hash)
	m.setMin(hash)
	n, err := m.buf.Write(kl)
	if err != nil {
		logrus.Fatalf("merge builder: unable to insert kl %s", err.Error())
	}
	if len(kl) != n {
		logrus.Fatalf("merge builder: kl is not written completly expected %d but got %d", len(kl), n)
	}
	n, err = m.buf.Write(vl)
	if err != nil {
		logrus.Fatalf("merge builder: unable to insert vl %s", err.Error())
	}
	if len(vl) != n {
		logrus.Fatalf("merge builder: vl is not written completly expected %d but got %d", len(kl), n)
	}
	n, err = m.buf.Write(key)
	if err != nil {
		logrus.Fatalf("merge builder: unable to insert key %s", err.Error())
	}
	if len(key) != n {
		logrus.Fatalf("merge builder: key is not written completly expected %d but got %d", len(kl), n)
	}
	n, err = m.buf.Write(val)
	if err != nil {
		logrus.Fatalf("merge builder: unable to insert val %s", err.Error())
	}
	if len(val) != n {
		logrus.Fatalf("merge builder: val is not written completly expected %d but got %d", len(kl), n)
	}
}

// merge hashmap and make filter for all the key, then write it to disk
func (m *mergeTableBuilder) mergeHashMap(left map[uint32]uint32, offsetAdder uint32) {
	for key, value := range left {
		m.offsetMap[key] = value + offsetAdder
		m.setMin(key)
		m.setMax(key)
	}
}

func (m *mergeTableBuilder) setMin(min uint32) {
	if m.min == 0 {
		m.min = min
		return
	}
	if m.min > min {
		m.min = min
	}
}

func (m *mergeTableBuilder) setMax(max uint32) {
	if m.max == 0 {
		m.max = max
		return
	}
	if m.max < max {
		m.max = max
	}
}

func (m *mergeTableBuilder) appendFileInfo(fi *fileInfo) {
	fib := make([]byte, 32)
	fi.Encode(fib)
	n, err := m.buf.Write(fib)
	if err != nil {
		logrus.Fatalf("merge builder: unable to append file info %s", err.Error())
	}
	if n != 32 {
		logrus.Fatalf("merge builder: unable to append file info completly expected %d got %d", 32, n)
	}
}

func (m *mergeTableBuilder) finish() []byte {
	el := len(m.offsetMap)
	filter := bbloom.New(float64(el), 0.01)
	buf := make([]byte, 4)
	for key := range m.offsetMap {
		binary.BigEndian.PutUint32(buf, key)
		filter.Add(buf)
	}
	mo := m.buf.Len()
	fJSON := filter.JSONMarshal()
	fl := len(fJSON)
	fi := &fileInfo{
		metaOffset: mo,
		minRange:   m.min,
		maxRange:   m.max,
		entries:    el,
		filterSize: fl,
	}
	e := gob.NewEncoder(m.buf)
	err := e.Encode(m.offsetMap)

	if err != nil {
		logrus.Fatalf("merge builder: unable to encode merged hashmap %s", err.Error())
	}
	n, err := m.buf.Write(fJSON)
	if err != nil {
		logrus.Fatalf("merge builder: unable to write filter to the buffer %s", err.Error())
	}
	if n != fl {
		logrus.Fatalf("merge builder: unable to write filter completley to the buffer expected %d got %d", fl, n)
	}
	m.appendFileInfo(fi)
	return m.buf.Bytes()
}
