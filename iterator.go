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
	"encoding/binary"
	"os"

	"github.com/Sirupsen/logrus"
)

// simple forward iterator
type iterator struct {
	currentOffset int
	metaOffset    int
	fp            *os.File
}

func newIterator(fp *os.File, metaOffset int) *iterator {
	fp.Seek(0, 0)
	return &iterator{currentOffset: 0, metaOffset: metaOffset, fp: fp}
}

func (t *iterator) has() bool {
	has := t.currentOffset != t.metaOffset
	if has == false {
		t.fp.Close()
	}
	return has
}

// kl, vl, key, val
func (t *iterator) next() ([]byte, []byte, []byte, []byte) {
	buf := make([]byte, 8)
	n, err := t.fp.Read(buf)
	if err != nil {
		logrus.Fatalf("iterator: failed during reading key and value length %s", err.Error())
	}
	if n != 8 {
		logrus.Fatalf("iterator: failed to read key and value length expected 8 but got %d", n)
	}
	kl := binary.BigEndian.Uint32(buf[0:4])
	vl := binary.BigEndian.Uint32(buf[4:8])
	bufval := make([]byte, kl+vl)
	n, err = t.fp.Read(bufval)
	if err != nil {
		logrus.Fatalf("iterator: failed during reading key and value  %s", err.Error())
	}
	if n != int(kl+vl) {
		logrus.Fatalf("iterator: failed to read key and value  expected %d but got %d", kl+vl, n)
	}
	t.currentOffset += 8 + int(kl) + int(vl)
	return buf[0:4], buf[4:8], bufval[0:kl], bufval[kl : kl+vl]
}
