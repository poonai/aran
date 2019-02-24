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
	"fmt"
	"os"
	"testing"
)

func testTable(key, value string, begin, end int, idx uint32) *table {
	mem := newHashMap(64 << 20)
	for ; begin < end; begin++ {
		key := []byte(fmt.Sprintf("%s%d", key, begin))
		value := []byte(fmt.Sprintf("%s%d", value, begin))
		mem.Set(key, value)
	}
	fmt.Println(mem.minRange)
	fmt.Println(mem.maxRange)
	mem.toDisk("./", idx)
	return newTable("./", idx)
}

func testValueExist(key, value string, tb *table, begin, end int, t *testing.T) {
	for ; begin < end; begin++ {
		key := []byte(fmt.Sprintf("%s%d", key, begin))
		value := []byte(fmt.Sprintf("%s%d", value, begin))
		inv, exist := tb.Get(key)
		if !exist {
			t.Fatalf("%s value not found", string(value))
		}
		if bytes.Compare(value, inv) != 0 {
			t.Fatalf("expected value %s but got %s", string(value), string(inv))
		}
	}
}

func removeTestTable(idx uint32) {
	os.Remove(fmt.Sprintf("./%d.table", idx))
}
func TestBuilder(t *testing.T) {
	t1 := testTable("hello", "value", 1, 100, 1)
	t2 := testTable("hello", "schoolboy", 101, 200, 2)
	builder := newTableMergeBuilder(int(t1.size + t2.size))
	t1.SeekBegin()
	t2.SeekBegin()
	builder.append(t1.fp, int64(t1.fileInfo.metaOffset))
	builder.append(t2.fp, int64(t2.fileInfo.metaOffset))
	builder.mergeHashMap(t1.offsetMap, 0)
	builder.mergeHashMap(t2.offsetMap, uint32(t1.fileInfo.metaOffset))
	buf := builder.finish()
	fp, _ := os.Create("3.table")
	fp.Write(buf)
	t3 := newTable("./", 3)
	testValueExist("hello", "value", t3, 1, 100, t)
	testValueExist("hello", "schoolboy", t3, 101, 200, t)
	removeTestTable(1)
	removeTestTable(2)
	removeTestTable(3)
}
