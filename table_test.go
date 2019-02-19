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

func TestTableGet(t *testing.T) {
	hashMap := newHashMap(64 << 20)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("vanakam%d", i))
		value := []byte(fmt.Sprintf("nanbare%d", i))
		hashMap.Set(key, value)
	}
	hashMap.toDisk("./", 1)
	table := newTable("./", 1)
	inv, exist := table.Get([]byte(fmt.Sprintf("vanakam%d", 99)))
	if !exist {
		t.Fatal("key not found in the hashmap")
	}
	if bytes.Compare([]byte(fmt.Sprintf("nanbare%d", 99)), inv) != 0 {
		t.Fatalf("expected value %s but got value %s", "nanbare99", string(inv))
	}
	os.Remove("./1.table")
}
