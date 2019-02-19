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
	"path/filepath"
	"testing"
)

func TestGetSet(t *testing.T) {
	hashMap := newHashMap(1024)
	key := []byte("vanakam")
	value := []byte("nanbare")
	hashMap.Set(key, value)
	inv, exist := hashMap.Get(key)
	if !exist {
		t.Fatal("key not found in the hashmap")
	}
	if bytes.Compare(value, inv) != 0 {
		t.Fatalf("expected value %s but got value %s", string(value), string(inv))
	}

}

func TestGetSet100(t *testing.T) {
	hashMap := newHashMap(64 << 20)
	for i := 0; i < 100; i++ {
		key := []byte("vanakam" + string(i))
		value := []byte("nanbare" + string(i))
		hashMap.Set(key, value)
		inv, exist := hashMap.Get(key)
		if !exist {
			t.Fatal("key not found in the hashmap")
		}
		if bytes.Compare(value, inv) != 0 {
			t.Fatalf("expected value %s but got value %s", string(value), string(inv))
		}
	}
}

func TestSaveToFile(t *testing.T) {
	hashMap := newHashMap(64 << 20)
	for i := 0; i < 100; i++ {
		key := []byte("vanakam" + string(i))
		value := []byte("nanbare" + string(i))

		hashMap.Set(key, value)
		inv, exist := hashMap.Get(key)
		if !exist {
			t.Fatal("key not found in the hashmap")
		}
		if bytes.Compare(value, inv) != 0 {
			t.Fatalf("expected value %s but got value %s", string(value), string(inv))
		}
	}
	hashMap.toDisk("./", 1)
	filePath, err := filepath.Abs("./")
	if err != nil {
		panic("unable to form path for flushing the disk")
	}

	if _, err := os.Stat(fmt.Sprintf("%s/%d.table", filePath, 1)); os.IsNotExist(err) {
		panic("file not exist")
	}
	os.Remove(fmt.Sprintf("%s/%d.table", filePath, 1))
}
