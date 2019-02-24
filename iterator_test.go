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
	"testing"
)

func TestIterator(t *testing.T) {
	tb := testTable("vanakam", "nanbare", 1, 100, 1)
	iter := tb.iter()
	records := 0
	for iter.has() {
		iter.next()
		records++
	}
	removeTestTable(1)
	if records != 99 {
		t.Fatalf("expected 99 records but got %d", records)
	}
}
