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

func TestLowerRange(t *testing.T) {
	tr := newTree()
	tr.insert(45, 1)
	tr.insert(20, 2)
	tr.insert(80, 3)
	tr.insert(70, 4)
	tr.insert(50, 5)
	n := tr.findLargestLowerRange(72)
	if n.lowerRange != 70 {
		t.Fatalf("expected 70 but got %d", n.lowerRange)
	}
	n = tr.findLargestLowerRange(20)
	if n.lowerRange != 20 {
		t.Fatalf("expected 20 but got %d", n.lowerRange)
	}
	n = tr.findLargestLowerRange(92)
	if n.lowerRange != 80 {
		t.Fatalf("expected 80 but got %d", n.lowerRange)
	}
	n = tr.findLargestLowerRange(69)
	if n.lowerRange != 50 {
		t.Fatalf("expected 50 but got %d", n.lowerRange)
	}
	n = tr.findLargestLowerRange(2)
	if n != nil {
		t.Fatalf("expected nil node but got %d", n)
	}

	ns := tr.findAllLargestRange(72)
	if len(ns) != 4 {
		t.Fatalf("expected 4 but got %d", len(ns))
	}
	for i := range ns {
		if i == 0 {
			continue
		}
		if ns[i].lowerRange > ns[i-1].lowerRange {
			t.Fatalf("expected in decrement order")
		}
	}
}

func TestDeleteTable(t *testing.T) {
	tr := newTree()
	tr.insert(34, 1)
	tr.insert(32, 5)
	tr.insert(31, 4)
	tr.insert(34, 20)
	tr.insert(32, 24)
	tr.insert(31, 10)
	tr.deleteTable(1)
	tr.deleteTable(5)
	tr.deleteTable(4)
	tr.deleteTable(20)
	tr.deleteTable(24)
	tr.deleteTable(10)
	if tr.root != nil {
		t.Fatalf("expected root to be nil but got %+v", tr.root)
	}
}
