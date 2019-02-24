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
	"fmt"
	"testing"
)

func testDB() *db {
	return &db{
		manifest:  &manifest{L0Files: make([]tableManifest, 0), L1Files: make([]tableManifest, 0), NextIDX: 5},
		l0handler: newLevelHanlder(),
		l1handler: newLevelHanlder(),
	}
}
func TestL0(t *testing.T) {
	db := testDB()
	db.absPath = "./"
	t1 := testTable("vanakam", "nanbare", 1, 100, 1)
	t2 := testTable("vanakam", "nanbare", 1, 100, 2)
	t3 := testTable("vanakam", "nanbare", 1, 100, 3)
	t4 := testTable("vanakam", "nanbare", 1, 100, 4)
	db.l0handler.addTable(t1, 1)
	db.l0handler.addTable(t2, 2)
	db.l0handler.addTable(t3, 3)
	db.l0handler.addTable(t4, 4)
	db.manifest.addl0file(uint32(t1.fileInfo.entries), t1.fileInfo.minRange, t1.fileInfo.maxRange, int(t1.size), 1)
	db.manifest.addl0file(uint32(t2.fileInfo.entries), t2.fileInfo.minRange, t2.fileInfo.maxRange, int(t2.size), 2)
	db.manifest.addl0file(uint32(t3.fileInfo.entries), t3.fileInfo.minRange, t3.fileInfo.maxRange, int(t3.size), 3)
	db.manifest.addl0file(uint32(t4.fileInfo.entries), t4.fileInfo.minRange, t4.fileInfo.maxRange, int(t4.size), 4)
	db.L0Compaction()
	if len(db.manifest.L0Files) != 2 {
		t.Fatalf("expected 2 level 0 files but got %d", len(db.manifest.L0Files))
	}
	if len(db.manifest.L1Files) != 1 {
		t.Fatalf("expected 1 level 1 files but got %d", len(db.manifest.L1Files))
	}
	removeTestTable(3)
	removeTestTable(4)
	// 1 and 2 has merged as 6
	removeTestTable(6)
}

func TestUnion(t *testing.T) {
	db := testDB()
	db.absPath = "./"
	t1 := testTable("vanakam", "nanbare", 1, 100, 1)
	t2 := testTable("vanakam", "nanbare", 1, 100, 2)
	db.l0handler.addTable(t1, 1)
	db.l1handler.addTable(t2, 2)
	db.manifest.addl0file(uint32(t1.fileInfo.entries), t1.fileInfo.minRange, t1.fileInfo.maxRange, int(t1.size), 1)
	db.manifest.addl1file(uint32(t2.fileInfo.entries), t2.fileInfo.minRange, t2.fileInfo.maxRange, int(t2.size), 2)
	p := db.manifest.findL1Policy(db.manifest.L0Files[0])
	if p.policy != UNION {
		t.Fatalf("expected UNION %d but got %d", UNION, p.policy)
	}
	db.handleUnion(p, db.manifest.L0Files[0])
	if len(db.manifest.L0Files) != 0 {
		t.Fatalf("expected 0 level0 files but got %d", len(db.manifest.L0Files))
	}
	if len(db.manifest.L1Files) != 1 {
		t.Fatalf("expected 1 level1 files but got %d", len(db.manifest.L1Files))
	}
	removeTestTable(6)
	db = testDB()
	db.absPath = "./"
	t1 = testTable("vanakam", "nanbare", 40, 100, 1)
	t2 = testTable("vanakam", "nanbare", 1, 100, 2)
	db.l0handler.addTable(t1, 1)
	db.l1handler.addTable(t2, 2)
	db.manifest.addl0file(uint32(t1.fileInfo.entries), t1.fileInfo.minRange, t1.fileInfo.maxRange, int(t1.size), 1)
	db.manifest.addl1file(uint32(t2.fileInfo.entries), t2.fileInfo.minRange, t2.fileInfo.maxRange, int(t2.size), 2)
	p = db.manifest.findL1Policy(db.manifest.L0Files[0])
	if p.policy != UNION {
		t.Fatalf("expected UNION %d but got %d", UNION, p.policy)
	}
	db.handleUnion(p, db.manifest.L0Files[0])
	if len(db.manifest.L0Files) != 0 {
		t.Fatalf("expected 0 level0 files but got %d", len(db.manifest.L0Files))
	}
	if len(db.manifest.L1Files) != 1 {
		t.Fatalf("expected 1 level1 files but got %d", len(db.manifest.L1Files))
	}
	removeTestTable(6)
}

func TestNotUnion(t *testing.T) {
	db := testDB()
	db.absPath = "./"
	t1 := testTable("vanakam", "nanbare", 1, 100, 1)
	t2 := testTable("vanakam", "nanbare", 3000, 4000, 2)
	db.l0handler.addTable(t1, 1)
	db.l1handler.addTable(t2, 2)
	t2.fileInfo.minRange = t1.fileInfo.maxRange + 1
	t2.fileInfo.maxRange = t1.fileInfo.maxRange + t1.fileInfo.minRange
	db.manifest.addl0file(uint32(t1.fileInfo.entries), t1.fileInfo.minRange, t1.fileInfo.maxRange, int(t1.size), 1)
	db.manifest.addl1file(uint32(t2.fileInfo.entries), t2.fileInfo.minRange, t2.fileInfo.maxRange, int(t2.size), 2)
	p := db.manifest.findL1Policy(db.manifest.L0Files[0])
	if p.policy != NOTUNION {
		t.Fatalf("expected NOTUNION %d but got %d", NOTUNION, p.policy)
	}
	db.handleNotUnion(p, db.manifest.L0Files[0])
	if len(db.manifest.L0Files) != 0 {
		t.Fatalf("expected 0 level0 files but got %d", len(db.manifest.L0Files))
	}
	if len(db.manifest.L1Files) != 2 {
		t.Fatalf("expected 2 level1 files but got %d", len(db.manifest.L1Files))
	}
	removeTestTable(1)
	removeTestTable(2)
}

func TestOverlapping(t *testing.T) {
	db := testDB()
	db.absPath = "./"
	t1 := testTable("vanakam", "nanbare", 1, 100, 1)
	t2 := testTable("vanakam", "nanbare", 50, 10000, 2)

	t2.fileInfo.minRange = t1.fileInfo.minRange + 100
	t2.fileInfo.maxRange = t1.fileInfo.maxRange + t1.fileInfo.minRange
	db.l0handler.addTable(t1, 1)
	db.l1handler.addTable(t2, 2)
	db.manifest.addl0file(uint32(t1.fileInfo.entries), t1.fileInfo.minRange, t1.fileInfo.maxRange, int(t1.size), 1)
	db.manifest.addl1file(uint32(t2.fileInfo.entries), t2.fileInfo.minRange, t2.fileInfo.maxRange, int(t2.size), 2)
	fmt.Printf("%d %d %d %d", t1.fileInfo.minRange, t1.fileInfo.maxRange, t2.fileInfo.minRange, t2.fileInfo.maxRange)
	p := db.manifest.findL1Policy(db.manifest.L0Files[0])
	if p.policy != OVERLAPPING {
		t.Fatalf("expected OVERLAPPING %d but got %d", NOTUNION, p.policy)
	}
	db.handleOverlapping(p, db.manifest.L0Files[0])
	if len(db.manifest.L0Files) != 0 {
		t.Fatalf("expected 0 level0 files but got %d", len(db.manifest.L0Files))
	}
	removeTestTable(6)
}
