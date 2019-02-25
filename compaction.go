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
	"hash/crc32"

	"github.com/Sirupsen/logrus"
)

func (d *db) handleNotUnion(p compactionPolicy, l0f tableManifest) {
	// normal push down
	newt := newTable(d.absPath, l0f.Idx)
	d.l1handler.addTable(newt, l0f.Idx)
	d.l0handler.deleteTable(l0f.Idx)
	d.manifest.addl1file(uint32(newt.fileInfo.entries), newt.fileInfo.minRange, newt.fileInfo.maxRange, int(newt.size), l0f.Idx)
	d.manifest.deleteL0Table(l0f.Idx)
	logrus.Info("compaction: NOT UNION found so simply pushing the l0 file to l1")
}

func (d *db) handleUnion(p compactionPolicy, l0f tableManifest) {
	t1, t2 := newTable(d.absPath, l0f.Idx), newTable(d.absPath, p.tableIDS[0])
	d.mergeTable(t1, t2)
	logrus.Infof("compaction: UNION SET found so merged l0 %d with l1 %d, pushed to l1", t1.ID(), t2.ID())
	t1.close()
	d.l0handler.deleteTable(t1.ID())
	d.manifest.deleteL0Table(t1.ID())
	removeTable(d.absPath, t1.ID())
	logrus.Infof("compaction: l0 file has been deleted %d", t1.ID())
	t2.close()
	d.l1handler.deleteTable(t2.ID())
	d.manifest.deleteL1Table(t2.ID())
	removeTable(d.absPath, t2.ID())
	logrus.Infof("compaction: l1 file has been deleted %d", t2.ID())
}

func (d *db) handleOverlapping(p compactionPolicy, l0f tableManifest) {
	logrus.Infof("compaction: OVERLAPPING found")
	builders := []*mergeTableBuilder{}
	// if the the value is not in the range, we'll create a new file and append everything
	// it it
	var extraBuilder *mergeTableBuilder
	// some crazy for loop has been written so try to refactor
	for _, idx := range p.tableIDS {
		t := newTable(d.absPath, idx)
		t.SeekBegin()
		builder := newTableMergeBuilder(int(t.size))
		builder.append(t.fp, int64(t.fileInfo.metaOffset))
		builder.mergeHashMap(t.offsetMap, 0)
		builders = append(builders, builder)
	}
	toCompacT := newTable(d.absPath, l0f.Idx)
	iter := toCompacT.iter()
	for iter.has() {
		kl, vl, key, val := iter.next()
		c := crc32.New(CastagnoliCrcTable)
		c.Write(key)
		hash := c.Sum32()
		for _, builder := range builders {
			if hash >= builder.Min() && hash <= builder.Max() {
				c := crc32.New(CastagnoliCrcTable)
				c.Write(key)
				hash := c.Sum32()
				builder.add(kl, vl, key, val, hash)
				continue
			}
			if extraBuilder == nil {
				extraBuilder = newTableMergeBuilder(10000000)
			}
			c := crc32.New(CastagnoliCrcTable)
			c.Write(key)
			hash := c.Sum32()
			extraBuilder.add(kl, vl, key, val, hash)
		}
	}
	for _, builder := range builders {
		d.saveL1Table(builder.finish())
	}
	if extraBuilder != nil {
		d.saveL1Table(extraBuilder.finish())
	}
	for _, idx := range p.tableIDS {
		d.l1handler.deleteTable(idx)
		removeTable(d.absPath, idx)
		d.manifest.deleteL1Table(idx)
	}
	d.l0handler.deleteTable(l0f.Idx)
	removeTable(d.absPath, l0f.Idx)
	d.manifest.deleteL0Table(l0f.Idx)
}
