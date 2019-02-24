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
	"sync"
)

type levelHandler struct {
	tables  []*table
	indexer *tree
	sync.RWMutex
}

func newLevelHanlder() *levelHandler {
	return &levelHandler{
		tables:  make([]*table, 0),
		indexer: newTree(),
	}
}

func (l *levelHandler) addTable(t *table, idx uint32) {
	l.Lock()
	defer l.Unlock()
	l.tables = append(l.tables, t)
	l.indexer.insert(t.fileInfo.minRange, idx)
}

func (l *levelHandler) deleteTable(idx uint32) {
	l.Lock()
	defer l.Unlock()
	l.indexer.deleteTable(idx)
	for i, table := range l.tables {
		if table.ID() == idx {
			l.tables[i] = l.tables[len(l.tables)-1]
			l.tables[len(l.tables)-1] = nil
			l.tables = l.tables[:len(l.tables)-1]
			break
		}
	}
}

func (l *levelHandler) get(key []byte) ([]byte, bool) {
	l.RLock()
	defer l.RUnlock()
	c := crc32.New(CastagnoliCrcTable)
	c.Write(key)
	hash := c.Sum32()
	nodes := l.indexer.findAllLargestRange(hash)

	for _, node := range nodes {
		for _, id := range node.idx {
			t := l.getTable(id)
			if t != nil {
				val, exist := t.Get(key)
				if exist {
					return val, true
				}
			}
		}

	}
	return nil, false
}

func (l *levelHandler) getTable(idx uint32) *table {
	for _, t := range l.tables {
		if t.ID() == idx {
			return t
		}
	}
	return nil
}
