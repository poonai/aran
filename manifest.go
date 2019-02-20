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
	"encoding/gob"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"
)

type manifest struct {
	L1Files []tableManifest
	L0Files []tableManifest
	NextIDX uint32
	mutex   sync.RWMutex
}

func loadOrCreateManifest(abspath string) (*manifest, error) {
	manifestPath := path.Join(abspath, "manifest.data")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		fp, err := os.Create(manifestPath)
		if err != nil {
			return nil, err
		}
		fp.Close()
		return &manifest{
			L1Files: make([]tableManifest, 0),
			L0Files: make([]tableManifest, 0),
			NextIDX: 0,
		}, nil
	}
	fp, err := os.Open(manifestPath)
	if err != nil {
		return nil, err
	}
	m := &manifest{}
	decoder := gob.NewDecoder(fp)
	err = decoder.Decode(m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (m *manifest) nextFileID() uint32 {
	atomic.AddUint32(&m.NextIDX, 1)
	return m.NextIDX
}
func (m *manifest) save(absPath string) error {
	manifestPath := path.Join(absPath, "manifest.data")
	fp, err := os.OpenFile(manifestPath, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(fp)
	return encoder.Encode(m)
}

type tableManifest struct {
	MaxRange uint32
	MinRange uint32
	Idx      uint32
	Size     uint32
	Records  uint32
	Density  float32
}

type tableDesencing []tableManifest

func (t tableDesencing) Len() int {
	return len(t)
}

func (t tableDesencing) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
func (t tableDesencing) Less(i, j int) bool {
	return t[i].Density > t[i].Density
}
func (m *manifest) addl0file(records, minRange, maxRange uint32, size int, idx uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.L0Files = append(m.L0Files, tableManifest{
		Records:  records,
		MinRange: minRange,
		MaxRange: maxRange,
		Size:     uint32(size),
		Density:  float32(records) / float32(maxRange-minRange),
		Idx:      idx,
	})
}
func (m *manifest) addl1file(records, minRange, maxRange uint32, size int, idx uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.L1Files = append(m.L1Files, tableManifest{
		Records:  records,
		MinRange: minRange,
		MaxRange: maxRange,
		Size:     uint32(size),
		Density:  float32(records) / float32(maxRange-minRange),
		Idx:      idx,
	})
}

func (m *manifest) l0Len() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.L0Files)
}
func (m *manifest) l1Len() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.L1Files)
}

func (m *manifest) sortL0() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	sort.Sort(tableDesencing(m.L0Files))
}

func (m *manifest) deleteL0Table(idx uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i := 0; i < len(m.L0Files); i++ {
		if m.L0Files[i].Idx == idx {
			m.L0Files[i] = m.L0Files[len(m.L0Files)-1]
			m.L0Files = m.L0Files[:len(m.L0Files)-1]
			break
		}
	}
}

func (m *manifest) deleteL1Table(idx uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i := 0; i < len(m.L1Files); i++ {
		if m.L1Files[i].Idx == idx {
			m.L1Files[i] = m.L1Files[len(m.L1Files)-1]
			m.L1Files = m.L1Files[:len(m.L1Files)-1]
			break
		}
	}
}

func (m *manifest) copyL0() []tableManifest {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.L0Files
}

func (m *manifest) copyL1() []tableManifest {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.L1Files
}
