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

type l1policyPush int

const (
	UNION l1policyPush = iota
	OVERLAPPING
	NOTUNION
)

type compactionPolicy struct {
	policy   l1policyPush
	tableIDS []uint32
}

func (m *manifest) findL1Policy(tm tableManifest) compactionPolicy {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	cp := compactionPolicy{
		tableIDS: make([]uint32, 0),
		policy:   NOTUNION,
	}
	for _, l1m := range m.L1Files {
		// we'll merge if both are union to vice versa
		if (l1m.MinRange <= tm.MinRange && l1m.MaxRange >= tm.MaxRange) || (l1m.MinRange >= tm.MinRange && l1m.MaxRange <= tm.MaxRange) {
			cp.policy = UNION
			cp.tableIDS = append(cp.tableIDS, l1m.Idx)
			return cp
		}
		if (l1m.MinRange <= tm.MinRange && l1m.MaxRange > tm.MinRange) || (l1m.MinRange < tm.MaxRange && l1m.MaxRange >= tm.MaxRange) {
			cp.policy = OVERLAPPING
			cp.tableIDS = append(cp.tableIDS, l1m.Idx)
		}
	}
	return cp
}
