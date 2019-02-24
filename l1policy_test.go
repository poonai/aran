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

import "testing"

func TestPolicy(t *testing.T) {
	m := &manifest{
		L1Files: []tableManifest{
			tableManifest{MaxRange: 100, MinRange: 100},
		},
	}
	p := m.findL1Policy(tableManifest{MaxRange: 100, MinRange: 100})
	if p.policy != UNION {
		t.Fatalf("exptected UNION %d but got %d", UNION, p.policy)
	}
	p = m.findL1Policy(tableManifest{MaxRange: 400, MinRange: 300})
	if p.policy != NOTUNION {
		t.Fatalf("exptected NOTUNION %d but got %d", NOTUNION, p.policy)
	}
	m.L1Files = append(m.L1Files, tableManifest{
		MaxRange: 300,
		MinRange: 200,
	})
	p = m.findL1Policy(tableManifest{MaxRange: 450, MinRange: 250})
	if p.policy != OVERLAPPING {
		t.Fatalf("exptected OVERLAPPING %d but got %d", OVERLAPPING, p.policy)
	}
	p = m.findL1Policy(tableManifest{MaxRange: 250, MinRange: 150})
	if p.policy != OVERLAPPING {
		t.Fatalf("exptected OVERLAPPING %d but got %d", OVERLAPPING, p.policy)
	}
}
