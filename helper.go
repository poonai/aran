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
	"os"

	"github.com/Sirupsen/logrus"
)

func giveTablePath(abs string, idx uint32) string {
	return fmt.Sprintf("%s/%d.table", abs, idx)
}

func minRange(a, b uint32) uint32 {
	if a > b {
		return b
	} else {
		return a
	}
}
func maxRange(a, b uint32) uint32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func removeTable(abs string, idx uint32) {
	tp := giveTablePath(abs, idx)
	err := os.Remove(tp)
	if err != nil {
		logrus.Errorf("unable to delete the %d table", idx)
	}
	logrus.Infof("compaction: remove %d table", idx)
}

// https://codereview.stackexchange.com/questions/60074/in-array-in-go
func in_array(val uint32, array []uint32) (index int, exists bool) {
	exists = false
	index = -1

	for i, v := range array {
		if val == v {
			index = i
			exists = true
			return
		}
	}

	return
}
