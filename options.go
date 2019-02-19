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

type Options struct {
	NoOfL0Files  int
	memtablesize int
	path         string
	maxL1Size    int
}

func DefaultOptions() Options {
	return Options{
		3,
		64 << 20, // default value is robbed from badger. badger is a good inspiration to write key value storage in golang
		"./",
		64 << 21,
	}
}
