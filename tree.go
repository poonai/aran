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

// simple binary serach tree to find the maximum lower range of incoming key

type node struct {
	root       *node
	left       *node
	right      *node
	lowerRange uint32
	idx        []uint32
}

func (n *node) insert(lowerRange, idx uint32) {

	if n.lowerRange == lowerRange {
		n.idx = append(n.idx, idx)
		return
	}
	if n.lowerRange > lowerRange {
		if n.left == nil {
			n.left = &node{
				left:       nil,
				right:      nil,
				root:       n,
				idx:        []uint32{idx},
				lowerRange: lowerRange,
			}
			return
		}
		n.left.insert(lowerRange, idx)
		return
	}
	if n.right == nil {
		n.right = &node{
			left:       nil,
			right:      nil,
			root:       n,
			idx:        []uint32{idx},
			lowerRange: lowerRange,
		}
		return
	}
	n.right.insert(lowerRange, idx)
}

func (n *node) rootNode() *node {
	return n.root
}
func (n *node) findLargestLowerRange(r uint32) *node {
	if n.lowerRange < r {
		if n.right != nil {
			return n.right.findLargestLowerRange(r)
		}
	}
	if n.lowerRange > r {
		if n.left != nil {
			return n.left.findLargestLowerRange(r)
		}
	}
	if n.lowerRange > r {
		return nil
	}
	return n
}

type tree struct {
	root *node
}

func (n *node) deleteTable(idx uint32) {
	i, ok := in_array(idx, n.idx)
	if ok {
		n.idx[i] = n.idx[len(n.idx)-1]
		n.idx = n.idx[:len(n.idx)-1]
		if len(n.idx) != 0 {
			return
		}
		if n.right != nil {
			n = n.right
			return
		}
		n = n.left
		return
	}

	if n.right != nil {
		n.right.deleteTable(idx)
	}
	if n.left != nil {
		n.left.deleteTable(idx)
	}
}

func newTree() *tree {
	return &tree{}
}

func (t *tree) insert(lowerRange, idx uint32) {
	if t.root == nil {
		t.root = &node{
			lowerRange: lowerRange,
			idx:        []uint32{idx},
			left:       nil,
			right:      nil,
			root:       t.root,
		}
		return
	}
	t.root.insert(lowerRange, idx)
}

func (t *tree) deleteTable(idx uint32) {
	i, ok := in_array(idx, t.root.idx)
	if ok {
		t.root.idx[i] = t.root.idx[len(t.root.idx)-1]
		t.root.idx = t.root.idx[:len(t.root.idx)-1]
		if len(t.root.idx) != 0 {
			return
		}
		if t.root.right != nil {
			t.root = t.root.right
			return
		}
		t.root = t.root.left
		return
	}
	if t.root.right != nil {
		t.root.right.deleteTable(idx)
	}
	if t.root.left != nil {
		t.root.left.deleteTable(idx)
	}
}

func (t *tree) findLargestLowerRange(r uint32) *node {
	if t.root == nil {
		return nil
	}
	if t.root.lowerRange < r {
		if t.root.right != nil {
			n := t.root.right.findLargestLowerRange(r)
			if n != nil {
				return n
			}
		}
	}
	if t.root.lowerRange > r {
		if t.root.left != nil {
			return t.root.left.findLargestLowerRange(r)
		}
	}
	if t.root.lowerRange > r {
		return nil
	}
	return t.root
}

func (t *tree) findAllLargestRange(r uint32) []*node {
	//TODO: it's a naive implementation.
	//It has to be changed to some stack based finding in the one iteration itself
	//instead of looping several time.
	//anyway I don't think so that It'll bring much performance that's why I kept it simple(Big lie I'm too lazy to do it)
	//It is good have that stack based finding

	nodes := []*node{}
	for {
		n := t.findLargestLowerRange(r)
		if n == nil {
			break
		}
		nodes = append(nodes, n)
		r = n.lowerRange - 1
	}

	return nodes
}
