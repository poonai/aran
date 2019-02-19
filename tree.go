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
	idx        uint32
}

func (n *node) insert(out *node) {
	out.root = n
	if n.lowerRange > out.lowerRange {
		if n.left == nil {
			n.left = out
			return
		}
		n.left.insert(out)
		return
	}
	if n.right == nil {
		n.right = out
		return
	}
	n.right.insert(out)
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
	if n.idx == idx {
		if n.right != nil {
			n = n.right
			return
		}
		n = n.right
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
	n := &node{
		lowerRange: lowerRange,
		idx:        idx,
		left:       nil,
		right:      nil,
		root:       t.root,
	}
	if t.root == nil {
		t.root = n
		return
	}
	t.root.insert(n)
}

func (t *tree) deleteTable(idx uint32) {
	if t.root.idx == idx {
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
