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
	"path/filepath"
	"sync"
	"time"

	"github.com/reusee/mmh3"

	"github.com/Sirupsen/logrus"
	"github.com/dgraph-io/badger/y"
)

type db struct {
	opts                Options
	writeChan           chan *request
	l0handler           *levelHandler
	l1handler           *levelHandler
	absPath             string
	manifest            *manifest
	mtable              *hashMap
	immtable            *hashMap
	flushDisk           chan *hashMap
	writeCloser         *y.Closer
	loadBalancingCloser *y.Closer
	compactionCloser    *y.Closer
	flushDiskCloser     *y.Closer
	sync.RWMutex
}

type request struct {
	key   []byte
	value []byte
	wg    sync.WaitGroup
}

func New(opts Options) (*db, error) {
	absPath, err := filepath.Abs(opts.path)
	if err != nil {
		return nil, err
	}
	manifest, err := loadOrCreateManifest(absPath)
	if err != nil {
		return nil, err
	}
	l0handler := newLevelHanlder()
	for _, l0file := range manifest.L0Files {
		t := newTable(absPath, l0file.Idx)
		l0handler.addTable(t, l0file.Idx)
	}
	l1handler := newLevelHanlder()
	for _, l1file := range manifest.L1Files {
		t := newTable(absPath, l1file.Idx)
		l1handler.addTable(t, l1file.Idx)
	}
	db := &db{
		opts:                opts,
		writeChan:           make(chan *request, 1000),
		absPath:             absPath,
		manifest:            manifest,
		mtable:              newHashMap(opts.memtablesize),
		l0handler:           l0handler,
		l1handler:           l1handler,
		writeCloser:         y.NewCloser(1),
		loadBalancingCloser: y.NewCloser(1),
		compactionCloser:    y.NewCloser(1),
		flushDiskCloser:     y.NewCloser(1),
		flushDisk:           make(chan *hashMap, 1),
	}
	go db.runCompaction(db.compactionCloser)
	go db.listenForFlushing(db.flushDiskCloser)
	go db.loadBalancing(db.loadBalancingCloser)
	go db.accpetWrite(db.writeCloser)
	return db, nil
}

func (d *db) Close() {

	if d.mtable.Len() > 0 {
		d.flushDisk <- d.mtable
	}

	d.writeCloser.SignalAndWait()
	d.loadBalancingCloser.SignalAndWait()
	d.compactionCloser.SignalAndWait()
	d.flushDiskCloser.SignalAndWait()
	err := d.manifest.save(d.absPath)
	if err != nil {
		logrus.Fatalf("manifest: unable to save the manifest %s", err.Error())
	}
}

func (d *db) Set(key, val []byte) {
	r := request{
		key:   key,
		value: val,
	}
	r.wg.Add(1)
	d.writeChan <- &r
	r.wg.Wait()
}
func (d *db) accpetWrite(closer *y.Closer) {
	reqs := []*request{}
loop:
	for {
		select {
		case req := <-d.writeChan:
			if len(reqs) > 10 {
				// do write
				d.write(reqs)
				reqs = []*request{}
			}
			reqs = append(reqs, req)

		case <-closer.HasBeenClosed():
			break loop
		default:
			// do write

			d.write(reqs)
			reqs = []*request{}
		}
	}
	if len(reqs) == 0 {
		reqs = []*request{}
	}
	close(d.writeChan)
	for req := range d.writeChan {
		reqs = append(reqs, req)
	}
	d.write(reqs)
	closer.Done()
}

func (d *db) write(reqs []*request) {
	for _, req := range reqs {
		if !d.mtable.isEnoughSpace(len(req.key) + len(req.value)) {
			d.Lock()
			d.immtable = d.mtable
			d.mtable = newHashMap(d.opts.memtablesize)
			d.Unlock()
			d.flushDisk <- d.immtable
		}
		d.mtable.Set(req.key, req.value)
		req.wg.Done()
	}
}

func (d *db) listenForFlushing(closer *y.Closer) {
	// original paper don't have this immutable table. btw I'm borrowing
	// it from wisckey's and badger implementation for async flushing to disk
	// instead of stalling at write.
loop:
	for {
		select {
		case <-closer.HasBeenClosed():
			break loop
		case imtable := <-d.flushDisk:
			d.flushMem(imtable)
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
	close(d.flushDisk)
	for imtable := range d.flushDisk {
		d.flushMem(imtable)
	}
	closer.Done()
}

func (d *db) flushMem(imtable *hashMap) {
	nxtID := d.manifest.nextFileID()
	imtable.toDisk(d.absPath, nxtID)
	d.Lock()
	d.manifest.addl0file(imtable.records, imtable.minRange, imtable.maxRange, imtable.occupiedSpace(), nxtID)
	table := newTable(d.absPath, nxtID)
	d.l0handler.addTable(table, nxtID)
	d.immtable = nil
	d.Unlock()
}

func (d *db) mergeTable(t1, t2 *table) {
	t1.SeekBegin()
	t2.SeekBegin()
	builder := newTableMergeBuilder(int(t1.size + t2.size))
	builder.append(t1.fp, int64(t1.fileInfo.metaOffset))
	builder.append(t2.fp, int64(t2.fileInfo.metaOffset))
	builder.mergeHashMap(t1.offsetMap, 0)
	builder.mergeHashMap(t2.offsetMap, uint32(t1.fileInfo.metaOffset))
	buf := builder.finish()
	d.saveL1Table(buf)
}

func (d *db) saveL1Table(buf []byte) {
	FID := d.manifest.nextFileID()
	fp, err := os.Create(giveTablePath(d.absPath, FID))
	if err != nil {
		logrus.Fatalf("compaction: unable to create new while pushing to level 1 %s", err.Error())
	}
	n, err := fp.Write(buf)
	if err != nil {
		logrus.Fatalf("compaction: unable to write to new level 1 table %s", err.Error())
	}
	if n != len(buf) {
		logrus.Fatalf("compaction: unable to write a new file at level 1 table expected %d but got %d", len(buf), n)
	}
	//l1 table has been created so have to remove those files from l0
	// and add it to l1
	newt := newTable(d.absPath, FID)
	d.l1handler.addTable(newt, FID)

	d.manifest.addl1file(uint32(newt.fileInfo.entries), newt.fileInfo.minRange, newt.fileInfo.maxRange, int(newt.size), FID)
	logrus.Infof("comapction: new l1 file has beed added %d", FID)
}

func (d *db) runCompaction(closer *y.Closer) {

loop:
	for {
		select {
		case <-closer.HasBeenClosed():
			fmt.Printf("breaking compaction")
			break loop
		default:
			// check for l0Tables
			fmt.Println(d.manifest.l0Len())
			if d.manifest.l0Len() >= d.opts.NoOfL0Files {
				if d.manifest.l1Len() == 0 {
					// sorting according to the denisty
					d.manifest.sortL1()
					// create two victim table
					t1, t2 := newTable(d.absPath, d.manifest.L0Files[0].Idx), newTable(d.absPath, d.manifest.L0Files[1].Idx)
					d.mergeTable(t1, t2)
					fmt.Println("deleting")
					//d.l0handler.deleteTable(t1.ID())
					t1.close()
					removeTable(d.absPath, t1.ID())
					d.manifest.deleteL0Table(t1.ID())
					logrus.Infof("comapction: l0 file has beed deleted %d", t1.ID())
					//d.l0handler.deleteTable(t2.ID())
					t2.close()
					removeTable(d.absPath, t2.ID())
					d.manifest.deleteL0Table(t2.ID())
					logrus.Infof("comapction: l0 file has beed deleted %d", t2.ID())
				}

				// level one files already exist so find union set to push
				// if overlapping range then append accordingly other wise just push down
				l0fs := d.manifest.copyL0()
				fmt.Println(l0fs)
				for _, l0f := range l0fs {
					p := d.manifest.findL1Policy(l0f)
					if p.policy == NOTUNION {
						// normal push down
						newt := newTable(d.absPath, l0f.Idx)
						d.l1handler.addTable(newt, l0f.Idx)
						d.l0handler.deleteTable(l0f.Idx)
						d.manifest.addl1file(uint32(newt.fileInfo.entries), newt.fileInfo.minRange, newt.fileInfo.maxRange, int(newt.size), l0f.Idx)
						d.manifest.deleteL0Table(l0f.Idx)
						logrus.Info("compaction: NOT UNION found so simply pushing the l0 file to l1")
						continue
					}

					if p.policy == UNION {
						t1, t2 := newTable(d.absPath, l0f.Idx), newTable(d.absPath, p.tableIDS[0])
						d.mergeTable(t1, t2)
						logrus.Infof("compaction: UNION SET found so merged l0 %d with l1 %d, pushed to l1", t1.ID(), t2.ID())
						t1.close()
						d.l0handler.deleteTable(t1.ID())
						d.manifest.deleteL0Table(t1.ID())
						logrus.Infof("compaction: l0 file has been deleted %d", t1.ID())
						t2.close()
						d.l1handler.deleteTable(t2.ID())
						d.manifest.deleteL1Table(t2.ID())
						continue
					}

					if p.policy == OVERLAPPING {
						builders := []*mergeTableBuilder{}
						// if the the value is not in the range, we'll create a new file and append everything
						// it it
						var extraBuilder *mergeTableBuilder
						// some crazy for has been written so try to refactor
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
							hash := mmh3.Hash32(key)
							for _, builder := range builders {
								if hash >= builder.Min() && hash <= builder.Max() {
									hash := mmh3.Hash32(key)
									builder.add(kl, vl, key, val, hash)
									continue
								}
								if extraBuilder == nil {
									extraBuilder = newTableMergeBuilder(10000000)
								}
								hash := mmh3.Hash32(key)
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
							d.l0handler.deleteTable(idx)
							removeTable(d.absPath, idx)
							d.manifest.deleteL0Table(idx)
						}
					}
				}
			}
		}
	}
	closer.Done()
}

func (d *db) loadBalancing(closer *y.Closer) {
loop:
	for {
		select {
		case <-closer.HasBeenClosed():
			break loop

		default:
			for _, l1f := range d.manifest.copyL1() {
				if l1f.Size > uint32(d.opts.maxL1Size) {
					logrus.Infof("load balancing: l1 file %d found which it larger than max l1 size", l1f.Idx)
					l1t := newTable(d.absPath, l1f.Idx)
					ents := l1t.entries()
					k := len(ents) / 2
					median := ents[k]
					builders := []*mergeTableBuilder{newTableMergeBuilder(int(l1f.Size) / 2), newTableMergeBuilder(int(l1f.Size) / 2)}
					iter := l1t.iter()
					for iter.has() {
						kl, vl, key, val := iter.next()
						hash := mmh3.Hash32(key)
						if hash < median {
							builders[0].add(kl, vl, key, val, hash)
							continue
						}
						builders[1].add(kl, vl, key, val, hash)
						continue
					}
					d.saveL1Table(builders[0].finish())
					d.saveL1Table(builders[1].finish())
					d.l1handler.deleteTable(l1f.Idx)
					d.manifest.deleteL1Table(l1f.Idx)
					logrus.Infof("load balancing: l1 file %d is splitted into two l1 files properly")
				}
			}

		}
	}
	closer.Done()
}

func (d *db) Get(key []byte) ([]byte, bool) {
	val, exist := d.mtable.Get(key)
	if exist {
		return val, exist
	}
	if d.immtable != nil {
		val, exist := d.immtable.Get(key)
		if exist {
			return val, exist
		}
	}

	val, exist = d.l0handler.get(key)
	if exist {
		return val, exist
	}
	return d.l1handler.get(key)
}
