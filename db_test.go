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
	"bytes"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/y"
)

func TestDB(t *testing.T) {
	opts := DefaultOptions()
	//opts.path = "/tmp"
	d, err := New(opts)
	if err != nil {
		t.Fatalf("db is expected to open but got error %s", err.Error())
	}
	d.Set([]byte("hello"), []byte("schoolboy"))
	d.Close()
	d, err = New(opts)
	if err != nil {
		t.Fatalf("db is expected to open but got error %s", err.Error())
	}
	val, exist := d.Get([]byte("hello"))
	if !exist {
		t.Fatalf("unable to retrive data")
	}
	if bytes.Compare(val, []byte("schoolboy")) != 0 {
		t.Fatalf("value is not same expected schoolboy but got %s", string(val))
	}
	d.Close()
}

func TestCloser(t *testing.T) {
	closer := y.NewCloser(1)
	go func() {
	loop:
		for {
			select {
			case <-closer.HasBeenClosed():

				break loop
			}
		}
		closer.Done()
	}()
	closer.SignalAndWait()
}

func TestConcurrent(t *testing.T) {
	opts := DefaultOptions()
	d, err := New(opts)
	if err != nil {
		t.Fatalf("db is expected to open but got error %s", err.Error())
	}
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte("vanakam" + string(i))
			value := []byte("nanbare" + string(i))
			d.Set(key, value)
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("vanakam" + string(i))
			value := []byte("nanbare" + string(i))
			d.Set(key, value)
		}
		wg.Done()
	}()
	wg.Wait()
	d.Close()
	wg.Add(1)
	d, err = New(opts)
	if err != nil {
		t.Fatalf("db is expected to open but got error %s", err.Error())
	}
	go func() {
		for i := 108; i < 234; i++ {
			key := []byte("vanakam" + string(i))
			value := []byte("nanbare" + string(i))
			d.Set(key, value)
		}
		wg.Done()
	}()
	wg.Wait()
	d.Close()
	d, err = New(opts)
	wg.Add(1)
	wg.Add(1)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte("vanakam" + string(i))
			value := []byte("nanbare" + string(i))
			inv, exist := d.Get(key)
			if !exist {
				t.Fatalf("value not found for %s", string(key))
			}
			if bytes.Compare(value, inv) != 0 {
				t.Fatalf("expected value %s but got %s", string(value), string(inv))
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("vanakam" + string(i))
			value := []byte("nanbare" + string(i))
			inv, exist := d.Get(key)
			if !exist {
				t.Fatalf("value not found for %s", string(key))
			}
			if bytes.Compare(value, inv) != 0 {
				t.Fatalf("expected value %s but got %s", string(value), string(inv))
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("vanakam" + string(i))
			value := []byte("nanbare" + string(i))
			inv, exist := d.Get(key)
			if !exist {
				t.Fatalf("value not found for %s", string(key))
			}
			if bytes.Compare(value, inv) != 0 {
				t.Fatalf("expected value %s but got %s", string(value), string(inv))
			}
		}
		wg.Done()
	}()
	wg.Wait()
	d.Close()
}

func TestCompaction(t *testing.T) {

}
