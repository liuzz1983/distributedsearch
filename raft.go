package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

func testBoltStore() *raftboltdb.BoltStore {
	fh, err := ioutil.TempFile("", "bolt")
	if err != nil {
		fmt.Printf("err: %s", err)
	}
	os.Remove(fh.Name())

	// Successfully creates and returns a store
	store, err := raftboltdb.NewBoltStore(fh.Name())
	if err != nil {
		fmt.Printf("err: %s", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}
