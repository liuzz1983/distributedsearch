package main

import (
	"sync"

	"github.com/hashicorp/raft"
)

// Node represents a Raft server node.
type Node struct {
	mu       sync.RWMutex
	addr     string
	snapshot raft.SnapshotStore
	raft     *raft.Raft
	closed   bool
	store    Storage
	peers    map[string]string
}

func NewNode() *Node {
	return &Node{}
}
