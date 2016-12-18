package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"

	"github.com/tidwall/finn"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

// Clone represent a Redis clone machine
type Clone struct {
	mu   sync.RWMutex
	keys map[string][]byte
}

// NewClone create a new clone
func NewClone() *Clone {
	return &Clone{
		keys: make(map[string][]byte),
	}
}

// Command processes a command
func (kvm *Clone) Command(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		return nil, finn.ErrUnknownCommand
	case "set":
		if len(cmd.Args) != 3 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			func() (interface{}, error) {
				kvm.mu.Lock()
				kvm.keys[string(cmd.Args[1])] = cmd.Args[2]
				kvm.mu.Unlock()
				return nil, nil
			},
			func(v interface{}) (interface{}, error) {
				conn.WriteString("OK")
				return nil, nil
			},
		)
	case "get":
		if len(cmd.Args) != 2 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd, nil,
			func(interface{}) (interface{}, error) {
				kvm.mu.RLock()
				val, ok := kvm.keys[string(cmd.Args[1])]
				kvm.mu.RUnlock()
				if !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
				return nil, nil
			},
		)
	case "del":
		if len(cmd.Args) < 2 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			func() (interface{}, error) {
				var n int
				kvm.mu.Lock()
				for i := 1; i < len(cmd.Args); i++ {
					key := string(cmd.Args[i])
					if _, ok := kvm.keys[key]; ok {
						delete(kvm.keys, key)
						n++
					}
				}
				kvm.mu.Unlock()
				return n, nil
			},
			func(v interface{}) (interface{}, error) {
				n := v.(int)
				conn.WriteInt(n)
				return nil, nil
			},
		)
	case "keys":
		if len(cmd.Args) != 2 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		pattern := string(cmd.Args[1])
		return m.Apply(conn, cmd, nil,
			func(v interface{}) (interface{}, error) {
				var keys []string
				kvm.mu.RLock()
				for key := range kvm.keys {
					if match.Match(key, pattern) {
						keys = append(keys, key)
					}
				}
				kvm.mu.RUnlock()
				sort.Strings(keys)
				conn.WriteArray(len(keys))
				for _, key := range keys {
					conn.WriteBulkString(key)
				}
				return nil, nil
			},
		)
	}
}

// Restore restores a snapshot
func (kvm *Clone) Restore(rd io.Reader) error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	data, err := ioutil.ReadAll(rd)
	if err != nil {
		return err
	}
	var keys map[string][]byte
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	kvm.keys = keys
	return nil
}

// Snapshot creates a snapshot
func (kvm *Clone) Snapshot(wr io.Writer) error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	data, err := json.Marshal(kvm.keys)
	if err != nil {
		return err
	}
	if _, err := wr.Write(data); err != nil {
		return err
	}
	return nil
}
