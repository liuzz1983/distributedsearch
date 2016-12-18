package main

import (
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

type BoltStore struct {
	conn     *bolt.DB
	path     string
	logName  []byte
	confName []byte
}

// NewBoldStore create new bolt storage
func NewBoltStore(path string) (*BoltStore, error) {
	handle, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	store := &BoltStore{
		path:     path,
		conn:     handle,
		logName:  []byte("logs"),
		confName: []byte("conf"),
	}
	if err := store.initialize(); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

// initialize is used to set up all of the buckets.
func (b *BoltStore) initialize() error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create all the buckets
	if _, err := tx.CreateBucketIfNotExists(b.logName); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(b.confName); err != nil {
		return err
	}

	return tx.Commit()
}

func (b *BoltStore) Close() {
	b.conn.Close()
}

func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	curs := tx.Bucket(b.logName).Cursor()
	if first, _ := curs.First(); first != nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex returns the last known index from the Raft log.
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(b.logName).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

func (b *BoltStore) GetLog(idx uint64, log *raft.Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(b.logName)
	val := bucket.Get(uint64ToBytes(idx))
	if val == nil {
		return raft.ErrLogNotFound
	}
	return nil
}

func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	tx.Rollback()

	bucket := tx.Bucket(b.logName)
	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log.Data)
		if err != nil {
			return err
		}
		err = bucket.Put(key, val.Bytes())
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Set is used to set a key/value set outside of the raft log
func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(b.confName)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get is used to retrieve a value from the k/v store by key
func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(b.confName)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), val...), nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	curs := tx.Bucket(b.confName).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) > max {
			break
		}
		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}
