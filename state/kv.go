package state

import (
	"time"

	"github.com/boltdb/bolt"
)

type KVConfig struct {
	DbFileName string `json:"dbFileName"`
	BucketName string `json:"bucketName"`
}

// KVStore provides a persistent KeyValue store
type KVStore struct {
	DbFileName string
	BucketName string
	db         *bolt.DB
}

// Init initialises the KeyValue store
func (k *KVStore) Init() error {
	var err error
	k.db, err = bolt.Open(k.DbFileName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}

	return k.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(k.BucketName))
		if err != nil {
			return err
		}
		return nil
	})
}

// Close closes the KeyValue Store ensuring it is persisted to disk
func (k *KVStore) Close() {
	k.db.Close()
}

// Set sets a Key to the defined value
func (k *KVStore) Set(key []byte, value []byte) error {
	return k.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(k.BucketName))
		return b.Put(key, value)
	})
}

// Get retrieves the specified value
func (k *KVStore) Get(key []byte) []byte {
	var value []byte
	k.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(k.BucketName))
		value = b.Get(key)
		return nil
	})

	return value
}

// ForEach executes the function for each key/value pair
func (k *KVStore) ForEach(function func(k, v []byte) error) error {
	return k.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(k.BucketName))

		return b.ForEach(function)
	})
}

// Delete deletes the given key
func (k *KVStore) Delete(key []byte) {
	k.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(k.BucketName))
		b.Delete(key)
		return nil
	})
}
