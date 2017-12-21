package state

import (
	"os"
	"testing"
)

func TestKVStore(t *testing.T) {
	kv := KVStore{
		DbFileName: "test.db",
		BucketName: "Test",
	}
	kv.Init()
	defer kv.Close()

	err := kv.Set([]byte("foo"), []byte("bar"))
	if err != nil {
		t.Error("Could not set key/value")
	}

	value := kv.Get([]byte("foo"))
	if string(value) != "bar" {
		t.Errorf("Expected value at foo to be bar, go %v", value)
	}

	kv.Delete([]byte("foo"))
	value = kv.Get([]byte("foo"))
	if string(value) != "" {
		t.Errorf("Expected value at foo to be empty, got %v", value)
	}

	os.Remove("test.db")
}
