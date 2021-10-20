package engine

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	lds "github.com/ipfs/go-ds-leveldb"
)

func TestDsCache(t *testing.T) {
	tmpDir := t.TempDir()
	ldstore, err := lds.NewDatastore(tmpDir, nil)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cache, err := newDsCache(ctx, ldstore, 5)
	if err != nil {
		t.Fatal(err)
	}

	key := ds.NewKey("hw")
	val := []byte("hello world")

	// Test Put and Get.
	err = cache.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	val2, err := cache.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val2, val) {
		t.Fatal("wrong value returned")
	}

	// Test cache eviction.
	for i := 0; i < cache.Cap(); i++ {
		k := ds.NewKey(fmt.Sprintf("key-%d", i))
		err = cache.Put(k, []byte(fmt.Sprintf("val-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if cache.Len() != cache.Cap() {
		t.Fatalf("expected len to be %d, got %d", cache.Cap(), cache.Len())
	}
	_, err = cache.Get(key)
	if err != ds.ErrNotFound {
		t.Fatalf("Expected error %s, got %s", ds.ErrNotFound, err)
	}
	_, err = cache.dstore.Get(key)
	if err != ds.ErrNotFound {
		t.Fatalf("value for %q was not removed from datastore", key)
	}

	// Test loading cache from datastore.
	cache, err = newDsCache(ctx, ldstore, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Check that cache was resized.
	if cache.Cap() != 5 {
		t.Fatal("cache did not resize to 5")
	}
	if cache.Len() != cache.Cap() {
		t.Fatalf("expected %d items in cache, got %d", cache.Cap(), cache.Len())
	}

	// Test cache Clear.
	cache.Clear()
	if cache.Len() != 0 {
		t.Fatal("cache was not purged")
	}
	// Check that no keys are loaded from datastore.
	cache, err = newDsCache(ctx, ldstore, 3)
	if err != nil {
		t.Fatal(err)
	}
	if cache.Len() != 0 {
		t.Fatal("cache was not purged")
	}
}
