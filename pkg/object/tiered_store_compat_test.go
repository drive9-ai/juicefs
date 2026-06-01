//go:build !nosqlite
// +build !nosqlite

package object

import (
	"testing"
)

func TestTieredStoreObjectStorageCompatibility(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	testStorage(t, store)
}
