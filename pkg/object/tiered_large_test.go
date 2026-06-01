//go:build !nosqlite
// +build !nosqlite

package object

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func newTestTieredLargeStore(t *testing.T, threshold int64, fault func(tieredLargeFault) error) *tieredLargeStore {
	t.Helper()
	index := newTestTieredSQLStore(t, threshold, nil)
	large, err := newMem("tiered-large", "", "", "")
	if err != nil {
		t.Fatalf("new mem large store: %v", err)
	}
	store, err := newTieredLargeStore(index, large, fault)
	if err != nil {
		t.Fatalf("new tiered large store: %v", err)
	}
	return store
}

func TestTieredLargePutUsesImmutableGenerationAndIndexVisibility(t *testing.T) {
	store := newTestTieredLargeStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putLarge(ctx, "k", strings.NewReader("large-data"))
	if err != nil {
		t.Fatalf("put large: %v", err)
	}
	entry, ok, err := store.headLarge(ctx, "k")
	if err != nil || !ok {
		t.Fatalf("head large = (%+v, %v, %v)", entry, ok, err)
	}
	if entry.generation != generation || entry.tier != tieredTierLarge || !strings.HasPrefix(string(entry.payloadRef), store.payloadPrefix()) {
		t.Fatalf("entry = %+v prefix=%q", entry, store.payloadPrefix())
	}
	if exists, err := store.largePayloadExists(ctx, generation); err != nil || !exists {
		t.Fatalf("large payload exists = %v, %v", exists, err)
	}
	if got := readTieredLarge(t, store, "k"); got != "large-data" {
		t.Fatalf("got %q", got)
	}
}

func TestTieredLargeUploadBeforeIndexCommitLeavesOrphanInvisibleAndGCDeletes(t *testing.T) {
	crash := errors.New("crash before index")
	store := newTestTieredLargeStore(t, 4, func(point tieredLargeFault) error {
		if point == tieredLargeFaultAfterPayloadPut {
			return crash
		}
		return nil
	})
	ctx := context.Background()
	generation, err := store.putLarge(ctx, "k", strings.NewReader("large-data"))
	if !errors.Is(err, crash) {
		t.Fatalf("put large with crash = %v, want %v", err, crash)
	}
	if _, ok, err := store.index.head(ctx, "k"); err != nil || ok {
		t.Fatalf("head after failed index commit = (%v, %v)", ok, err)
	}
	if exists, err := store.largePayloadExists(ctx, generation); err != nil || !exists {
		t.Fatalf("orphan payload exists = %v, %v", exists, err)
	}
	cleaned, err := store.drainLargeOrphans(ctx, 100)
	if err != nil {
		t.Fatalf("drain large orphans: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned orphans = %d, want 1", cleaned)
	}
	if exists, err := store.largePayloadExists(ctx, generation); err != nil || exists {
		t.Fatalf("orphan payload after GC exists = %v, %v", exists, err)
	}
}

func TestTieredLargeOrphanDrainMakesProgressAcrossReferencedFirstPage(t *testing.T) {
	store := newTestTieredLargeStore(t, 4, nil)
	ctx := context.Background()
	activeOne, err := store.putLarge(ctx, "a", strings.NewReader("large-active-one"))
	if err != nil {
		t.Fatalf("put active one: %v", err)
	}
	activeTwo, err := store.putLarge(ctx, "b", strings.NewReader("large-active-two"))
	if err != nil {
		t.Fatalf("put active two: %v", err)
	}
	orphanKey := store.payloadRef(activeTwo + 1)
	if err := store.large.Put(ctx, orphanKey, strings.NewReader("large-orphan")); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	cleaned, err := store.drainLargeOrphans(ctx, 1)
	if err != nil {
		t.Fatalf("drain large orphans: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned orphans = %d, want 1", cleaned)
	}
	if _, err := store.large.Head(ctx, orphanKey); !os.IsNotExist(err) {
		t.Fatalf("orphan head after drain = %v", err)
	}
	for _, generation := range []uint64{activeOne, activeTwo} {
		if exists, err := store.largePayloadExists(ctx, generation); err != nil || !exists {
			t.Fatalf("active payload %d exists after orphan drain = %v, %v", generation, exists, err)
		}
	}
}

func TestTieredLargeHeadAndGetDetectCorruptPayload(t *testing.T) {
	store := newTestTieredLargeStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putLarge(ctx, "k", strings.NewReader("large-data"))
	if err != nil {
		t.Fatalf("put large: %v", err)
	}
	if err := store.overwriteLargePayloadForTest(ctx, generation, []byte("corrupt")); err != nil {
		t.Fatalf("overwrite payload: %v", err)
	}
	if _, _, err := store.headLarge(ctx, "k"); !errors.Is(err, errTieredSQLCorruption) {
		t.Fatalf("head corrupt payload = %v, want corruption", err)
	}
	if _, err := store.getLarge(ctx, "k", 0, -1); !errors.Is(err, errTieredSQLCorruption) {
		t.Fatalf("get corrupt payload = %v, want corruption", err)
	}
}

func TestTieredLargeDeleteIsIndexDrivenAndCleanupDeletesPayload(t *testing.T) {
	store := newTestTieredLargeStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putLarge(ctx, "k", strings.NewReader("large-data"))
	if err != nil {
		t.Fatalf("put large: %v", err)
	}
	if err := store.delete(ctx, "k"); err != nil {
		t.Fatalf("delete large: %v", err)
	}
	if _, ok, err := store.index.head(ctx, "k"); err != nil || ok {
		t.Fatalf("head after delete = (%v, %v)", ok, err)
	}
	if exists, err := store.largePayloadExists(ctx, generation); err != nil || !exists {
		t.Fatalf("payload should remain before GC = %v, %v", exists, err)
	}
	if count, err := store.index.cleanupQueueLen(ctx); err != nil || count != 1 {
		t.Fatalf("cleanup count = %d, %v", count, err)
	}
	cleaned, err := store.drainLargeCleanup(ctx, 100)
	if err != nil {
		t.Fatalf("drain large cleanup: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned large rows = %d, want 1", cleaned)
	}
	if exists, err := store.largePayloadExists(ctx, generation); err != nil || exists {
		t.Fatalf("payload after GC exists = %v, %v", exists, err)
	}
}

func TestTieredLargeCleanupNeverDeletesActiveGeneration(t *testing.T) {
	store := newTestTieredLargeStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putLarge(ctx, "k", strings.NewReader("large-data"))
	if err != nil {
		t.Fatalf("put large: %v", err)
	}
	payloadRef := []byte(store.payloadRef(generation))
	if err := store.index.insertCleanupForTest(ctx, "k", generation, tieredTierLarge, payloadRef); err != nil {
		t.Fatalf("insert active cleanup: %v", err)
	}
	cleaned, err := store.drainLargeCleanup(ctx, 100)
	if err != nil {
		t.Fatalf("drain large cleanup: %v", err)
	}
	if cleaned != 0 {
		t.Fatalf("cleaned active generation = %d, want 0", cleaned)
	}
	if exists, err := store.largePayloadExists(ctx, generation); err != nil || !exists {
		t.Fatalf("active payload exists = %v, %v", exists, err)
	}
	if got := readTieredLarge(t, store, "k"); got != "large-data" {
		t.Fatalf("got %q", got)
	}
}

func TestTieredLargeRangeReadStillValidatesFullPayload(t *testing.T) {
	store := newTestTieredLargeStore(t, 4, nil)
	ctx := context.Background()
	if _, err := store.putLarge(ctx, "k", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	reader, err := store.getLarge(ctx, "k", 6, 4)
	if err != nil {
		t.Fatalf("get range: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if string(data) != "data" {
		t.Fatalf("range = %q", data)
	}
}

func readTieredLarge(t *testing.T, store *tieredLargeStore, key string) string {
	t.Helper()
	reader, err := store.getLarge(context.Background(), key, 0, -1)
	if err != nil {
		t.Fatalf("get large %s: %v", key, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read large %s: %v", key, err)
	}
	return string(data)
}

func TestTieredLargeMissingPayloadIsCorruption(t *testing.T) {
	store := newTestTieredLargeStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putLarge(ctx, "k", strings.NewReader("large-data"))
	if err != nil {
		t.Fatalf("put large: %v", err)
	}
	if err := store.large.Delete(ctx, store.payloadRef(generation)); err != nil && !os.IsNotExist(err) {
		t.Fatalf("delete payload: %v", err)
	}
	if _, _, err := store.headLarge(ctx, "k"); !errors.Is(err, errTieredSQLCorruption) {
		t.Fatalf("head missing payload = %v, want corruption", err)
	}
}
