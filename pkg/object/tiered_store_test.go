//go:build !nosqlite
// +build !nosqlite

package object

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func newTestTieredObjectStore(t *testing.T, threshold int64) *tieredObjectStore {
	t.Helper()
	index := newTestTieredSQLStore(t, threshold, nil)
	large, err := newMem("tiered-combined-large", "", "", "")
	if err != nil {
		t.Fatalf("new large store: %v", err)
	}
	store, err := newTieredObjectStore(index, large)
	if err != nil {
		t.Fatalf("new tiered store: %v", err)
	}
	return store
}

func TestTieredStoreDefaultRuntimeConfigFailsClosed(t *testing.T) {
	store, err := CreateStorage("tiered", "", "", "", "")
	if store != nil {
		t.Fatalf("default tiered backend returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreExperimental) {
		t.Fatalf("default tiered backend error = %v, want %v", err, errTieredStoreExperimental)
	}
}

func TestTieredStoreRoutesSmallAndLargeByObjectSize(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	smallEntry, ok, err := store.index.head(ctx, "small")
	if err != nil || !ok || smallEntry.tier != tieredTierSmall {
		t.Fatalf("small entry = (%+v, %v, %v)", smallEntry, ok, err)
	}
	largeEntry, ok, err := store.index.head(ctx, "large")
	if err != nil || !ok || largeEntry.tier != tieredTierLarge {
		t.Fatalf("large entry = (%+v, %v, %v)", largeEntry, ok, err)
	}
	if got := readTieredStore(t, store, "small", 0, -1); got != "abc" {
		t.Fatalf("small got %q", got)
	}
	if got := readTieredStore(t, store, "large", 0, -1); got != "large-data" {
		t.Fatalf("large got %q", got)
	}
}

func TestTieredStoreOverwriteAcrossSmallAndLargePayloads(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "k", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	smallEntry, ok, err := store.index.head(ctx, "k")
	if err != nil || !ok || smallEntry.tier != tieredTierSmall {
		t.Fatalf("small entry = (%+v, %v, %v)", smallEntry, ok, err)
	}
	if err := store.Put(ctx, "k", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	largeEntry, ok, err := store.index.head(ctx, "k")
	if err != nil || !ok || largeEntry.tier != tieredTierLarge {
		t.Fatalf("large entry = (%+v, %v, %v)", largeEntry, ok, err)
	}
	if got := readTieredStore(t, store, "k", 0, -1); got != "large-data" {
		t.Fatalf("after small->large got %q", got)
	}
	if cleaned, err := store.drainSmallCleanup(ctx, 10); err != nil || cleaned != 1 {
		t.Fatalf("drain small cleanup = %d, %v", cleaned, err)
	}
	if exists, err := store.index.smallBlobExists(ctx, "k", smallEntry.generation); err != nil || exists {
		t.Fatalf("old small payload exists = %v, %v", exists, err)
	}
	if err := store.Put(ctx, "k", strings.NewReader("xy")); err != nil {
		t.Fatalf("put small again: %v", err)
	}
	newSmallEntry, ok, err := store.index.head(ctx, "k")
	if err != nil || !ok || newSmallEntry.tier != tieredTierSmall {
		t.Fatalf("new small entry = (%+v, %v, %v)", newSmallEntry, ok, err)
	}
	if got := readTieredStore(t, store, "k", 0, -1); got != "xy" {
		t.Fatalf("after large->small got %q", got)
	}
	if _, err := store.large.Head(ctx, string(largeEntry.payloadRef)); err != nil {
		t.Fatalf("old large payload should remain before cleanup: %v", err)
	}
	if cleaned, err := store.drainLargeCleanup(ctx, 10); err != nil || cleaned != 1 {
		t.Fatalf("drain large cleanup = %d, %v", cleaned, err)
	}
	if _, err := store.large.Head(ctx, string(largeEntry.payloadRef)); !os.IsNotExist(err) {
		t.Fatalf("old large payload after cleanup = %v", err)
	}
	if got := readTieredStore(t, store, "k", 0, -1); got != "xy" {
		t.Fatalf("active payload changed after cleanup: %q", got)
	}
}

func TestTieredStoreLargePutAcceptsStreamingReader(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	reader := &chunkedReader{chunks: []string{"large-", "stream-", "payload"}}
	if err := store.Put(context.Background(), "large", reader); err != nil {
		t.Fatalf("put large: %v", err)
	}
	if got := readTieredStore(t, store, "large", 0, -1); got != "large-stream-payload" {
		t.Fatalf("got %q", got)
	}
}

func TestTieredStoreLargeUploadBeforeIndexCommitLeavesOrphanInvisibleAndGCDeletes(t *testing.T) {
	crash := errors.New("crash before index")
	store := newTestTieredObjectStore(t, 4)
	store.fault = func(point tieredStoreFault) error {
		if point == tieredStoreFaultAfterLargePayloadPut {
			return crash
		}
		return nil
	}
	ctx := context.Background()
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); !errors.Is(err, crash) {
		t.Fatalf("put large with crash = %v, want %v", err, crash)
	}
	if _, ok, err := store.index.head(ctx, "large"); err != nil || ok {
		t.Fatalf("head after failed index commit = (%v, %v)", ok, err)
	}
	if _, err := store.Head(ctx, "large"); !os.IsNotExist(err) {
		t.Fatalf("public Head after failed index commit = %v, want not exist", err)
	}
	if reader, err := store.Get(ctx, "large", 0, -1); !os.IsNotExist(err) {
		if err == nil {
			_ = reader.Close()
		}
		t.Fatalf("public Get after failed index commit = %v, want not exist", err)
	}
	visible, hasMore, token, err := store.List(ctx, "large", "", "", "", 10, false)
	if err != nil {
		t.Fatalf("public List after failed index commit: %v", err)
	}
	if len(visible) != 0 || hasMore || token != "" {
		t.Fatalf("public List after failed index commit = objects %d hasMore %v token %q, want empty", len(visible), hasMore, token)
	}
	objects, _, _, err := store.large.List(ctx, store.payloadPrefix(), "", "", "", 10, false)
	if err != nil {
		t.Fatalf("list large payloads: %v", err)
	}
	if len(objects) != 1 {
		t.Fatalf("orphan payload count = %d, want 1", len(objects))
	}
	cleaned, err := store.drainLargeOrphans(ctx, 1)
	if err != nil {
		t.Fatalf("drain orphan: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned = %d, want 1", cleaned)
	}
	objects, _, _, err = store.large.List(ctx, store.payloadPrefix(), "", "", "", 10, false)
	if err != nil {
		t.Fatalf("list after cleanup: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("orphan payloads after cleanup = %d, want 0", len(objects))
	}
}

func TestTieredStoreLargeRangeRead(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	if got := readTieredStore(t, store, "large", 6, 4); got != "data" {
		t.Fatalf("range got %q", got)
	}
}

func TestTieredStoreLargeFullReadDetectsChecksumMismatch(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	entry, ok, err := store.index.head(ctx, "large")
	if err != nil || !ok {
		t.Fatalf("head large = (%+v, %v, %v)", entry, ok, err)
	}
	if err := store.large.Put(ctx, string(entry.payloadRef), strings.NewReader("corrupt!!!")); err != nil {
		t.Fatalf("corrupt large payload: %v", err)
	}
	reader, err := store.Get(ctx, "large", 0, -1)
	if err != nil {
		t.Fatalf("get corrupt large: %v", err)
	}
	_, err = io.ReadAll(reader)
	_ = reader.Close()
	if !errors.Is(err, errTieredSQLCorruption) {
		t.Fatalf("read corrupt large = %v, want corruption", err)
	}
}

func TestTieredStoreLargeHeadDetectsMissingPayload(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	entry, ok, err := store.index.head(ctx, "large")
	if err != nil || !ok {
		t.Fatalf("head large = (%+v, %v, %v)", entry, ok, err)
	}
	if err := store.large.Delete(ctx, string(entry.payloadRef)); err != nil && !os.IsNotExist(err) {
		t.Fatalf("delete payload: %v", err)
	}
	if _, err := store.Head(ctx, "large"); !errors.Is(err, errTieredSQLCorruption) {
		t.Fatalf("head missing payload = %v, want corruption", err)
	}
}

func TestTieredStoreListAndCopyUnsupported(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	for _, key := range []string{"p/b", "p/a", "p/dir/1"} {
		if err := store.Put(ctx, key, strings.NewReader("data")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	objects, hasMore, token, err := store.List(ctx, "p/", "", "", "/", 10, false)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if hasMore || token != "" {
		t.Fatalf("list page = hasMore %v token %q", hasMore, token)
	}
	if got := objectKeys(objects); strings.Join(got, "|") != "p/a|p/b|p/dir/" {
		t.Fatalf("list keys = %#v", got)
	}
	if err := store.Copy(ctx, "dst", "src"); !errors.Is(err, notSupported) {
		t.Fatalf("copy = %v, want %v", err, notSupported)
	}
}

func TestTieredStoreListAllFallsBackToIndexList(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	for _, key := range []string{"p/b", "p/a", "q/c"} {
		if err := store.Put(ctx, key, strings.NewReader("data")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	ch, err := ListAll(ctx, store, "p/", "", false, true)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	keys := make([]string, 0)
	for object := range ch {
		if object == nil {
			t.Fatal("list all returned nil object")
		}
		keys = append(keys, object.Key())
	}
	if strings.Join(keys, "|") != "p/a|p/b" {
		t.Fatalf("list all keys = %#v", keys)
	}
}

func TestTieredStoreLargeOrphanDrainMakesProgressAcrossReferencedFirstPage(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "a", strings.NewReader("large-active-one")); err != nil {
		t.Fatalf("put active one: %v", err)
	}
	if err := store.Put(ctx, "b", strings.NewReader("large-active-two")); err != nil {
		t.Fatalf("put active two: %v", err)
	}
	generation, err := store.index.reserveGeneration(ctx)
	if err != nil {
		t.Fatalf("reserve orphan generation: %v", err)
	}
	orphanRef := store.payloadRef(generation)
	if err := store.large.Put(ctx, orphanRef, strings.NewReader("large-orphan")); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	cleaned, err := store.drainLargeOrphans(ctx, 1)
	if err != nil {
		t.Fatalf("drain large orphans: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned orphans = %d, want 1", cleaned)
	}
	if _, err := store.large.Head(ctx, orphanRef); !os.IsNotExist(err) {
		t.Fatalf("orphan head after drain = %v", err)
	}
	for _, key := range []string{"a", "b"} {
		if got := readTieredStore(t, store, key, 0, -1); !strings.HasPrefix(got, "large-active-") {
			t.Fatalf("active %s changed after orphan drain: %q", key, got)
		}
	}
}

func TestTieredStoreLargeOrphanDrainHonorsCleanupBudget(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "a", strings.NewReader("large-active-one")); err != nil {
		t.Fatalf("put active one: %v", err)
	}
	if err := store.Put(ctx, "b", strings.NewReader("large-active-two")); err != nil {
		t.Fatalf("put active two: %v", err)
	}
	firstOrphanGeneration, err := store.index.reserveGeneration(ctx)
	if err != nil {
		t.Fatalf("reserve first orphan generation: %v", err)
	}
	firstOrphanRef := store.payloadRef(firstOrphanGeneration)
	if err := store.large.Put(ctx, firstOrphanRef, strings.NewReader("large-orphan-one")); err != nil {
		t.Fatalf("put first orphan: %v", err)
	}
	secondOrphanGeneration, err := store.index.reserveGeneration(ctx)
	if err != nil {
		t.Fatalf("reserve second orphan generation: %v", err)
	}
	secondOrphanRef := store.payloadRef(secondOrphanGeneration)
	if err := store.large.Put(ctx, secondOrphanRef, strings.NewReader("large-orphan-two")); err != nil {
		t.Fatalf("put second orphan: %v", err)
	}
	cleaned, err := store.drainLargeOrphans(ctx, 1)
	if err != nil {
		t.Fatalf("drain first orphan: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("first drain cleaned = %d, want 1", cleaned)
	}
	if _, err := store.large.Head(ctx, firstOrphanRef); !os.IsNotExist(err) {
		t.Fatalf("first orphan head after first drain = %v", err)
	}
	if _, err := store.large.Head(ctx, secondOrphanRef); err != nil {
		t.Fatalf("second orphan should remain after first drain: %v", err)
	}
	cleaned, err = store.drainLargeOrphans(ctx, 1)
	if err != nil {
		t.Fatalf("drain second orphan: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("second drain cleaned = %d, want 1", cleaned)
	}
	if _, err := store.large.Head(ctx, secondOrphanRef); !os.IsNotExist(err) {
		t.Fatalf("second orphan head after second drain = %v", err)
	}
}

func TestTieredStoreDeleteAndCleanupPreservesActiveLargeGeneration(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	entry, ok, err := store.index.head(ctx, "large")
	if err != nil || !ok {
		t.Fatalf("head large = (%+v, %v, %v)", entry, ok, err)
	}
	if err := store.index.insertCleanupForTest(ctx, "large", entry.generation, tieredTierLarge, entry.payloadRef); err != nil {
		t.Fatalf("insert active cleanup: %v", err)
	}
	cleaned, err := store.drainLargeCleanup(ctx, 10)
	if err != nil {
		t.Fatalf("drain large cleanup: %v", err)
	}
	if cleaned != 0 {
		t.Fatalf("cleaned active generation = %d, want 0", cleaned)
	}
	if got := readTieredStore(t, store, "large", 0, -1); got != "large-data" {
		t.Fatalf("got %q", got)
	}
	if err := store.Delete(ctx, "large"); err != nil {
		t.Fatalf("delete large: %v", err)
	}
	if _, err := store.Head(ctx, "large"); !os.IsNotExist(err) {
		t.Fatalf("head after delete = %v", err)
	}
	cleaned, err = store.drainLargeCleanup(ctx, 10)
	if err != nil {
		t.Fatalf("drain deleted large cleanup: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned deleted large = %d, want 1", cleaned)
	}
}

func readTieredStore(t *testing.T, store *tieredObjectStore, key string, off, limit int64) string {
	t.Helper()
	reader, err := store.Get(context.Background(), key, off, limit)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer reader.Close()
	var out bytes.Buffer
	if _, err := io.Copy(&out, reader); err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return out.String()
}

type chunkedReader struct {
	chunks []string
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	chunk := r.chunks[0]
	r.chunks = r.chunks[1:]
	return copy(p, chunk), nil
}
