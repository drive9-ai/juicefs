//go:build !nosqlite
// +build !nosqlite

package object

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func newTestTieredSQLStore(t *testing.T, threshold int64, fault func(tieredSQLFault) error) *tieredSQLStore {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "tiered.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := newTieredSQLStore(context.Background(), db, tieredSQLConfig{
		Dialect:        tieredDialectSQLite,
		VolumeID:       []byte("vol-1"),
		SmallThreshold: threshold,
		Fault:          fault,
	})
	if err != nil {
		t.Fatalf("new tiered sql store: %v", err)
	}
	return store
}

func TestTieredSQLSchemaUsesBinaryKeys(t *testing.T) {
	schema := strings.Join(tieredSQLSchema(tieredDialectMySQL), "\n")
	for _, table := range []string{"tiered_object_index", "tiered_object_blob", "tiered_object_gc_queue", "tiered_object_meta"} {
		if !strings.Contains(schema, table) {
			t.Fatalf("schema missing %s", table)
		}
	}
	if !strings.Contains(schema, "object_key VARBINARY(1024) NOT NULL") {
		t.Fatalf("object_key must be binary in MySQL/TiDB schema:\n%s", schema)
	}
	if strings.Contains(schema, "jfs_") {
		t.Fatalf("schema must not use jfs-prefixed tables:\n%s", schema)
	}
}

func TestTieredSQLVolumeConfigFailClosed(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	_, err := newTieredSQLStore(context.Background(), store.db, tieredSQLConfig{
		Dialect:        tieredDialectSQLite,
		VolumeID:       []byte("vol-1"),
		SmallThreshold: 8,
	})
	if !errors.Is(err, errTieredSQLConfigMismatch) {
		t.Fatalf("new with mismatched threshold = %v, want %v", err, errTieredSQLConfigMismatch)
	}
}

func TestTieredSQLPutSmallUsesIndexVisibility(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putSmall(ctx, "k", []byte("abc"))
	if err != nil {
		t.Fatalf("put small: %v", err)
	}
	if generation != 1 {
		t.Fatalf("generation = %d, want 1", generation)
	}
	entry, ok, err := store.head(ctx, "k")
	if err != nil || !ok {
		t.Fatalf("head = (%+v, %v, %v)", entry, ok, err)
	}
	if entry.tier != tieredTierSmall || entry.size != 3 {
		t.Fatalf("entry = %+v", entry)
	}
	if got := readTieredSQL(t, store, "k"); got != "abc" {
		t.Fatalf("got %q", got)
	}
}

func TestTieredSQLRejectsOversizedSmallPut(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	if _, err := store.putSmall(context.Background(), "k", []byte("abcde")); !errors.Is(err, errTieredSQLUnsupported) {
		t.Fatalf("put oversized small = %v, want %v", err, errTieredSQLUnsupported)
	}
}

func TestTieredSQLSmallOverwriteEnqueuesOldGeneration(t *testing.T) {
	store := newTestTieredSQLStore(t, 8, nil)
	ctx := context.Background()
	oldGeneration, err := store.putSmall(ctx, "k", []byte("old"))
	if err != nil {
		t.Fatalf("put old: %v", err)
	}
	newGeneration, err := store.putSmall(ctx, "k", []byte("new"))
	if err != nil {
		t.Fatalf("put new: %v", err)
	}
	if newGeneration <= oldGeneration {
		t.Fatalf("new generation %d <= old generation %d", newGeneration, oldGeneration)
	}
	if got := readTieredSQL(t, store, "k"); got != "new" {
		t.Fatalf("got %q", got)
	}
	count, err := store.cleanupQueueLen(ctx)
	if err != nil {
		t.Fatalf("cleanup count: %v", err)
	}
	if count != 1 {
		t.Fatalf("cleanup queue count = %d, want 1", count)
	}
	if exists, err := store.smallBlobExists(ctx, "k", oldGeneration); err != nil || !exists {
		t.Fatalf("old blob exists = %v, %v", exists, err)
	}
	cleaned, err := store.drainSmallCleanup(ctx, 10)
	if err != nil {
		t.Fatalf("drain cleanup: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned = %d, want 1", cleaned)
	}
	if exists, err := store.smallBlobExists(ctx, "k", oldGeneration); err != nil || exists {
		t.Fatalf("old blob exists after cleanup = %v, %v", exists, err)
	}
	if got := readTieredSQL(t, store, "k"); got != "new" {
		t.Fatalf("active payload changed after cleanup: %q", got)
	}
}

func TestTieredSQLOverwriteAcrossSmallAndLargeRefs(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	ctx := context.Background()

	smallGeneration, err := store.putSmall(ctx, "k", []byte("abc"))
	if err != nil {
		t.Fatalf("put small: %v", err)
	}
	largeGeneration, err := store.commitLargeRef(ctx, "k", 10, []byte("large-checksum"), []byte("objects/vol-1/large"))
	if err != nil {
		t.Fatalf("commit large: %v", err)
	}
	entry, ok, err := store.head(ctx, "k")
	if err != nil || !ok {
		t.Fatalf("head large = (%+v, %v, %v)", entry, ok, err)
	}
	if entry.generation != largeGeneration || entry.tier != tieredTierLarge {
		t.Fatalf("large entry = %+v", entry)
	}
	if count, err := store.cleanupQueueLen(ctx); err != nil || count != 1 {
		t.Fatalf("cleanup after small->large = %d, %v", count, err)
	}
	if exists, err := store.smallBlobExists(ctx, "k", smallGeneration); err != nil || !exists {
		t.Fatalf("old small blob exists = %v, %v", exists, err)
	}

	newSmallGeneration, err := store.putSmall(ctx, "k", []byte("xy"))
	if err != nil {
		t.Fatalf("put small again: %v", err)
	}
	entry, ok, err = store.head(ctx, "k")
	if err != nil || !ok || entry.generation != newSmallGeneration || entry.tier != tieredTierSmall {
		t.Fatalf("small entry = (%+v, %v, %v)", entry, ok, err)
	}
	if got := readTieredSQL(t, store, "k"); got != "xy" {
		t.Fatalf("got %q", got)
	}
	if count, err := store.cleanupQueueLen(ctx); err != nil || count != 2 {
		t.Fatalf("cleanup after large->small = %d, %v", count, err)
	}
	cleaned, err := store.drainSmallCleanup(ctx, 10)
	if err != nil {
		t.Fatalf("drain small cleanup: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned small rows = %d, want 1", cleaned)
	}
	if count, err := store.cleanupQueueLen(ctx); err != nil || count != 1 {
		t.Fatalf("large cleanup row must remain for S3 GC, count = %d, %v", count, err)
	}
}

func TestTieredSQLDeleteIsIndexDrivenAndCleanupQueued(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putSmall(ctx, "k", []byte("abc"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.delete(ctx, "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, err := store.head(ctx, "k"); err != nil || ok {
		t.Fatalf("head after delete = (%v, %v)", ok, err)
	}
	if exists, err := store.smallBlobExists(ctx, "k", generation); err != nil || !exists {
		t.Fatalf("deleted payload should remain orphan before cleanup = %v, %v", exists, err)
	}
	if count, err := store.cleanupQueueLen(ctx); err != nil || count != 1 {
		t.Fatalf("cleanup count = %d, %v", count, err)
	}
}

func TestTieredSQLCleanupNeverDeletesActiveGeneration(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putSmall(ctx, "k", []byte("abc"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.insertActiveCleanupForTest(ctx, "k", generation, tieredTierSmall); err != nil {
		t.Fatalf("insert active cleanup: %v", err)
	}
	cleaned, err := store.drainSmallCleanup(ctx, 10)
	if err != nil {
		t.Fatalf("drain cleanup: %v", err)
	}
	if cleaned != 0 {
		t.Fatalf("cleaned active generation = %d, want 0", cleaned)
	}
	if exists, err := store.smallBlobExists(ctx, "k", generation); err != nil || !exists {
		t.Fatalf("active blob exists = %v, %v", exists, err)
	}
	if got := readTieredSQL(t, store, "k"); got != "abc" {
		t.Fatalf("active read after cleanup = %q", got)
	}
}

func TestTieredSQLListUsesBinaryOrderingTokenAndDelimiter(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	ctx := context.Background()
	keys := []string{"p/b", "p/a", "p/a\x00x", "p/a\xff", "p/dir/1", "p/dir/2", "q/a"}
	for _, key := range keys {
		if _, err := store.putSmall(ctx, key, []byte("x")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	entries, hasMore, token, err := store.list(ctx, "p/", "", "", "", 3)
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if !hasMore || token != "p/a\xff" {
		t.Fatalf("page 1 hasMore=%v token=%q", hasMore, token)
	}
	if got := tieredEntryKeys(entries); strings.Join(got, "|") != "p/a|p/a\x00x|p/a\xff" {
		t.Fatalf("page 1 keys = %#v", got)
	}
	entries, hasMore, token, err = store.list(ctx, "p/", "", token, "", 10)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if hasMore || token != "" {
		t.Fatalf("page 2 hasMore=%v token=%q", hasMore, token)
	}
	if got := tieredEntryKeys(entries); strings.Join(got, "|") != "p/b|p/dir/1|p/dir/2" {
		t.Fatalf("page 2 keys = %#v", got)
	}

	entries, hasMore, token, err = store.list(ctx, "p/", "", "", "/", 10)
	if err != nil {
		t.Fatalf("delimiter list: %v", err)
	}
	if hasMore || token != "" {
		t.Fatalf("delimiter list hasMore=%v token=%q", hasMore, token)
	}
	if got := tieredEntryKeys(entries); strings.Join(got, "|") != "p/a|p/a\x00x|p/a\xff|p/b|p/dir/" {
		t.Fatalf("delimiter keys = %#v", got)
	}
}

func TestTieredSQLConcurrentPutPublishesCompleteGeneration(t *testing.T) {
	store := newTestTieredSQLStore(t, 16, nil)
	ctx := context.Background()
	values := [][]byte{[]byte("aaa"), []byte("bbbb"), []byte("ccccc"), []byte("dddddd")}
	var wg sync.WaitGroup
	for _, value := range values {
		value := append([]byte(nil), value...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.putSmall(ctx, "k", value); err != nil {
				t.Errorf("put %q: %v", value, err)
			}
		}()
	}
	wg.Wait()
	got := readTieredSQL(t, store, "k")
	found := false
	for _, value := range values {
		if got == string(value) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("got partial or unknown value %q", got)
	}
}

func TestTieredSQLCrashBeforeIndexCommitRollsBackSmallPayload(t *testing.T) {
	crash := errors.New("crash before index commit")
	store := newTestTieredSQLStore(t, 4, func(point tieredSQLFault) error {
		if point == tieredSQLFaultAfterSmallBlobInsert {
			return crash
		}
		return nil
	})
	ctx := context.Background()
	if _, err := store.putSmall(ctx, "k", []byte("abc")); !errors.Is(err, crash) {
		t.Fatalf("put with crash = %v, want %v", err, crash)
	}
	if _, ok, err := store.head(ctx, "k"); err != nil || ok {
		t.Fatalf("head after rollback = (%v, %v)", ok, err)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tiered_object_blob WHERE volume_id = ?", store.volumeID).Scan(&count); err != nil {
		t.Fatalf("count blobs: %v", err)
	}
	if count != 0 {
		t.Fatalf("blob count after rollback = %d, want 0", count)
	}
}

func TestTieredSQLCrashAfterIndexCommitKeepsNewGenerationVisible(t *testing.T) {
	store := newTestTieredSQLStore(t, 8, nil)
	ctx := context.Background()
	oldGeneration, err := store.putSmall(ctx, "k", []byte("old"))
	if err != nil {
		t.Fatalf("put old: %v", err)
	}
	crash := errors.New("crash after index commit")
	store.fault = func(point tieredSQLFault) error {
		if point == tieredSQLFaultAfterIndexCommit {
			return crash
		}
		return nil
	}
	newGeneration, err := store.putSmall(ctx, "k", []byte("new"))
	if !errors.Is(err, crash) {
		t.Fatalf("put with crash = %v, want %v", err, crash)
	}
	if newGeneration <= oldGeneration {
		t.Fatalf("new generation %d <= old generation %d", newGeneration, oldGeneration)
	}
	if got := readTieredSQL(t, store, "k"); got != "new" {
		t.Fatalf("got %q", got)
	}
	if exists, err := store.smallBlobExists(ctx, "k", oldGeneration); err != nil || !exists {
		t.Fatalf("old payload should remain for GC after post-commit crash = %v, %v", exists, err)
	}
	if count, err := store.cleanupQueueLen(ctx); err != nil || count != 1 {
		t.Fatalf("cleanup count = %d, %v", count, err)
	}
}

func TestTieredSQLHeadDetectsMissingIndexedPayload(t *testing.T) {
	store := newTestTieredSQLStore(t, 4, nil)
	ctx := context.Background()
	generation, err := store.putSmall(ctx, "k", []byte("abc"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, "DELETE FROM tiered_object_blob WHERE volume_id = ? AND object_key = ? AND generation = ?", store.volumeID, []byte("k"), int64(generation)); err != nil {
		t.Fatalf("delete blob: %v", err)
	}
	if _, _, err := store.head(ctx, "k"); !errors.Is(err, errTieredSQLCorruption) {
		t.Fatalf("head missing payload = %v, want corruption", err)
	}
}

func readTieredSQL(t *testing.T, store *tieredSQLStore, key string) string {
	t.Helper()
	r, err := store.getSmall(context.Background(), key, 0, -1)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer r.Close()
	var out bytes.Buffer
	if _, err := io.Copy(&out, r); err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return out.String()
}

func tieredEntryKeys(entries []tieredSQLIndexEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, string(entry.key))
	}
	return keys
}
