//go:build !nomysql && !nos3
// +build !nomysql,!nos3

package object

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func newMySQLMinIOTieredStore(t *testing.T, threshold int64) *tieredObjectStore {
	t.Helper()
	if os.Getenv("MINIO_TEST_BUCKET") == "" {
		t.Skip("MINIO_TEST_BUCKET not set")
	}
	index := newMySQLTieredSQLStore(t, threshold)
	large, err := newMinio(os.Getenv("MINIO_TEST_BUCKET"), os.Getenv("MINIO_ACCESS_KEY"), os.Getenv("MINIO_SECRET_KEY"), "")
	if err != nil {
		t.Fatalf("new minio large store: %v", err)
	}
	ctx := context.Background()
	if err := large.Create(ctx); err != nil {
		t.Fatalf("create minio bucket: %v", err)
	}
	store, err := newTieredObjectStore(index, large)
	if err != nil {
		t.Fatalf("new tiered store: %v", err)
	}
	t.Cleanup(func() {
		cleanupTieredLargePayloads(t, store)
	})
	return store
}

func TestTieredStoreMySQLMinIOIntegrationRoutingCleanupAndCheck(t *testing.T) {
	store := newMySQLMinIOTieredStore(t, 4)
	ctx := context.Background()

	if err := store.Put(ctx, "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	if got := readTieredStoreIntegration(t, store, "small"); got != "abc" {
		t.Fatalf("small got %q", got)
	}
	if got := readTieredStoreIntegration(t, store, "large"); got != "large-data" {
		t.Fatalf("large got %q", got)
	}

	if err := store.Copy(ctx, "copy", "large"); !errors.Is(err, notSupported) {
		t.Fatalf("copy = %v, want %v", err, notSupported)
	}
	if _, err := store.CreateMultipartUpload(ctx, "mpu"); !errors.Is(err, notSupported) {
		t.Fatalf("create multipart = %v, want %v", err, notSupported)
	}

	if err := store.Put(ctx, "k", strings.NewReader("tiny")); err != nil {
		t.Fatalf("put initial small: %v", err)
	}
	if err := store.Put(ctx, "k", strings.NewReader("large-overwrite")); err != nil {
		t.Fatalf("small->large overwrite: %v", err)
	}
	if got := readTieredStoreIntegration(t, store, "k"); got != "large-overwrite" {
		t.Fatalf("after small->large got %q", got)
	}
	if err := store.Put(ctx, "k", strings.NewReader("xy")); err != nil {
		t.Fatalf("large->small overwrite: %v", err)
	}
	if got := readTieredStoreIntegration(t, store, "k"); got != "xy" {
		t.Fatalf("after large->small got %q", got)
	}
	if _, _, _, err := store.List(ctx, "", "", "", "", 100, false); err != nil {
		t.Fatalf("list visible objects: %v", err)
	}

	if _, err := store.check(ctx); err != nil {
		t.Fatalf("check before cleanup: %v", err)
	}
	if _, err := store.drainSmallCleanup(ctx, 100); err != nil {
		t.Fatalf("drain small cleanup: %v", err)
	}
	if _, err := store.drainLargeCleanup(ctx, 100); err != nil {
		t.Fatalf("drain large cleanup: %v", err)
	}
	report, err := store.check(ctx)
	if err != nil {
		t.Fatalf("check after cleanup: %v", err)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("post-cleanup issues = %+v", report.Issues)
	}
}

func TestTieredStoreMySQLMinIOLargeUploadBeforeIndexFailureIsInvisibleAndRecoverable(t *testing.T) {
	store := newMySQLMinIOTieredStore(t, 4)
	crash := errors.New("crash before mysql index commit")
	store.fault = func(point tieredStoreFault) error {
		if point == tieredStoreFaultAfterLargePayloadPut {
			return crash
		}
		return nil
	}
	ctx := context.Background()

	if err := store.Put(ctx, "large", strings.NewReader("large-data")); !errors.Is(err, crash) {
		t.Fatalf("put with injected crash = %v, want %v", err, crash)
	}
	if _, err := store.Head(ctx, "large"); !os.IsNotExist(err) {
		t.Fatalf("public head after failed index commit = %v, want not exist", err)
	}
	if reader, err := store.Get(ctx, "large", 0, -1); !os.IsNotExist(err) {
		if err == nil {
			_ = reader.Close()
		}
		t.Fatalf("public get after failed index commit = %v, want not exist", err)
	}
	objects, hasMore, token, err := store.List(ctx, "large", "", "", "", 100, false)
	if err != nil {
		t.Fatalf("public list after failed index commit: %v", err)
	}
	if len(objects) != 0 || hasMore || token != "" {
		t.Fatalf("public list after failed index commit = objects %d hasMore %v token %q, want empty", len(objects), hasMore, token)
	}

	report, err := store.check(ctx)
	if err != nil {
		t.Fatalf("check after failed index commit: %v", err)
	}
	if !hasTieredIntegrationCheckIssue(report, tieredCheckOrphanLargePayload, tieredTierLarge, "") {
		t.Fatalf("missing large orphan issue in %+v", report.Issues)
	}

	cleaned, err := store.drainLargeOrphans(ctx, 1)
	if err != nil {
		t.Fatalf("drain large orphan: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned large orphans = %d, want 1", cleaned)
	}
	report, err = store.check(ctx)
	if err != nil {
		t.Fatalf("check after orphan cleanup: %v", err)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("issues after orphan cleanup = %+v", report.Issues)
	}
}

func readTieredStoreIntegration(t *testing.T, store *tieredObjectStore, key string) string {
	t.Helper()
	reader, err := store.Get(context.Background(), key, 0, -1)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return string(data)
}

func cleanupTieredLargePayloads(t *testing.T, store *tieredObjectStore) {
	t.Helper()
	ctx := context.Background()
	marker := ""
	for {
		objects, hasMore, nextMarker, err := store.large.List(ctx, store.payloadPrefix(), marker, "", "", 1000, false)
		if err != nil {
			t.Logf("cleanup list large payloads: %v", err)
			return
		}
		for _, object := range objects {
			if err := store.large.Delete(ctx, object.Key()); err != nil && !os.IsNotExist(err) {
				t.Logf("cleanup large payload %q: %v", object.Key(), err)
			}
		}
		if !hasMore || nextMarker == "" {
			return
		}
		marker = nextMarker
	}
}

func hasTieredIntegrationCheckIssue(report tieredCheckReport, kind tieredCheckIssueKind, tier, key string) bool {
	for _, issue := range report.Issues {
		if issue.Kind != kind || issue.Tier != tier {
			continue
		}
		if key != "" && string(issue.Key) != key {
			continue
		}
		return true
	}
	return false
}
