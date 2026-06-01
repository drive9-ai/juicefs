//go:build !nosqlite
// +build !nosqlite

package object

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestTieredCheckReportsCleanStore(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	report, err := store.check(ctx)
	if err != nil {
		t.Fatalf("check clean store: %v", err)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("clean store issues = %+v", report.Issues)
	}
}

func TestTieredCheckReportsMissingIndexedPayloads(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	smallEntry, ok, err := store.index.head(ctx, "small")
	if err != nil || !ok {
		t.Fatalf("head small = (%+v, %v, %v)", smallEntry, ok, err)
	}
	if _, err := store.index.db.ExecContext(ctx, "DELETE FROM tiered_object_blob WHERE volume_id = ? AND object_key = ? AND generation = ?", store.index.volumeID, []byte("small"), int64(smallEntry.generation)); err != nil {
		t.Fatalf("delete small payload: %v", err)
	}
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	largeEntry, ok, err := store.index.head(ctx, "large")
	if err != nil || !ok {
		t.Fatalf("head large = (%+v, %v, %v)", largeEntry, ok, err)
	}
	if err := store.large.Delete(ctx, string(largeEntry.payloadRef)); err != nil && !os.IsNotExist(err) {
		t.Fatalf("delete large payload: %v", err)
	}
	report, err := store.check(ctx)
	if err != nil {
		t.Fatalf("check missing payloads: %v", err)
	}
	assertTieredCheckIssue(t, report, tieredCheckMissingIndexedPayload, tieredTierSmall, "small")
	assertTieredCheckIssue(t, report, tieredCheckMissingIndexedPayload, tieredTierLarge, "large")
}

func TestTieredCheckReportsCorruptIndexedPayloads(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	smallEntry, ok, err := store.index.head(ctx, "small")
	if err != nil || !ok {
		t.Fatalf("head small = (%+v, %v, %v)", smallEntry, ok, err)
	}
	if _, err := store.index.db.ExecContext(ctx, "UPDATE tiered_object_blob SET data = ? WHERE volume_id = ? AND object_key = ? AND generation = ?", []byte("xyz"), store.index.volumeID, []byte("small"), int64(smallEntry.generation)); err != nil {
		t.Fatalf("corrupt small payload: %v", err)
	}
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put large: %v", err)
	}
	largeEntry, ok, err := store.index.head(ctx, "large")
	if err != nil || !ok {
		t.Fatalf("head large = (%+v, %v, %v)", largeEntry, ok, err)
	}
	if err := store.large.Put(ctx, string(largeEntry.payloadRef), strings.NewReader("corrupt!!!")); err != nil {
		t.Fatalf("corrupt large payload: %v", err)
	}
	report, err := store.check(ctx)
	if err != nil {
		t.Fatalf("check corrupt payloads: %v", err)
	}
	assertTieredCheckIssue(t, report, tieredCheckCorruptIndexedPayload, tieredTierSmall, "small")
	assertTieredCheckIssue(t, report, tieredCheckCorruptIndexedPayload, tieredTierLarge, "large")
}

func TestTieredCheckReportsOrphansWithoutDeleting(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	smallEntry, ok, err := store.index.head(ctx, "small")
	if err != nil || !ok {
		t.Fatalf("head small = (%+v, %v, %v)", smallEntry, ok, err)
	}
	if err := store.Delete(ctx, "small"); err != nil {
		t.Fatalf("delete small: %v", err)
	}
	largeGeneration, err := store.index.reserveGeneration(ctx)
	if err != nil {
		t.Fatalf("reserve orphan generation: %v", err)
	}
	largeRef := store.payloadRef(largeGeneration)
	if err := store.large.Put(ctx, largeRef, strings.NewReader("large-orphan")); err != nil {
		t.Fatalf("put large orphan: %v", err)
	}
	if err := store.Put(ctx, "large", strings.NewReader("large-data")); err != nil {
		t.Fatalf("put queued large: %v", err)
	}
	queuedEntry, ok, err := store.index.head(ctx, "large")
	if err != nil || !ok {
		t.Fatalf("head queued large = (%+v, %v, %v)", queuedEntry, ok, err)
	}
	if err := store.Delete(ctx, "large"); err != nil {
		t.Fatalf("delete queued large: %v", err)
	}
	report, err := store.check(ctx)
	if err != nil {
		t.Fatalf("check orphans: %v", err)
	}
	assertTieredCheckIssue(t, report, tieredCheckOrphanSmallPayload, tieredTierSmall, "small")
	assertTieredCheckIssue(t, report, tieredCheckOrphanLargePayload, tieredTierLarge, "")
	if countTieredCheckIssues(report, tieredCheckOrphanLargePayload, tieredTierLarge) != 2 {
		t.Fatalf("large orphan issue count = %+v, want manual and queued orphan", report.Issues)
	}
	if exists, err := store.index.smallBlobExists(ctx, "small", smallEntry.generation); err != nil || !exists {
		t.Fatalf("small orphan exists after check = %v, %v", exists, err)
	}
	if _, err := store.large.Head(ctx, largeRef); err != nil {
		t.Fatalf("large orphan exists after check: %v", err)
	}
	if _, err := store.large.Head(ctx, string(queuedEntry.payloadRef)); err != nil {
		t.Fatalf("queued large orphan exists after check: %v", err)
	}
}

func TestTieredCheckDoesNotReportActiveGenerationAsOrphan(t *testing.T) {
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
	report, err := store.check(ctx)
	if err != nil {
		t.Fatalf("check active cleanup: %v", err)
	}
	if hasTieredCheckIssue(report, tieredCheckOrphanLargePayload, tieredTierLarge, "") {
		t.Fatalf("active large generation reported as orphan: %+v", report.Issues)
	}
}

func assertTieredCheckIssue(t *testing.T, report tieredCheckReport, kind tieredCheckIssueKind, tier, key string) {
	t.Helper()
	if !hasTieredCheckIssue(report, kind, tier, key) {
		t.Fatalf("missing issue kind=%s tier=%s key=%q in %+v", kind, tier, key, report.Issues)
	}
}

func hasTieredCheckIssue(report tieredCheckReport, kind tieredCheckIssueKind, tier, key string) bool {
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

func countTieredCheckIssues(report tieredCheckReport, kind tieredCheckIssueKind, tier string) int {
	count := 0
	for _, issue := range report.Issues {
		if issue.Kind == kind && issue.Tier == tier {
			count++
		}
	}
	return count
}
