//go:build !nosqlite
// +build !nosqlite

package object

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestTieredCleanupRejectsNegativeLimits(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	for _, limits := range []tieredCleanupLimits{
		{SmallQueue: -1},
		{LargeQueue: -1},
		{LargeOrphans: -1},
	} {
		if _, err := store.cleanup(context.Background(), limits); !errors.Is(err, errTieredAdminInvalidLimit) {
			t.Fatalf("cleanup(%+v) = %v, want %v", limits, err, errTieredAdminInvalidLimit)
		}
	}
}

func TestTieredCleanupReportsBeforeAndAfterWithoutDeletingWhenLimitsZero(t *testing.T) {
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

	result, err := store.cleanup(ctx, tieredCleanupLimits{})
	if err != nil {
		t.Fatalf("cleanup with zero limits: %v", err)
	}
	if result.SmallQueued != 0 || result.LargeQueued != 0 || result.LargeOrphans != 0 {
		t.Fatalf("zero-limit cleanup result = %+v", result)
	}
	assertTieredCheckIssue(t, result.BeforeCleanup, tieredCheckOrphanSmallPayload, tieredTierSmall, "small")
	assertTieredCheckIssue(t, result.AfterCleanup, tieredCheckOrphanSmallPayload, tieredTierSmall, "small")
	if exists, err := store.index.smallBlobExists(ctx, "small", smallEntry.generation); err != nil || !exists {
		t.Fatalf("zero-limit cleanup deleted small payload = %v, %v", exists, err)
	}
}

func TestTieredCleanupDrainsQueuedAndOrphanPayloads(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	smallEntry, ok, err := store.index.head(ctx, "small")
	if err != nil || !ok {
		t.Fatalf("head small = (%+v, %v, %v)", smallEntry, ok, err)
	}
	if err := store.Put(ctx, "small", strings.NewReader("large-overwrite")); err != nil {
		t.Fatalf("small->large overwrite: %v", err)
	}
	largeEntry, ok, err := store.index.head(ctx, "small")
	if err != nil || !ok || largeEntry.tier != tieredTierLarge {
		t.Fatalf("head large overwrite = (%+v, %v, %v)", largeEntry, ok, err)
	}
	if err := store.Delete(ctx, "small"); err != nil {
		t.Fatalf("delete large: %v", err)
	}
	orphanGeneration, err := store.index.reserveGeneration(ctx)
	if err != nil {
		t.Fatalf("reserve orphan generation: %v", err)
	}
	orphanRef := store.payloadRef(orphanGeneration)
	if err := store.large.Put(ctx, orphanRef, strings.NewReader("large-orphan")); err != nil {
		t.Fatalf("put large orphan: %v", err)
	}

	result, err := store.cleanup(ctx, tieredCleanupLimits{
		SmallQueue:   10,
		LargeQueue:   10,
		LargeOrphans: 10,
	})
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if result.SmallQueued != 1 || result.LargeQueued != 1 || result.LargeOrphans != 1 {
		t.Fatalf("cleanup result = %+v", result)
	}
	if exists, err := store.index.smallBlobExists(ctx, "small", smallEntry.generation); err != nil || exists {
		t.Fatalf("old small payload exists after cleanup = %v, %v", exists, err)
	}
	if _, err := store.large.Head(ctx, string(largeEntry.payloadRef)); !os.IsNotExist(err) {
		t.Fatalf("queued large payload after cleanup = %v, want not exist", err)
	}
	if _, err := store.large.Head(ctx, orphanRef); !os.IsNotExist(err) {
		t.Fatalf("orphan large payload after cleanup = %v, want not exist", err)
	}
	if len(result.AfterCleanup.Issues) != 0 {
		t.Fatalf("after cleanup issues = %+v", result.AfterCleanup.Issues)
	}
}

func TestTieredCleanupFailsClosedWhenIndexedPayloadMissing(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "active", strings.NewReader("large-active")); err != nil {
		t.Fatalf("put active: %v", err)
	}
	activeEntry, ok, err := store.index.head(ctx, "active")
	if err != nil || !ok {
		t.Fatalf("head active = (%+v, %v, %v)", activeEntry, ok, err)
	}
	if err := store.large.Delete(ctx, string(activeEntry.payloadRef)); err != nil && !os.IsNotExist(err) {
		t.Fatalf("delete active payload: %v", err)
	}
	if err := store.Put(ctx, "queued", strings.NewReader("abc")); err != nil {
		t.Fatalf("put queued small: %v", err)
	}
	queuedEntry, ok, err := store.index.head(ctx, "queued")
	if err != nil || !ok {
		t.Fatalf("head queued = (%+v, %v, %v)", queuedEntry, ok, err)
	}
	if err := store.Delete(ctx, "queued"); err != nil {
		t.Fatalf("delete queued: %v", err)
	}
	orphanGeneration, err := store.index.reserveGeneration(ctx)
	if err != nil {
		t.Fatalf("reserve orphan generation: %v", err)
	}
	orphanRef := store.payloadRef(orphanGeneration)
	if err := store.large.Put(ctx, orphanRef, strings.NewReader("large-orphan")); err != nil {
		t.Fatalf("put large orphan: %v", err)
	}

	result, err := store.cleanup(ctx, tieredCleanupLimits{
		SmallQueue:   10,
		LargeOrphans: 10,
	})
	if !errors.Is(err, errTieredAdminUnsafeCleanup) {
		t.Fatalf("cleanup with missing active payload = %v, want %v", err, errTieredAdminUnsafeCleanup)
	}
	assertTieredCheckIssue(t, result.BeforeCleanup, tieredCheckMissingIndexedPayload, tieredTierLarge, "active")
	if result.SmallQueued != 0 || result.LargeQueued != 0 || result.LargeOrphans != 0 {
		t.Fatalf("unsafe cleanup mutated result counts = %+v", result)
	}
	if exists, err := store.index.smallBlobExists(ctx, "queued", queuedEntry.generation); err != nil || !exists {
		t.Fatalf("queued small payload deleted during unsafe cleanup = %v, %v", exists, err)
	}
	if _, err := store.large.Head(ctx, orphanRef); err != nil {
		t.Fatalf("large orphan deleted during unsafe cleanup: %v", err)
	}
}

func TestTieredCleanupZeroLimitsReportsIndexedCorruptionWithoutDeleting(t *testing.T) {
	store := newTestTieredObjectStore(t, 4)
	ctx := context.Background()
	if err := store.Put(ctx, "active", strings.NewReader("large-active")); err != nil {
		t.Fatalf("put active: %v", err)
	}
	activeEntry, ok, err := store.index.head(ctx, "active")
	if err != nil || !ok {
		t.Fatalf("head active = (%+v, %v, %v)", activeEntry, ok, err)
	}
	if err := store.large.Put(ctx, string(activeEntry.payloadRef), strings.NewReader("corrupt")); err != nil {
		t.Fatalf("corrupt active payload: %v", err)
	}
	if err := store.Put(ctx, "queued", strings.NewReader("abc")); err != nil {
		t.Fatalf("put queued small: %v", err)
	}
	queuedEntry, ok, err := store.index.head(ctx, "queued")
	if err != nil || !ok {
		t.Fatalf("head queued = (%+v, %v, %v)", queuedEntry, ok, err)
	}
	if err := store.Delete(ctx, "queued"); err != nil {
		t.Fatalf("delete queued: %v", err)
	}

	result, err := store.cleanup(ctx, tieredCleanupLimits{})
	if err != nil {
		t.Fatalf("zero-limit cleanup with corrupt payload: %v", err)
	}
	assertTieredCheckIssue(t, result.BeforeCleanup, tieredCheckCorruptIndexedPayload, tieredTierLarge, "active")
	assertTieredCheckIssue(t, result.AfterCleanup, tieredCheckCorruptIndexedPayload, tieredTierLarge, "active")
	if exists, err := store.index.smallBlobExists(ctx, "queued", queuedEntry.generation); err != nil || !exists {
		t.Fatalf("zero-limit cleanup deleted queued small payload = %v, %v", exists, err)
	}
}

func TestTieredCleanupDoesNotDeleteActiveLargeGeneration(t *testing.T) {
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

	result, err := store.cleanup(ctx, tieredCleanupLimits{LargeQueue: 10})
	if err != nil {
		t.Fatalf("cleanup active large: %v", err)
	}
	if result.LargeQueued != 0 {
		t.Fatalf("active large cleanup count = %d, want 0", result.LargeQueued)
	}
	if got := readTieredStore(t, store, "large", 0, -1); got != "large-data" {
		t.Fatalf("active large payload changed after cleanup: %q", got)
	}
	if len(result.AfterCleanup.Issues) != 0 {
		t.Fatalf("active cleanup left issues = %+v", result.AfterCleanup.Issues)
	}
}
