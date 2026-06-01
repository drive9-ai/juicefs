package object

import (
	"context"
	"errors"
)

var errTieredAdminInvalidLimit = errors.New("invalid tiered object store admin limit")
var errTieredAdminUnsafeCleanup = errors.New("unsafe tiered object store cleanup")

type tieredCleanupLimits struct {
	SmallQueue   int
	LargeQueue   int
	LargeOrphans int
}

type tieredCleanupResult struct {
	SmallQueued   int
	LargeQueued   int
	LargeOrphans  int
	BeforeCleanup tieredCheckReport
	AfterCleanup  tieredCheckReport
}

func (s *tieredObjectStore) cleanup(ctx context.Context, limits tieredCleanupLimits) (tieredCleanupResult, error) {
	var result tieredCleanupResult
	if limits.SmallQueue < 0 || limits.LargeQueue < 0 || limits.LargeOrphans < 0 {
		return result, errTieredAdminInvalidLimit
	}

	before, err := s.check(ctx)
	if err != nil {
		return result, err
	}
	result.BeforeCleanup = before

	if tieredCleanupIsDestructive(limits) && before.hasIndexedPayloadIssue() {
		return result, errTieredAdminUnsafeCleanup
	}

	if limits.SmallQueue > 0 {
		result.SmallQueued, err = s.drainSmallCleanup(ctx, limits.SmallQueue)
		if err != nil {
			return result, err
		}
	}
	if limits.LargeQueue > 0 {
		result.LargeQueued, err = s.drainLargeCleanup(ctx, limits.LargeQueue)
		if err != nil {
			return result, err
		}
	}
	if limits.LargeOrphans > 0 {
		result.LargeOrphans, err = s.drainLargeOrphans(ctx, limits.LargeOrphans)
		if err != nil {
			return result, err
		}
	}

	after, err := s.check(ctx)
	if err != nil {
		return result, err
	}
	result.AfterCleanup = after
	return result, nil
}

func tieredCleanupIsDestructive(limits tieredCleanupLimits) bool {
	return limits.SmallQueue > 0 || limits.LargeQueue > 0 || limits.LargeOrphans > 0
}

func (r tieredCheckReport) hasIndexedPayloadIssue() bool {
	for _, issue := range r.Issues {
		switch issue.Kind {
		case tieredCheckMissingIndexedPayload, tieredCheckCorruptIndexedPayload:
			return true
		}
	}
	return false
}
