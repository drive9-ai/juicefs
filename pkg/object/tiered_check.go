package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

type tieredCheckIssueKind string

const (
	tieredCheckMissingIndexedPayload tieredCheckIssueKind = "missing_indexed_payload"
	tieredCheckCorruptIndexedPayload tieredCheckIssueKind = "corrupt_indexed_payload"
	tieredCheckOrphanSmallPayload    tieredCheckIssueKind = "orphan_small_payload"
	tieredCheckOrphanLargePayload    tieredCheckIssueKind = "orphan_large_payload"
)

type tieredCheckIssue struct {
	Kind       tieredCheckIssueKind
	Tier       string
	Key        []byte
	Generation uint64
	PayloadRef []byte
	Message    string
}

type tieredCheckReport struct {
	Issues []tieredCheckIssue
}

func (r *tieredCheckReport) add(issue tieredCheckIssue) {
	r.Issues = append(r.Issues, issue)
}

func (s *tieredObjectStore) check(ctx context.Context) (tieredCheckReport, error) {
	var report tieredCheckReport
	entries, err := s.index.activeIndexEntries(ctx)
	if err != nil {
		return report, err
	}
	for _, entry := range entries {
		switch entry.tier {
		case tieredTierSmall:
			missing, corrupt, err := s.index.checkSmallPayload(ctx, entry)
			if err != nil {
				return report, err
			}
			if missing {
				report.add(tieredCheckIssue{
					Kind:       tieredCheckMissingIndexedPayload,
					Tier:       entry.tier,
					Key:        append([]byte(nil), entry.key...),
					Generation: entry.generation,
					PayloadRef: append([]byte(nil), entry.payloadRef...),
					Message:    "indexed small payload is missing",
				})
			}
			if corrupt {
				report.add(tieredCheckIssue{
					Kind:       tieredCheckCorruptIndexedPayload,
					Tier:       entry.tier,
					Key:        append([]byte(nil), entry.key...),
					Generation: entry.generation,
					PayloadRef: append([]byte(nil), entry.payloadRef...),
					Message:    "indexed small payload checksum or size mismatch",
				})
			}
		case tieredTierLarge:
			missing, corrupt, err := s.checkLargePayload(ctx, entry)
			if err != nil {
				return report, err
			}
			if missing {
				report.add(tieredCheckIssue{
					Kind:       tieredCheckMissingIndexedPayload,
					Tier:       entry.tier,
					Key:        append([]byte(nil), entry.key...),
					Generation: entry.generation,
					PayloadRef: append([]byte(nil), entry.payloadRef...),
					Message:    "indexed large payload is missing",
				})
			}
			if corrupt {
				report.add(tieredCheckIssue{
					Kind:       tieredCheckCorruptIndexedPayload,
					Tier:       entry.tier,
					Key:        append([]byte(nil), entry.key...),
					Generation: entry.generation,
					PayloadRef: append([]byte(nil), entry.payloadRef...),
					Message:    "indexed large payload checksum or size mismatch",
				})
			}
		default:
			report.add(tieredCheckIssue{
				Kind:       tieredCheckCorruptIndexedPayload,
				Tier:       entry.tier,
				Key:        append([]byte(nil), entry.key...),
				Generation: entry.generation,
				PayloadRef: append([]byte(nil), entry.payloadRef...),
				Message:    fmt.Sprintf("unsupported indexed tier %q", entry.tier),
			})
		}
	}
	if err := s.checkSmallOrphans(ctx, &report); err != nil {
		return report, err
	}
	if err := s.checkLargeOrphans(ctx, &report); err != nil {
		return report, err
	}
	return report, nil
}

func (s *tieredObjectStore) checkLargePayload(ctx context.Context, entry tieredSQLIndexEntry) (bool, bool, error) {
	object, err := s.large.Head(ctx, string(entry.payloadRef))
	if err != nil {
		if os.IsNotExist(err) {
			return true, false, nil
		}
		return false, false, err
	}
	if object.Size() != entry.size {
		return false, true, nil
	}
	reader, err := s.large.Get(ctx, string(entry.payloadRef), 0, -1)
	if err != nil {
		if os.IsNotExist(err) {
			return true, false, nil
		}
		return false, false, err
	}
	defer reader.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, reader)
	if err != nil {
		return false, false, err
	}
	if size != entry.size || !bytes.Equal(hash.Sum(nil), entry.checksum) {
		return false, true, nil
	}
	return false, false, nil
}

func (s *tieredObjectStore) checkSmallOrphans(ctx context.Context, report *tieredCheckReport) error {
	blobs, err := s.index.smallPayloadItems(ctx)
	if err != nil {
		return err
	}
	for _, blob := range blobs {
		active, err := s.index.isActiveGeneration(ctx, blob.key, blob.generation, tieredTierSmall)
		if err != nil {
			return err
		}
		if active {
			continue
		}
		report.add(tieredCheckIssue{
			Kind:       tieredCheckOrphanSmallPayload,
			Tier:       tieredTierSmall,
			Key:        append([]byte(nil), blob.key...),
			Generation: blob.generation,
			PayloadRef: generationRef(blob.generation),
			Message:    "small payload is not referenced by active index",
		})
	}
	return nil
}

func (s *tieredObjectStore) checkLargeOrphans(ctx context.Context, report *tieredCheckReport) error {
	marker := ""
	for {
		objects, hasMore, nextMarker, err := s.large.List(ctx, s.payloadPrefix(), marker, "", "", 1000, false)
		if err != nil {
			return err
		}
		if len(objects) > 0 {
			marker = objects[len(objects)-1].Key()
		}
		for _, object := range objects {
			ref := object.Key()
			referenced, err := s.index.payloadRefActive(ctx, []byte(ref))
			if err != nil {
				return err
			}
			if referenced {
				continue
			}
			report.add(tieredCheckIssue{
				Kind:       tieredCheckOrphanLargePayload,
				Tier:       tieredTierLarge,
				PayloadRef: []byte(ref),
				Message:    "large payload is not referenced by active index",
			})
		}
		if !hasMore || nextMarker == "" {
			return nil
		}
		marker = nextMarker
	}
}
