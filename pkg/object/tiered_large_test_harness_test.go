//go:build !nosqlite
// +build !nosqlite

package object

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type tieredLargeFault string

const tieredLargeFaultAfterPayloadPut tieredLargeFault = "after-large-payload-put"

type tieredLargeStore struct {
	index *tieredSQLStore
	large ObjectStorage
	fault func(tieredLargeFault) error
}

func newTieredLargeStore(index *tieredSQLStore, large ObjectStorage, fault func(tieredLargeFault) error) (*tieredLargeStore, error) {
	if index == nil {
		return nil, errors.New("nil tiered sql index")
	}
	if large == nil {
		return nil, errors.New("nil large object store")
	}
	return &tieredLargeStore{index: index, large: large, fault: fault}, nil
}

func (s *tieredLargeStore) putLarge(ctx context.Context, key string, in io.Reader) (uint64, error) {
	data, err := io.ReadAll(in)
	if err != nil {
		return 0, err
	}
	if int64(len(data)) <= s.index.smallThreshold {
		return 0, fmt.Errorf("%w: large payload size %d does not exceed threshold %d", errTieredSQLUnsupported, len(data), s.index.smallThreshold)
	}
	checksum := tieredChecksum(data)
	generation, err := s.index.reserveGeneration(ctx)
	if err != nil {
		return 0, err
	}
	payloadRef := s.payloadRef(generation)
	if err := s.large.Put(ctx, payloadRef, bytes.NewReader(data)); err != nil {
		return 0, err
	}
	if s.fault != nil {
		if err := s.fault(tieredLargeFaultAfterPayloadPut); err != nil {
			return generation, err
		}
	}
	if err := s.index.commitLargeGenerationRef(ctx, key, generation, int64(len(data)), checksum, []byte(payloadRef)); err != nil {
		return 0, err
	}
	return generation, nil
}

func (s *tieredLargeStore) getLarge(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	entry, ok, err := s.index.head(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	data, err := s.readLargePayload(ctx, entry)
	if err != nil {
		return nil, err
	}
	if off > int64(len(data)) {
		off = int64(len(data))
	}
	data = data[off:]
	if limit >= 0 && limit < int64(len(data)) {
		data = data[:limit]
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), nil
}

func (s *tieredLargeStore) headLarge(ctx context.Context, key string) (tieredSQLIndexEntry, bool, error) {
	entry, ok, err := s.index.head(ctx, key)
	if err != nil || !ok {
		return tieredSQLIndexEntry{}, ok, err
	}
	if entry.tier != tieredTierLarge {
		return tieredSQLIndexEntry{}, false, errTieredSQLUnsupported
	}
	if _, err := s.readLargePayload(ctx, entry); err != nil {
		return tieredSQLIndexEntry{}, false, err
	}
	return entry, true, nil
}

func (s *tieredLargeStore) delete(ctx context.Context, key string) error {
	return s.index.delete(ctx, key)
}

func (s *tieredLargeStore) drainLargeCleanup(ctx context.Context, limit int) (int, error) {
	items, err := s.index.cleanupItems(ctx, tieredTierLarge, limit)
	if err != nil {
		return 0, err
	}
	cleaned := 0
	for _, item := range items {
		active, err := s.index.isActiveGeneration(ctx, item.key, item.generation, item.tier)
		if err != nil {
			return cleaned, err
		}
		if active {
			if err := s.index.deleteCleanupQueueItem(ctx, item); err != nil {
				return cleaned, err
			}
			continue
		}
		if err := s.large.Delete(ctx, string(item.payloadRef)); err != nil && !os.IsNotExist(err) {
			return cleaned, err
		}
		if err := s.index.deleteCleanupQueueItem(ctx, item); err != nil {
			return cleaned, err
		}
		cleaned++
	}
	return cleaned, nil
}

func (s *tieredLargeStore) drainLargeOrphans(ctx context.Context, limit int) (int, error) {
	cleaned := 0
	marker := ""
	for {
		objects, hasMore, nextMarker, err := s.large.List(ctx, s.payloadPrefix(), marker, "", "", int64(limit), false)
		if err != nil {
			return cleaned, err
		}
		if len(objects) > 0 {
			marker = objects[len(objects)-1].Key()
		}
		for _, object := range objects {
			ref := object.Key()
			if !strings.HasPrefix(ref, s.payloadPrefix()) {
				continue
			}
			referenced, err := s.index.payloadRefReferenced(ctx, []byte(ref))
			if err != nil {
				return cleaned, err
			}
			if referenced {
				continue
			}
			if err := s.large.Delete(ctx, ref); err != nil && !os.IsNotExist(err) {
				return cleaned, err
			}
			cleaned++
		}
		if !hasMore || nextMarker == "" {
			break
		}
		marker = nextMarker
	}
	return cleaned, nil
}

func (s *tieredLargeStore) largePayloadExists(ctx context.Context, generation uint64) (bool, error) {
	_, err := s.large.Head(ctx, s.payloadRef(generation))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *tieredLargeStore) overwriteLargePayloadForTest(ctx context.Context, generation uint64, data []byte) error {
	return s.large.Put(ctx, s.payloadRef(generation), bytes.NewReader(data))
}

func (s *tieredLargeStore) readLargePayload(ctx context.Context, entry tieredSQLIndexEntry) ([]byte, error) {
	if entry.tier != tieredTierLarge {
		return nil, errTieredSQLUnsupported
	}
	object, err := s.large.Head(ctx, string(entry.payloadRef))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: indexed large payload missing", errTieredSQLCorruption)
		}
		return nil, err
	}
	if object.Size() != entry.size {
		return nil, fmt.Errorf("%w: indexed large payload size mismatch", errTieredSQLCorruption)
	}
	reader, err := s.large.Get(ctx, string(entry.payloadRef), 0, -1)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: indexed large payload missing", errTieredSQLCorruption)
		}
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != entry.size || !bytes.Equal(tieredChecksum(data), entry.checksum) {
		return nil, fmt.Errorf("%w: indexed large payload checksum or size mismatch", errTieredSQLCorruption)
	}
	return data, nil
}

func (s *tieredLargeStore) payloadPrefix() string {
	return "objects/" + hex.EncodeToString(s.index.volumeID) + "/"
}

func (s *tieredLargeStore) payloadRef(generation uint64) string {
	return s.payloadPrefix() + strconv.FormatUint(generation, 10)
}
