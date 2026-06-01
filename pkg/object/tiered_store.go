package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

var errTieredStoreExperimental = errors.New("tiered object store is experimental and not registered")

type tieredObjectStore struct {
	DefaultObjectStorage
	index *tieredSQLStore
	large ObjectStorage
	fault func(tieredStoreFault) error
}

type tieredStoreFault string

const tieredStoreFaultAfterLargePayloadPut tieredStoreFault = "after-large-payload-put"

func newTieredObjectStore(index *tieredSQLStore, large ObjectStorage) (*tieredObjectStore, error) {
	if index == nil {
		return nil, errors.New("nil tiered sql index")
	}
	if large == nil {
		return nil, errors.New("nil large object store")
	}
	return &tieredObjectStore{index: index, large: large}, nil
}

func (s *tieredObjectStore) String() string {
	return "tiered://experimental/"
}

func (s *tieredObjectStore) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	entry, ok, err := s.index.head(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	if entry.tier == tieredTierSmall {
		return s.index.getSmall(ctx, key, off, limit)
	}
	if entry.tier != tieredTierLarge {
		return nil, fmt.Errorf("%w: unsupported tier %q", errTieredSQLCorruption, entry.tier)
	}
	if err := s.validateLargeHead(ctx, entry); err != nil {
		return nil, err
	}
	reader, err := s.large.Get(ctx, string(entry.payloadRef), off, limit)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: indexed large payload missing", errTieredSQLCorruption)
		}
		return nil, err
	}
	if off == 0 && limit < 0 {
		return newTieredValidatingReadCloser(reader, entry.checksum), nil
	}
	return reader, nil
}

func (s *tieredObjectStore) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	spool, err := newTieredSpool(s.index.smallThreshold)
	if err != nil {
		return err
	}
	defer spool.Close()

	checksum := sha256.New()
	size, smallData, err := spool.ReadFrom(io.TeeReader(in, checksum))
	if err != nil {
		return err
	}
	if size <= s.index.smallThreshold {
		_, err := s.index.putSmall(ctx, key, smallData)
		return err
	}

	generation, err := s.index.reserveGeneration(ctx)
	if err != nil {
		return err
	}
	payloadRef := s.payloadRef(generation)
	reader, err := spool.Reader()
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := s.large.Put(ctx, payloadRef, reader); err != nil {
		return err
	}
	if s.fault != nil {
		if err := s.fault(tieredStoreFaultAfterLargePayloadPut); err != nil {
			return err
		}
	}
	return s.index.commitLargeGenerationRef(ctx, key, generation, size, checksum.Sum(nil), []byte(payloadRef))
}

func (s *tieredObjectStore) Copy(ctx context.Context, dst, src string) error {
	return notSupported
}

func (s *tieredObjectStore) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	return s.index.delete(ctx, key)
}

func (s *tieredObjectStore) Head(ctx context.Context, key string) (Object, error) {
	entry, ok, err := s.index.head(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	if entry.tier == tieredTierLarge {
		if err := s.validateLargeHead(ctx, entry); err != nil {
			return nil, err
		}
	}
	return &obj{
		key:   key,
		size:  entry.size,
		mtime: entry.updatedAt,
		isDir: strings.HasSuffix(key, "/"),
	}, nil
}

func (s *tieredObjectStore) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	entries, hasMore, nextToken, err := s.index.list(ctx, prefix, startAfter, token, delimiter, limit)
	if err != nil {
		return nil, false, "", err
	}
	objects := make([]Object, 0, len(entries))
	for _, entry := range entries {
		key := string(entry.key)
		objects = append(objects, &obj{
			key:   key,
			size:  entry.size,
			mtime: entry.updatedAt,
			isDir: strings.HasSuffix(key, "/"),
		})
	}
	return objects, hasMore, nextToken, nil
}

func (s *tieredObjectStore) drainSmallCleanup(ctx context.Context, limit int) (int, error) {
	return s.index.drainSmallCleanup(ctx, limit)
}

func (s *tieredObjectStore) drainLargeCleanup(ctx context.Context, limit int) (int, error) {
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

func (s *tieredObjectStore) drainLargeOrphans(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	cleaned := 0
	marker := ""
	for {
		remaining := limit - cleaned
		objects, hasMore, nextMarker, err := s.large.List(ctx, s.payloadPrefix(), marker, "", "", int64(remaining), false)
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
			if cleaned >= limit {
				return cleaned, nil
			}
		}
		if !hasMore || nextMarker == "" {
			break
		}
		marker = nextMarker
	}
	return cleaned, nil
}

func (s *tieredObjectStore) payloadPrefix() string {
	return "objects/" + hex.EncodeToString(s.index.volumeID) + "/"
}

func (s *tieredObjectStore) payloadRef(generation uint64) string {
	return s.payloadPrefix() + strconv.FormatUint(generation, 10)
}

func (s *tieredObjectStore) validateLargeHead(ctx context.Context, entry tieredSQLIndexEntry) error {
	object, err := s.large.Head(ctx, string(entry.payloadRef))
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: indexed large payload missing", errTieredSQLCorruption)
		}
		return err
	}
	if object.Size() != entry.size {
		return fmt.Errorf("%w: indexed large payload size mismatch", errTieredSQLCorruption)
	}
	return nil
}

func ensureTieredStoreNotRegistered() error {
	if _, ok := storages["tiered"]; ok {
		return errTieredStoreExperimental
	}
	return nil
}

type tieredSpool struct {
	threshold int64
	file      *os.File
	path      string
	small     bytes.Buffer
	size      int64
}

func newTieredSpool(threshold int64) (*tieredSpool, error) {
	file, err := os.CreateTemp("", "juicefs-tiered-spool-*")
	if err != nil {
		return nil, err
	}
	return &tieredSpool{threshold: threshold, file: file, path: file.Name()}, nil
}

func (s *tieredSpool) ReadFrom(reader io.Reader) (int64, []byte, error) {
	buf := make([]byte, 128*1024)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if _, err := s.file.Write(chunk); err != nil {
				return s.size, nil, err
			}
			if s.size <= s.threshold {
				remaining := s.threshold + 1 - s.size
				if remaining > 0 {
					if int64(len(chunk)) > remaining {
						chunk = chunk[:remaining]
					}
					if _, err := s.small.Write(chunk); err != nil {
						return s.size, nil, err
					}
				}
			}
			s.size += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return s.size, nil, readErr
		}
	}
	if s.size <= s.threshold {
		return s.size, append([]byte(nil), s.small.Bytes()...), nil
	}
	return s.size, nil, nil
}

func (s *tieredSpool) Reader() (io.ReadCloser, error) {
	if err := s.file.Sync(); err != nil {
		return nil, err
	}
	if err := s.file.Close(); err != nil {
		return nil, err
	}
	return os.Open(s.path)
}

func (s *tieredSpool) Close() error {
	if s.file != nil {
		_ = s.file.Close()
	}
	if s.path != "" {
		return os.Remove(s.path)
	}
	return nil
}

type tieredValidatingReadCloser struct {
	reader io.ReadCloser
	hash   hashWriter
	want   []byte
}

type hashWriter interface {
	io.Writer
	Sum([]byte) []byte
}

func newTieredValidatingReadCloser(reader io.ReadCloser, want []byte) *tieredValidatingReadCloser {
	return &tieredValidatingReadCloser{reader: reader, hash: sha256.New(), want: append([]byte(nil), want...)}
}

func (r *tieredValidatingReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		_, _ = r.hash.Write(p[:n])
	}
	if errors.Is(err, io.EOF) && !bytes.Equal(r.hash.Sum(nil), r.want) {
		return n, fmt.Errorf("%w: indexed large payload checksum mismatch", errTieredSQLCorruption)
	}
	return n, err
}

func (r *tieredValidatingReadCloser) Close() error {
	return r.reader.Close()
}
