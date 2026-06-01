package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

type tieredSQLDialect string

const (
	tieredDialectSQLite tieredSQLDialect = "sqlite3"
	tieredDialectMySQL  tieredSQLDialect = "mysql"

	tieredSQLSchemaVersion = "1"
	tieredSQLChecksum      = "sha256"

	tieredIndexStateActive = "active"
	tieredTierSmall        = "tidb"
	tieredTierLarge        = "s3"
)

var (
	errTieredSQLRetry          = errors.New("retry tiered sql transaction")
	errTieredSQLConfigMismatch = errors.New("tiered object store config mismatch")
	errTieredSQLCorruption     = errors.New("tiered object store corruption")
	errTieredSQLUnsupported    = errors.New("tiered object store operation unsupported")
)

type tieredSQLConfig struct {
	Dialect        tieredSQLDialect
	VolumeID       []byte
	SmallThreshold int64
	Now            func() time.Time
	Fault          func(tieredSQLFault) error
}

type tieredSQLFault string

const (
	tieredSQLFaultAfterSmallBlobInsert tieredSQLFault = "after-small-blob-insert"
	tieredSQLFaultAfterIndexCommit     tieredSQLFault = "after-index-commit"
)

type tieredSQLStore struct {
	db              *sql.DB
	dialect         tieredSQLDialect
	volumeID        []byte
	smallThreshold  int64
	now             func() time.Time
	fault           func(tieredSQLFault) error
	selectForUpdate string
}

type tieredSQLIndexEntry struct {
	key        []byte
	generation uint64
	tier       string
	size       int64
	checksum   []byte
	state      string
	payloadRef []byte
	updatedAt  time.Time
}

type tieredSQLCleanupItem struct {
	key        []byte
	generation uint64
	tier       string
	payloadRef []byte
	reason     string
	createdAt  time.Time
}

type tieredSQLSmallPayloadItem struct {
	key        []byte
	generation uint64
}

func newTieredSQLStore(ctx context.Context, db *sql.DB, cfg tieredSQLConfig) (*tieredSQLStore, error) {
	if db == nil {
		return nil, errors.New("nil db")
	}
	if len(cfg.VolumeID) == 0 {
		return nil, errors.New("empty volume id")
	}
	if cfg.SmallThreshold < 0 {
		return nil, errors.New("negative small threshold")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	store := &tieredSQLStore{
		db:             db,
		dialect:        cfg.Dialect,
		volumeID:       append([]byte(nil), cfg.VolumeID...),
		smallThreshold: cfg.SmallThreshold,
		now:            cfg.Now,
		fault:          cfg.Fault,
	}
	if store.dialect == "" {
		store.dialect = tieredDialectMySQL
	}
	if store.dialect == tieredDialectMySQL {
		store.selectForUpdate = " FOR UPDATE"
	}
	if err := store.createSchema(ctx); err != nil {
		return nil, err
	}
	if err := store.verifyVolumeConfig(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *tieredSQLStore) putSmall(ctx context.Context, key string, data []byte) (uint64, error) {
	if int64(len(data)) > s.smallThreshold {
		return 0, fmt.Errorf("%w: small payload size %d exceeds threshold %d", errTieredSQLUnsupported, len(data), s.smallThreshold)
	}
	checksum := tieredChecksum(data)
	var generation uint64
	err := s.withRetry(ctx, func(tx *sql.Tx) error {
		var err error
		generation, err = s.nextGeneration(ctx, tx)
		if err != nil {
			return err
		}
		old, hadOld, err := s.getIndexForUpdate(ctx, tx, []byte(key))
		if err != nil {
			return err
		}
		if err := s.insertSmallBlob(ctx, tx, []byte(key), generation, data, checksum); err != nil {
			return err
		}
		if err := s.injectFault(tieredSQLFaultAfterSmallBlobInsert); err != nil {
			return err
		}
		entry := tieredSQLIndexEntry{
			key:        []byte(key),
			generation: generation,
			tier:       tieredTierSmall,
			size:       int64(len(data)),
			checksum:   checksum,
			state:      tieredIndexStateActive,
			payloadRef: generationRef(generation),
			updatedAt:  s.now(),
		}
		if err := s.commitIndex(ctx, tx, old, hadOld, entry); err != nil {
			return err
		}
		if hadOld {
			return s.enqueueCleanup(ctx, tx, old, "overwrite")
		}
		return nil
	})
	return generation, err
}

func (s *tieredSQLStore) commitLargeRef(ctx context.Context, key string, size int64, checksum, payloadRef []byte) (uint64, error) {
	generation, err := s.reserveGeneration(ctx)
	if err != nil {
		return 0, err
	}
	return generation, s.commitLargeGenerationRef(ctx, key, generation, size, checksum, payloadRef)
}

func (s *tieredSQLStore) commitLargeGenerationRef(ctx context.Context, key string, generation uint64, size int64, checksum, payloadRef []byte) error {
	if size <= s.smallThreshold {
		return fmt.Errorf("%w: large payload size %d does not exceed threshold %d", errTieredSQLUnsupported, size, s.smallThreshold)
	}
	if len(payloadRef) == 0 {
		return errors.New("empty large payload ref")
	}
	return s.withRetry(ctx, func(tx *sql.Tx) error {
		old, hadOld, err := s.getIndexForUpdate(ctx, tx, []byte(key))
		if err != nil {
			return err
		}
		entry := tieredSQLIndexEntry{
			key:        []byte(key),
			generation: generation,
			tier:       tieredTierLarge,
			size:       size,
			checksum:   append([]byte(nil), checksum...),
			state:      tieredIndexStateActive,
			payloadRef: append([]byte(nil), payloadRef...),
			updatedAt:  s.now(),
		}
		if err := s.commitIndex(ctx, tx, old, hadOld, entry); err != nil {
			return err
		}
		if hadOld {
			return s.enqueueCleanup(ctx, tx, old, "overwrite")
		}
		return nil
	})
}

func (s *tieredSQLStore) getSmall(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	entry, ok, err := s.head(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	if entry.tier != tieredTierSmall {
		return nil, errTieredSQLUnsupported
	}
	data, err := s.readSmallPayload(ctx, entry)
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

func (s *tieredSQLStore) head(ctx context.Context, key string) (tieredSQLIndexEntry, bool, error) {
	var entry tieredSQLIndexEntry
	row := s.db.QueryRowContext(ctx, "SELECT object_key, generation, tier, size, checksum, state, payload_ref, updated_at FROM tiered_object_index WHERE volume_id = ? AND object_key = ?", s.volumeID, []byte(key))
	err := scanTieredIndex(row, &entry)
	if errors.Is(err, sql.ErrNoRows) {
		return tieredSQLIndexEntry{}, false, nil
	}
	if err != nil {
		return tieredSQLIndexEntry{}, false, err
	}
	if entry.tier == tieredTierSmall {
		if _, err := s.readSmallPayload(ctx, entry); err != nil {
			return tieredSQLIndexEntry{}, false, err
		}
	}
	return entry, true, nil
}

func (s *tieredSQLStore) delete(ctx context.Context, key string) error {
	return s.withRetry(ctx, func(tx *sql.Tx) error {
		old, ok, err := s.getIndexForUpdate(ctx, tx, []byte(key))
		if err != nil || !ok {
			return err
		}
		res, err := tx.ExecContext(ctx, "DELETE FROM tiered_object_index WHERE volume_id = ? AND object_key = ? AND generation = ?", s.volumeID, []byte(key), int64(old.generation))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return errTieredSQLRetry
		}
		return s.enqueueCleanup(ctx, tx, old, "delete")
	})
}

func (s *tieredSQLStore) list(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64) ([]tieredSQLIndexEntry, bool, string, error) {
	if token != "" {
		startAfter = token
	}
	rows, err := s.db.QueryContext(ctx, "SELECT object_key, generation, tier, size, checksum, state, payload_ref, updated_at FROM tiered_object_index WHERE volume_id = ? AND state = ? AND object_key > ? ORDER BY object_key", s.volumeID, tieredIndexStateActive, []byte(startAfter))
	if err != nil {
		return nil, false, "", err
	}
	defer rows.Close()

	entriesByKey := make(map[string]tieredSQLIndexEntry)
	keys := make([]string, 0)
	for rows.Next() {
		var entry tieredSQLIndexEntry
		if err := scanTieredIndex(rows, &entry); err != nil {
			return nil, false, "", err
		}
		key := string(entry.key)
		if !strings.HasPrefix(key, prefix) {
			if prefix != "" && key > prefix {
				break
			}
			continue
		}
		listKey := key
		if delimiter != "" {
			rest := strings.TrimPrefix(key, prefix)
			if i := strings.Index(rest, delimiter); i >= 0 {
				listKey = prefix + rest[:i+len(delimiter)]
			}
		}
		if listKey <= startAfter {
			continue
		}
		if _, ok := entriesByKey[listKey]; ok {
			continue
		}
		listEntry := entry
		listEntry.key = []byte(listKey)
		if listKey != key {
			listEntry.size = 0
			listEntry.tier = ""
			listEntry.payloadRef = nil
			listEntry.checksum = nil
		}
		entriesByKey[listKey] = listEntry
		keys = append(keys, listKey)
	}
	if err := rows.Err(); err != nil {
		return nil, false, "", err
	}
	sort.Strings(keys)
	hasMore := false
	nextToken := ""
	if limit > 0 && int64(len(keys)) > limit {
		hasMore = true
		nextToken = keys[limit-1]
		keys = keys[:limit]
	}
	entries := make([]tieredSQLIndexEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, entriesByKey[key])
	}
	return entries, hasMore, nextToken, nil
}

func (s *tieredSQLStore) activeIndexEntries(ctx context.Context) ([]tieredSQLIndexEntry, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT object_key, generation, tier, size, checksum, state, payload_ref, updated_at FROM tiered_object_index WHERE volume_id = ? AND state = ? ORDER BY object_key", s.volumeID, tieredIndexStateActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]tieredSQLIndexEntry, 0)
	for rows.Next() {
		var entry tieredSQLIndexEntry
		if err := scanTieredIndex(rows, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (s *tieredSQLStore) drainSmallCleanup(ctx context.Context, limit int) (int, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT object_key, generation, tier, payload_ref, reason, created_at FROM tiered_object_gc_queue WHERE volume_id = ? AND tier = ? ORDER BY created_at, object_key LIMIT ?", s.volumeID, tieredTierSmall, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	items := make([]tieredSQLCleanupItem, 0)
	for rows.Next() {
		var item tieredSQLCleanupItem
		var gen int64
		if err := rows.Scan(&item.key, &gen, &item.tier, &item.payloadRef, &item.reason, &item.createdAt); err != nil {
			return 0, err
		}
		item.generation = uint64(gen)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	cleaned := 0
	for _, item := range items {
		err := s.withRetry(ctx, func(tx *sql.Tx) error {
			active, ok, err := s.getIndexForUpdate(ctx, tx, item.key)
			if err != nil {
				return err
			}
			if ok && active.generation == item.generation && active.tier == item.tier {
				return s.deleteCleanupItem(ctx, tx, item)
			}
			if item.tier == tieredTierSmall {
				if _, err := tx.ExecContext(ctx, "DELETE FROM tiered_object_blob WHERE volume_id = ? AND object_key = ? AND generation = ?", s.volumeID, item.key, int64(item.generation)); err != nil {
					return err
				}
			}
			if err := s.deleteCleanupItem(ctx, tx, item); err != nil {
				return err
			}
			cleaned++
			return nil
		})
		if err != nil {
			return cleaned, err
		}
	}
	return cleaned, nil
}

func (s *tieredSQLStore) cleanupQueueLen(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tiered_object_gc_queue WHERE volume_id = ?", s.volumeID).Scan(&count)
	return count, err
}

func (s *tieredSQLStore) cleanupItems(ctx context.Context, tier string, limit int) ([]tieredSQLCleanupItem, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT object_key, generation, tier, payload_ref, reason, created_at FROM tiered_object_gc_queue WHERE volume_id = ? AND tier = ? ORDER BY created_at, object_key LIMIT ?", s.volumeID, tier, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]tieredSQLCleanupItem, 0)
	for rows.Next() {
		var item tieredSQLCleanupItem
		var gen int64
		if err := rows.Scan(&item.key, &gen, &item.tier, &item.payloadRef, &item.reason, &item.createdAt); err != nil {
			return nil, err
		}
		item.generation = uint64(gen)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *tieredSQLStore) isActiveGeneration(ctx context.Context, key []byte, generation uint64, tier string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tiered_object_index WHERE volume_id = ? AND object_key = ? AND generation = ? AND tier = ?", s.volumeID, key, int64(generation), tier).Scan(&count)
	return count > 0, err
}

func (s *tieredSQLStore) payloadRefReferenced(ctx context.Context, payloadRef []byte) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tiered_object_index WHERE volume_id = ? AND payload_ref = ?", s.volumeID, payloadRef).Scan(&count)
	if err != nil || count > 0 {
		return count > 0, err
	}
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tiered_object_gc_queue WHERE volume_id = ? AND payload_ref = ?", s.volumeID, payloadRef).Scan(&count)
	return count > 0, err
}

func (s *tieredSQLStore) payloadRefActive(ctx context.Context, payloadRef []byte) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tiered_object_index WHERE volume_id = ? AND payload_ref = ?", s.volumeID, payloadRef).Scan(&count)
	return count > 0, err
}

func (s *tieredSQLStore) smallBlobExists(ctx context.Context, key string, generation uint64) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tiered_object_blob WHERE volume_id = ? AND object_key = ? AND generation = ?", s.volumeID, []byte(key), int64(generation)).Scan(&count)
	return count > 0, err
}

func (s *tieredSQLStore) smallPayloadItems(ctx context.Context) ([]tieredSQLSmallPayloadItem, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT object_key, generation FROM tiered_object_blob WHERE volume_id = ? ORDER BY object_key, generation", s.volumeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]tieredSQLSmallPayloadItem, 0)
	for rows.Next() {
		var item tieredSQLSmallPayloadItem
		var gen int64
		if err := rows.Scan(&item.key, &gen); err != nil {
			return nil, err
		}
		item.generation = uint64(gen)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *tieredSQLStore) checkSmallPayload(ctx context.Context, entry tieredSQLIndexEntry) (bool, bool, error) {
	var data []byte
	var size int64
	var checksum []byte
	err := s.db.QueryRowContext(ctx, "SELECT data, size, checksum FROM tiered_object_blob WHERE volume_id = ? AND object_key = ? AND generation = ?", s.volumeID, entry.key, int64(entry.generation)).Scan(&data, &size, &checksum)
	if errors.Is(err, sql.ErrNoRows) {
		return true, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if size != int64(len(data)) || size != entry.size || !bytes.Equal(checksum, entry.checksum) || !bytes.Equal(tieredChecksum(data), entry.checksum) {
		return false, true, nil
	}
	return false, false, nil
}

func (s *tieredSQLStore) insertActiveCleanupForTest(ctx context.Context, key string, generation uint64, tier string) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO tiered_object_gc_queue(volume_id, object_key, generation, tier, payload_ref, reason, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)", s.volumeID, []byte(key), int64(generation), tier, generationRef(generation), "test", s.now())
	return err
}

func (s *tieredSQLStore) insertCleanupForTest(ctx context.Context, key string, generation uint64, tier string, payloadRef []byte) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO tiered_object_gc_queue(volume_id, object_key, generation, tier, payload_ref, reason, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)", s.volumeID, []byte(key), int64(generation), tier, payloadRef, "test", s.now())
	return err
}

func (s *tieredSQLStore) createSchema(ctx context.Context) error {
	for _, stmt := range tieredSQLSchema(s.dialect) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *tieredSQLStore) verifyVolumeConfig(ctx context.Context) error {
	values := map[string][]byte{
		"schema_version":  []byte(tieredSQLSchemaVersion),
		"small_threshold": []byte(fmt.Sprintf("%d", s.smallThreshold)),
		"checksum":        []byte(tieredSQLChecksum),
	}
	for key, value := range values {
		if err := s.verifyMeta(ctx, key, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *tieredSQLStore) verifyMeta(ctx context.Context, key string, value []byte) error {
	var existing []byte
	err := s.db.QueryRowContext(ctx, "SELECT meta_value FROM tiered_object_meta WHERE volume_id = ? AND meta_key = ?", s.volumeID, []byte(key)).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = s.db.ExecContext(ctx, "INSERT INTO tiered_object_meta(volume_id, meta_key, meta_value, updated_at) VALUES(?, ?, ?, ?)", s.volumeID, []byte(key), value, s.now())
		return err
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, value) {
		return fmt.Errorf("%w: %s", errTieredSQLConfigMismatch, key)
	}
	return nil
}

func (s *tieredSQLStore) withRetry(ctx context.Context, fn func(*sql.Tx) error) error {
	var last error
	for i := 0; i < 8; i++ {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			err = tx.Commit()
			if err == nil {
				if faultErr := s.injectFault(tieredSQLFaultAfterIndexCommit); faultErr != nil {
					return faultErr
				}
				return nil
			}
		}
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return rbErr
		}
		if errors.Is(err, errTieredSQLRetry) || isTieredSQLConflict(err) {
			last = err
			continue
		}
		return err
	}
	return fmt.Errorf("tiered sql transaction retry exhausted: %w", last)
}

func (s *tieredSQLStore) nextGeneration(ctx context.Context, tx *sql.Tx) (uint64, error) {
	res, err := tx.ExecContext(ctx, "INSERT INTO tiered_object_generation(volume_id, created_at) VALUES(?, ?)", s.volumeID, s.now())
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if id <= 0 {
		return 0, errors.New("invalid generation id")
	}
	return uint64(id), nil
}

func (s *tieredSQLStore) reserveGeneration(ctx context.Context) (uint64, error) {
	var generation uint64
	err := s.withRetry(ctx, func(tx *sql.Tx) error {
		var err error
		generation, err = s.nextGeneration(ctx, tx)
		return err
	})
	return generation, err
}

func (s *tieredSQLStore) getIndexForUpdate(ctx context.Context, tx *sql.Tx, key []byte) (tieredSQLIndexEntry, bool, error) {
	var entry tieredSQLIndexEntry
	row := tx.QueryRowContext(ctx, "SELECT object_key, generation, tier, size, checksum, state, payload_ref, updated_at FROM tiered_object_index WHERE volume_id = ? AND object_key = ?"+s.selectForUpdate, s.volumeID, key)
	err := scanTieredIndex(row, &entry)
	if errors.Is(err, sql.ErrNoRows) {
		return tieredSQLIndexEntry{}, false, nil
	}
	if err != nil {
		return tieredSQLIndexEntry{}, false, err
	}
	return entry, true, nil
}

func (s *tieredSQLStore) insertSmallBlob(ctx context.Context, tx *sql.Tx, key []byte, generation uint64, data, checksum []byte) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO tiered_object_blob(volume_id, object_key, generation, data, size, checksum, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)", s.volumeID, key, int64(generation), data, int64(len(data)), checksum, s.now())
	return err
}

func (s *tieredSQLStore) commitIndex(ctx context.Context, tx *sql.Tx, old tieredSQLIndexEntry, hadOld bool, entry tieredSQLIndexEntry) error {
	if hadOld {
		res, err := tx.ExecContext(ctx, "UPDATE tiered_object_index SET generation = ?, tier = ?, size = ?, checksum = ?, state = ?, payload_ref = ?, updated_at = ? WHERE volume_id = ? AND object_key = ? AND generation = ?", int64(entry.generation), entry.tier, entry.size, entry.checksum, entry.state, entry.payloadRef, entry.updatedAt, s.volumeID, entry.key, int64(old.generation))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return errTieredSQLRetry
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO tiered_object_index(volume_id, object_key, generation, tier, size, checksum, state, payload_ref, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", s.volumeID, entry.key, int64(entry.generation), entry.tier, entry.size, entry.checksum, entry.state, entry.payloadRef, entry.updatedAt)
	if isTieredSQLConflict(err) {
		return errTieredSQLRetry
	}
	return err
}

func (s *tieredSQLStore) enqueueCleanup(ctx context.Context, tx *sql.Tx, entry tieredSQLIndexEntry, reason string) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM tiered_object_gc_queue WHERE volume_id = ? AND object_key = ? AND generation = ? AND tier = ?", s.volumeID, entry.key, int64(entry.generation), entry.tier); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO tiered_object_gc_queue(volume_id, object_key, generation, tier, payload_ref, reason, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)", s.volumeID, entry.key, int64(entry.generation), entry.tier, entry.payloadRef, reason, s.now())
	return err
}

func (s *tieredSQLStore) deleteCleanupItem(ctx context.Context, tx *sql.Tx, item tieredSQLCleanupItem) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM tiered_object_gc_queue WHERE volume_id = ? AND object_key = ? AND generation = ? AND tier = ?", s.volumeID, item.key, int64(item.generation), item.tier)
	return err
}

func (s *tieredSQLStore) deleteCleanupQueueItem(ctx context.Context, item tieredSQLCleanupItem) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM tiered_object_gc_queue WHERE volume_id = ? AND object_key = ? AND generation = ? AND tier = ?", s.volumeID, item.key, int64(item.generation), item.tier)
	return err
}

func (s *tieredSQLStore) readSmallPayload(ctx context.Context, entry tieredSQLIndexEntry) ([]byte, error) {
	var data []byte
	var size int64
	var checksum []byte
	err := s.db.QueryRowContext(ctx, "SELECT data, size, checksum FROM tiered_object_blob WHERE volume_id = ? AND object_key = ? AND generation = ?", s.volumeID, entry.key, int64(entry.generation)).Scan(&data, &size, &checksum)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: indexed small payload missing", errTieredSQLCorruption)
	}
	if err != nil {
		return nil, err
	}
	if size != int64(len(data)) || size != entry.size || !bytes.Equal(checksum, entry.checksum) || !bytes.Equal(tieredChecksum(data), entry.checksum) {
		return nil, fmt.Errorf("%w: indexed small payload checksum or size mismatch", errTieredSQLCorruption)
	}
	return data, nil
}

type tieredIndexScanner interface {
	Scan(dest ...interface{}) error
}

func scanTieredIndex(scanner tieredIndexScanner, entry *tieredSQLIndexEntry) error {
	var gen int64
	if err := scanner.Scan(&entry.key, &gen, &entry.tier, &entry.size, &entry.checksum, &entry.state, &entry.payloadRef, &entry.updatedAt); err != nil {
		return err
	}
	entry.generation = uint64(gen)
	return nil
}

func generationRef(generation uint64) []byte {
	return []byte(fmt.Sprintf("%d", generation))
}

func tieredChecksum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func isTieredSQLConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "constraint failed")
}

func (s *tieredSQLStore) injectFault(point tieredSQLFault) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(point)
}

func tieredSQLSchema(dialect tieredSQLDialect) []string {
	if dialect == tieredDialectSQLite {
		return []string{
			`CREATE TABLE IF NOT EXISTS tiered_object_index (
				volume_id BLOB NOT NULL,
				object_key BLOB NOT NULL,
				generation INTEGER NOT NULL,
				tier TEXT NOT NULL,
				size INTEGER NOT NULL,
				checksum BLOB NOT NULL,
				state TEXT NOT NULL,
				payload_ref BLOB NOT NULL,
				updated_at TIMESTAMP NOT NULL,
				PRIMARY KEY (volume_id, object_key)
			)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS tiered_object_index_generation ON tiered_object_index(volume_id, object_key, generation)`,
			`CREATE TABLE IF NOT EXISTS tiered_object_blob (
				volume_id BLOB NOT NULL,
				object_key BLOB NOT NULL,
				generation INTEGER NOT NULL,
				data BLOB NOT NULL,
				size INTEGER NOT NULL,
				checksum BLOB NOT NULL,
				created_at TIMESTAMP NOT NULL,
				PRIMARY KEY (volume_id, object_key, generation)
			)`,
			`CREATE TABLE IF NOT EXISTS tiered_object_gc_queue (
				volume_id BLOB NOT NULL,
				object_key BLOB NOT NULL,
				generation INTEGER NOT NULL,
				tier TEXT NOT NULL,
				payload_ref BLOB NOT NULL,
				reason TEXT NOT NULL,
				created_at TIMESTAMP NOT NULL,
				PRIMARY KEY (volume_id, object_key, generation, tier)
			)`,
			`CREATE TABLE IF NOT EXISTS tiered_object_meta (
				volume_id BLOB NOT NULL,
				meta_key BLOB NOT NULL,
				meta_value BLOB NOT NULL,
				updated_at TIMESTAMP NOT NULL,
				PRIMARY KEY (volume_id, meta_key)
			)`,
			`CREATE TABLE IF NOT EXISTS tiered_object_generation (
				generation INTEGER PRIMARY KEY AUTOINCREMENT,
				volume_id BLOB NOT NULL,
				created_at TIMESTAMP NOT NULL
			)`,
		}
	}
	return []string{
		`CREATE TABLE IF NOT EXISTS tiered_object_index (
			volume_id VARBINARY(255) NOT NULL,
			object_key VARBINARY(1024) NOT NULL,
			generation BIGINT UNSIGNED NOT NULL,
			tier VARCHAR(16) NOT NULL,
			size BIGINT NOT NULL,
			checksum VARBINARY(64) NOT NULL,
			state VARCHAR(16) NOT NULL,
			payload_ref VARBINARY(1024) NOT NULL,
			updated_at DATETIME(6) NOT NULL,
			PRIMARY KEY (volume_id, object_key),
			UNIQUE KEY tiered_object_index_generation (volume_id, object_key, generation)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS tiered_object_blob (
			volume_id VARBINARY(255) NOT NULL,
			object_key VARBINARY(1024) NOT NULL,
			generation BIGINT UNSIGNED NOT NULL,
			data LONGBLOB NOT NULL,
			size BIGINT NOT NULL,
			checksum VARBINARY(64) NOT NULL,
			created_at DATETIME(6) NOT NULL,
			PRIMARY KEY (volume_id, object_key, generation)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS tiered_object_gc_queue (
			volume_id VARBINARY(255) NOT NULL,
			object_key VARBINARY(1024) NOT NULL,
			generation BIGINT UNSIGNED NOT NULL,
			tier VARCHAR(16) NOT NULL,
			payload_ref VARBINARY(1024) NOT NULL,
			reason VARCHAR(32) NOT NULL,
			created_at DATETIME(6) NOT NULL,
			PRIMARY KEY (volume_id, object_key, generation, tier)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS tiered_object_meta (
			volume_id VARBINARY(255) NOT NULL,
			meta_key VARBINARY(64) NOT NULL,
			meta_value VARBINARY(255) NOT NULL,
			updated_at DATETIME(6) NOT NULL,
			PRIMARY KEY (volume_id, meta_key)
		) ENGINE=InnoDB`,
		`CREATE TABLE IF NOT EXISTS tiered_object_generation (
			generation BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			volume_id VARBINARY(255) NOT NULL,
			created_at DATETIME(6) NOT NULL,
			PRIMARY KEY (generation)
		) ENGINE=InnoDB`,
	}
}
