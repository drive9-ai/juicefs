package object

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var errTieredStoreInvalidConfig = errors.New("invalid tiered object store config")

type tieredObjectStoreConfig struct {
	Enabled        bool
	SQLDialect     tieredSQLDialect
	VolumeID       []byte
	SmallThreshold int64
}

func newTieredObjectStoreFromConfig(ctx context.Context, db *sql.DB, cfg tieredObjectStoreConfig, large ObjectStorage) (*tieredObjectStore, error) {
	if !cfg.Enabled {
		return nil, errTieredStoreExperimental
	}
	if db == nil {
		return nil, fmt.Errorf("%w: nil SQL DB", errTieredStoreInvalidConfig)
	}
	if large == nil {
		return nil, fmt.Errorf("%w: nil large object store", errTieredStoreInvalidConfig)
	}
	if len(cfg.VolumeID) == 0 {
		return nil, fmt.Errorf("%w: empty volume ID", errTieredStoreInvalidConfig)
	}
	if cfg.SmallThreshold <= 0 {
		return nil, fmt.Errorf("%w: small threshold must be positive", errTieredStoreInvalidConfig)
	}
	if !isTieredObjectStoreDialectSupported(cfg.SQLDialect) {
		return nil, fmt.Errorf("%w: unsupported SQL dialect %q", errTieredStoreInvalidConfig, cfg.SQLDialect)
	}

	index, err := newTieredSQLStore(ctx, db, tieredSQLConfig{
		Dialect:        cfg.SQLDialect,
		VolumeID:       append([]byte(nil), cfg.VolumeID...),
		SmallThreshold: cfg.SmallThreshold,
	})
	if err != nil {
		return nil, err
	}
	return newTieredObjectStore(index, large)
}

func isTieredObjectStoreDialectSupported(dialect tieredSQLDialect) bool {
	switch dialect {
	case tieredDialectMySQL, tieredDialectSQLite:
		return true
	default:
		return false
	}
}
