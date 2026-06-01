//go:build !nosqlite
// +build !nosqlite

package object

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func newTieredConfigTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "tiered-config.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newTieredConfigTestLarge(t *testing.T) ObjectStorage {
	t.Helper()
	large, err := newMem("tiered-config-large", "", "", "")
	if err != nil {
		t.Fatalf("new large store: %v", err)
	}
	return large
}

func validTieredConfig() tieredObjectStoreConfig {
	return tieredObjectStoreConfig{
		Enabled:        true,
		SQLDialect:     tieredDialectSQLite,
		VolumeID:       []byte("vol-config"),
		SmallThreshold: 4,
	}
}

func TestTieredObjectStoreConfigDisabledFailsClosed(t *testing.T) {
	db := newTieredConfigTestDB(t)
	cfg := validTieredConfig()
	cfg.Enabled = false

	store, err := newTieredObjectStoreFromConfig(context.Background(), db, cfg, newTieredConfigTestLarge(t))
	if store != nil {
		t.Fatalf("disabled config returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreExperimental) {
		t.Fatalf("disabled config error = %v, want %v", err, errTieredStoreExperimental)
	}

	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'tiered_object_%'").Scan(&count); err != nil {
		t.Fatalf("count tables after disabled config: %v", err)
	}
	if count != 0 {
		t.Fatalf("disabled config created %d tiered tables", count)
	}
}

func TestTieredObjectStoreConfigValidation(t *testing.T) {
	tests := []struct {
		name  string
		db    *sql.DB
		cfg   tieredObjectStoreConfig
		large ObjectStorage
	}{
		{name: "nil db", db: nil, cfg: validTieredConfig(), large: newTieredConfigTestLarge(t)},
		{name: "nil large", db: newTieredConfigTestDB(t), cfg: validTieredConfig(), large: nil},
		{name: "empty volume", db: newTieredConfigTestDB(t), cfg: tieredObjectStoreConfig{Enabled: true, SQLDialect: tieredDialectSQLite, SmallThreshold: 4}, large: newTieredConfigTestLarge(t)},
		{name: "zero threshold", db: newTieredConfigTestDB(t), cfg: tieredObjectStoreConfig{Enabled: true, SQLDialect: tieredDialectSQLite, VolumeID: []byte("vol")}, large: newTieredConfigTestLarge(t)},
		{name: "empty dialect", db: newTieredConfigTestDB(t), cfg: tieredObjectStoreConfig{Enabled: true, VolumeID: []byte("vol"), SmallThreshold: 4}, large: newTieredConfigTestLarge(t)},
		{name: "unknown dialect", db: newTieredConfigTestDB(t), cfg: tieredObjectStoreConfig{Enabled: true, SQLDialect: "postgres", VolumeID: []byte("vol"), SmallThreshold: 4}, large: newTieredConfigTestLarge(t)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := newTieredObjectStoreFromConfig(context.Background(), tt.db, tt.cfg, tt.large)
			if store != nil {
				t.Fatalf("invalid config returned store %+v", store)
			}
			if !errors.Is(err, errTieredStoreInvalidConfig) {
				t.Fatalf("invalid config error = %v, want %v", err, errTieredStoreInvalidConfig)
			}
		})
	}
}

func TestTieredObjectStoreConfigInvalidDialectFailsBeforeSchema(t *testing.T) {
	db := newTieredConfigTestDB(t)
	cfg := validTieredConfig()
	cfg.SQLDialect = ""

	store, err := newTieredObjectStoreFromConfig(context.Background(), db, cfg, newTieredConfigTestLarge(t))
	if store != nil {
		t.Fatalf("invalid dialect returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("invalid dialect error = %v, want %v", err, errTieredStoreInvalidConfig)
	}

	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'tiered_object_%'").Scan(&count); err != nil {
		t.Fatalf("count tables after invalid dialect: %v", err)
	}
	if count != 0 {
		t.Fatalf("invalid dialect created %d tiered tables", count)
	}
}

func TestTieredObjectStoreConfigCreatesUnregisteredStore(t *testing.T) {
	store, err := newTieredObjectStoreFromConfig(context.Background(), newTieredConfigTestDB(t), validTieredConfig(), newTieredConfigTestLarge(t))
	if err != nil {
		t.Fatalf("new tiered store from config: %v", err)
	}
	if store == nil {
		t.Fatal("new tiered store from config returned nil")
	}
	if err := ensureTieredStoreNotRegistered(); err != nil {
		t.Fatalf("tiered store must remain unregistered: %v", err)
	}
	if !strings.Contains(store.String(), "experimental") {
		t.Fatalf("store string = %q, want experimental marker", store.String())
	}
}

func TestTieredObjectStoreConfigMismatchFailsClosed(t *testing.T) {
	db := newTieredConfigTestDB(t)
	cfg := validTieredConfig()
	if _, err := newTieredObjectStoreFromConfig(context.Background(), db, cfg, newTieredConfigTestLarge(t)); err != nil {
		t.Fatalf("initial store from config: %v", err)
	}

	cfg.SmallThreshold = 8
	store, err := newTieredObjectStoreFromConfig(context.Background(), db, cfg, newTieredConfigTestLarge(t))
	if store != nil {
		t.Fatalf("mismatched config returned store %+v", store)
	}
	if !errors.Is(err, errTieredSQLConfigMismatch) {
		t.Fatalf("mismatched config error = %v, want %v", err, errTieredSQLConfigMismatch)
	}
}
