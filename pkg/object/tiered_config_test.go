//go:build !nosqlite
// +build !nosqlite

package object

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const tieredConfigTestSQLDSNEnv = "JFS_TIERED_CONFIG_TEST_SQL_DSN"

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

func tieredConfigTestEndpoint(sqlDSNEnv string, overrides map[string]string) string {
	values := url.Values{}
	values.Set("experimental", "true")
	values.Set("integration", "verified")
	values.Set("sql_dialect", string(tieredDialectSQLite))
	values.Set("sql_dsn_env", sqlDSNEnv)
	values.Set("volume_id", "vol-runtime")
	values.Set("small_threshold", "4")
	values.Set("large_storage", "mem")
	values.Set("large_endpoint", "tiered-runtime-large")
	for key, value := range overrides {
		if value == "" {
			values.Del(key)
			continue
		}
		values.Set(key, value)
	}
	return "tiered://vol-runtime?" + values.Encode()
}

func assertNoTieredSQLiteTables(t *testing.T, dbPath string) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite for table check: %v", err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'tiered_object_%'").Scan(&count); err != nil {
		t.Fatalf("count tiered tables: %v", err)
	}
	if count != 0 {
		t.Fatalf("found %d tiered tables, want none", count)
	}
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

func TestTieredObjectStoreConfigCreatesStore(t *testing.T) {
	store, err := newTieredObjectStoreFromConfig(context.Background(), newTieredConfigTestDB(t), validTieredConfig(), newTieredConfigTestLarge(t))
	if err != nil {
		t.Fatalf("new tiered store from config: %v", err)
	}
	if store == nil {
		t.Fatal("new tiered store from config returned nil")
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

func TestTieredCreateStorageFailsClosedByDefault(t *testing.T) {
	store, err := CreateStorage("tiered", "", "", "", "")
	if store != nil {
		t.Fatalf("default tiered storage returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreExperimental) {
		t.Fatalf("default tiered storage error = %v, want %v", err, errTieredStoreExperimental)
	}
}

func TestTieredCreateStorageRequiresRuntimeGatesBeforeSchema(t *testing.T) {
	sqlDSN := filepath.Join(t.TempDir(), "tiered-runtime.db")
	endpoint := tieredConfigTestEndpoint(tieredConfigTestSQLDSNEnv, nil)

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("tiered storage without env returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreExperimental) {
		t.Fatalf("tiered storage without env error = %v, want %v", err, errTieredStoreExperimental)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)

	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	endpoint = tieredConfigTestEndpoint(tieredConfigTestSQLDSNEnv, map[string]string{"integration": ""})
	store, err = CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("tiered storage without integration marker returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreExperimental) {
		t.Fatalf("tiered storage without integration marker error = %v, want %v", err, errTieredStoreExperimental)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageInvalidConfigFailsBeforeSchema(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	sqlDSN := filepath.Join(t.TempDir(), "tiered-invalid.db")
	t.Setenv(tieredConfigTestSQLDSNEnv, sqlDSN)
	endpoint := tieredConfigTestEndpoint(tieredConfigTestSQLDSNEnv, map[string]string{"small_threshold": "0"})

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("invalid tiered storage returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("invalid tiered storage error = %v, want %v", err, errTieredStoreInvalidConfig)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageRejectsInlineDSNBeforeSchema(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	sqlDSN := filepath.Join(t.TempDir(), "tiered-inline-dsn.db")
	values := url.Values{}
	values.Set("experimental", "true")
	values.Set("integration", "verified")
	values.Set("sql_dialect", string(tieredDialectSQLite))
	values.Set("sql_dsn", "user:secret@tcp(127.0.0.1:4000)/db")
	values.Set("volume_id", "vol-runtime")
	values.Set("small_threshold", "4")
	values.Set("large_storage", "mem")
	values.Set("large_endpoint", "tiered-runtime-large")
	endpoint := "tiered://vol-runtime?" + values.Encode()

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("tiered storage with inline SQL DSN returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("tiered storage with inline SQL DSN error = %v, want %v", err, errTieredStoreInvalidConfig)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageRejectsInlineDSNEvenWithDSNEnvBeforeSchema(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	sqlDSN := filepath.Join(t.TempDir(), "tiered-inline-and-env-dsn.db")
	t.Setenv(tieredConfigTestSQLDSNEnv, sqlDSN)
	values := url.Values{}
	values.Set("experimental", "true")
	values.Set("integration", "verified")
	values.Set("sql_dialect", string(tieredDialectSQLite))
	values.Set("sql_dsn", "user:secret@tcp(127.0.0.1:4000)/db")
	values.Set("sql_dsn_env", tieredConfigTestSQLDSNEnv)
	values.Set("volume_id", "vol-runtime")
	values.Set("small_threshold", "4")
	values.Set("large_storage", "mem")
	values.Set("large_endpoint", "tiered-runtime-large")
	endpoint := "tiered://vol-runtime?" + values.Encode()

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("tiered storage with inline SQL DSN and SQL DSN env returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("tiered storage with inline SQL DSN and SQL DSN env error = %v, want %v", err, errTieredStoreInvalidConfig)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageRejectsSecretLikeDSNEnvNameBeforeSchema(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	sqlDSN := filepath.Join(t.TempDir(), "tiered-secret-like-env.db")
	endpoint := tieredConfigTestEndpoint("user:secret@tcp(127.0.0.1:4000)/db", nil)

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("tiered storage with secret-like SQL DSN env name returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("tiered storage with secret-like SQL DSN env name error = %v, want %v", err, errTieredStoreInvalidConfig)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageRequiresDSNEnvBeforeSchema(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	sqlDSN := filepath.Join(t.TempDir(), "tiered-missing-dsn-env.db")
	endpoint := tieredConfigTestEndpoint(tieredConfigTestSQLDSNEnv, nil)

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("tiered storage without SQL DSN env returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("tiered storage without SQL DSN env error = %v, want %v", err, errTieredStoreInvalidConfig)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageRejectsInvalidSchemeBeforeSchema(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	sqlDSN := filepath.Join(t.TempDir(), "tiered-invalid-scheme.db")
	t.Setenv(tieredConfigTestSQLDSNEnv, sqlDSN)
	values := url.Values{}
	values.Set("experimental", "true")
	values.Set("integration", "verified")
	values.Set("sql_dialect", string(tieredDialectSQLite))
	values.Set("sql_dsn_env", tieredConfigTestSQLDSNEnv)
	values.Set("volume_id", "vol-runtime")
	values.Set("small_threshold", "4")
	values.Set("large_storage", "mem")
	values.Set("large_endpoint", "tiered-runtime-large")
	endpoint := "s3://bucket?" + values.Encode()

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("invalid scheme tiered storage returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("invalid scheme tiered storage error = %v, want %v", err, errTieredStoreInvalidConfig)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageRejectsRecursiveLargeStorageBeforeSchema(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	sqlDSN := filepath.Join(t.TempDir(), "tiered-recursive.db")
	t.Setenv(tieredConfigTestSQLDSNEnv, sqlDSN)
	endpoint := tieredConfigTestEndpoint(tieredConfigTestSQLDSNEnv, map[string]string{"large_storage": "tiered"})

	store, err := CreateStorage("tiered", endpoint, "", "", "")
	if store != nil {
		t.Fatalf("recursive tiered storage returned store %+v", store)
	}
	if !errors.Is(err, errTieredStoreInvalidConfig) {
		t.Fatalf("recursive tiered storage error = %v, want %v", err, errTieredStoreInvalidConfig)
	}
	assertNoTieredSQLiteTables(t, sqlDSN)
}

func TestTieredCreateStorageExplicitExperimentalConfig(t *testing.T) {
	t.Setenv(tieredObjectStoreExperimentalEnv, "true")
	t.Setenv(tieredConfigTestSQLDSNEnv, filepath.Join(t.TempDir(), "tiered-enabled.db"))
	endpoint := tieredConfigTestEndpoint(tieredConfigTestSQLDSNEnv, nil)

	storage, err := CreateStorage("tiered", endpoint, "", "", "")
	if err != nil {
		t.Fatalf("create explicitly enabled tiered storage: %v", err)
	}
	store, ok := storage.(*tieredObjectStore)
	if !ok {
		t.Fatalf("storage type = %T, want *tieredObjectStore", storage)
	}

	if err := store.Put(context.Background(), "small", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small through registered tiered storage: %v", err)
	}
	reader, err := store.Get(context.Background(), "small", 0, -1)
	if err != nil {
		t.Fatalf("get small through registered tiered storage: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read small through registered tiered storage: %v", err)
	}
	if string(data) != "abc" {
		t.Fatalf("registered tiered read = %q, want abc", string(data))
	}
}
