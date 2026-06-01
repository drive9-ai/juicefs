//go:build !nomysql
// +build !nomysql

package object

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func newMySQLTieredSQLStore(t *testing.T, threshold int64) *tieredSQLStore {
	t.Helper()
	addr := os.Getenv("MYSQL_ADDR")
	if addr == "" {
		t.Skip("MYSQL_ADDR not set")
	}
	dsn := mysqlTieredDSN(addr, os.Getenv("MYSQL_USER"), os.Getenv("MYSQL_PASSWORD"))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := newTieredSQLStore(context.Background(), db, tieredSQLConfig{
		Dialect:        tieredDialectMySQL,
		VolumeID:       []byte(fmt.Sprintf("tiered-mysql-%d", time.Now().UnixNano())),
		SmallThreshold: threshold,
	})
	if err != nil {
		t.Fatalf("new mysql tiered sql store: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, table := range []string{"tiered_object_index", "tiered_object_blob", "tiered_object_gc_queue", "tiered_object_meta", "tiered_object_generation"} {
			_, _ = db.ExecContext(ctx, "DELETE FROM "+table+" WHERE volume_id = ?", store.volumeID)
		}
	})
	return store
}

func mysqlTieredDSN(addr, user, password string) string {
	dsn := addr
	if user != "" {
		dsn = user + ":" + password + "@" + addr
	}
	if strings.Contains(dsn, "parseTime=") {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&parseTime=true"
	}
	return dsn + "?parseTime=true"
}

func TestTieredSQLMySQLIntegrationOrderingOverwriteAndRetry(t *testing.T) {
	store := newMySQLTieredSQLStore(t, 4)
	ctx := context.Background()

	keys := []string{"p/b", "p/a", "p/a\x00x", "p/a\xff"}
	for _, key := range keys {
		if _, err := store.putSmall(ctx, key, []byte("x")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	entries, hasMore, token, err := store.list(ctx, "p/", "", "", "", 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if hasMore || token != "" {
		t.Fatalf("list page = hasMore %v token %q", hasMore, token)
	}
	if got := tieredMySQLEntryKeys(entries); strings.Join(got, "|") != "p/a|p/a\x00x|p/a\xff|p/b" {
		t.Fatalf("mysql binary ordering keys = %#v", got)
	}

	smallGeneration, err := store.putSmall(ctx, "k", []byte("abc"))
	if err != nil {
		t.Fatalf("put small: %v", err)
	}
	if _, err := store.commitLargeRef(ctx, "k", 10, []byte("large-checksum"), []byte("objects/mysql/large")); err != nil {
		t.Fatalf("small->large: %v", err)
	}
	if _, err := store.putSmall(ctx, "k", []byte("xy")); err != nil {
		t.Fatalf("large->small: %v", err)
	}
	if got := readTieredSQLMySQL(t, store, "k"); got != "xy" {
		t.Fatalf("got %q", got)
	}
	if exists, err := store.smallBlobExists(ctx, "k", smallGeneration); err != nil || !exists {
		t.Fatalf("old small generation should await cleanup = %v, %v", exists, err)
	}

	values := []string{"one", "two", "three", "four"}
	var wg sync.WaitGroup
	for _, value := range values {
		value := value
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.putSmall(ctx, "race", []byte(value)); err != nil {
				t.Errorf("put race %q: %v", value, err)
			}
		}()
	}
	wg.Wait()
	got := readTieredSQLMySQL(t, store, "race")
	valid := false
	for _, value := range values {
		if got == value {
			valid = true
			break
		}
	}
	if !valid {
		t.Fatalf("got partial or unknown race value %q", got)
	}
}

func readTieredSQLMySQL(t *testing.T, store *tieredSQLStore, key string) string {
	t.Helper()
	r, err := store.getSmall(context.Background(), key, 0, -1)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return string(data)
}

func tieredMySQLEntryKeys(entries []tieredSQLIndexEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, string(entry.key))
	}
	return keys
}
