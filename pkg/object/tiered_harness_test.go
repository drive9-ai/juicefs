package object

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type harnessTier string

const (
	harnessTierSmall harnessTier = "tidb"
	harnessTierLarge harnessTier = "s3"
)

type harnessPayload struct {
	data      []byte
	createdAt time.Time
}

type harnessIndexEntry struct {
	key        string
	generation uint64
	tier       harnessTier
	size       int64
	createdAt  time.Time
}

type harnessObject struct {
	obj
}

type harnessFault string

const (
	faultAfterSmallBlobWrite  harnessFault = "after-small-blob-write"
	faultAfterLargePayloadPut harnessFault = "after-large-payload-put"
	faultAfterIndexCommit     harnessFault = "after-index-commit"
	faultAfterDeleteIndex     harnessFault = "after-delete-index"
)

type tieredHarnessStore struct {
	DefaultObjectStorage
	mu             sync.Mutex
	threshold      int64
	nextGeneration uint64
	index          map[string]harnessIndexEntry
	smallPayloads  map[uint64]harnessPayload
	largePayloads  map[uint64]harnessPayload
	faults         map[harnessFault]error
}

func newTieredHarnessStore(threshold int64) *tieredHarnessStore {
	return &tieredHarnessStore{
		threshold:     threshold,
		index:         make(map[string]harnessIndexEntry),
		smallPayloads: make(map[uint64]harnessPayload),
		largePayloads: make(map[uint64]harnessPayload),
		faults:        make(map[harnessFault]error),
	}
}

func (s *tieredHarnessStore) String() string { return "tiered-harness" }

func (s *tieredHarnessStore) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	generation := s.nextGeneration + 1
	s.nextGeneration = generation
	now := time.Now()
	tier := harnessTierSmall
	payloads := s.smallPayloads
	if int64(len(data)) > s.threshold {
		tier = harnessTierLarge
		payloads = s.largePayloads
	}
	payloads[generation] = harnessPayload{data: append([]byte(nil), data...), createdAt: now}

	if tier == harnessTierSmall {
		if err := s.faults[faultAfterSmallBlobWrite]; err != nil {
			return err
		}
	} else {
		if err := s.faults[faultAfterLargePayloadPut]; err != nil {
			return err
		}
	}

	old, hadOld := s.index[key]
	s.index[key] = harnessIndexEntry{key: key, generation: generation, tier: tier, size: int64(len(data)), createdAt: now}
	if err := s.faults[faultAfterIndexCommit]; err != nil {
		return err
	}
	if hadOld {
		s.deletePayloadLocked(old)
	}
	return nil
}

func (s *tieredHarnessStore) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.index[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	payload, ok := s.payloadLocked(entry)
	if !ok {
		return nil, errors.New("indexed payload missing")
	}
	data := payload.data
	if off > int64(len(data)) {
		off = int64(len(data))
	}
	data = data[off:]
	if limit >= 0 && limit < int64(len(data)) {
		data = data[:limit]
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), nil
}

func (s *tieredHarnessStore) Head(ctx context.Context, key string) (Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.index[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if _, ok := s.payloadLocked(entry); !ok {
		return nil, errors.New("indexed payload missing")
	}
	return &harnessObject{obj{key: key, size: entry.size, mtime: entry.createdAt, isDir: strings.HasSuffix(key, "/")}}, nil
}

func (s *tieredHarnessStore) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.index[key]
	if !ok {
		return nil
	}
	delete(s.index, key)
	if err := s.faults[faultAfterDeleteIndex]; err != nil {
		return err
	}
	s.deletePayloadLocked(entry)
	return nil
}

func (s *tieredHarnessStore) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	if token != "" {
		startAfter = token
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.index))
	seen := make(map[string]struct{})
	for key := range s.index {
		if !strings.HasPrefix(key, prefix) || key <= startAfter {
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
		if _, ok := seen[listKey]; ok {
			continue
		}
		seen[listKey] = struct{}{}
		keys = append(keys, listKey)
	}
	sort.Strings(keys)
	hasMore := false
	nextToken := ""
	if limit > 0 && int64(len(keys)) > limit {
		hasMore = true
		nextToken = keys[limit-1]
		keys = keys[:limit]
	}
	objects := make([]Object, 0, len(keys))
	for _, key := range keys {
		if entry, ok := s.index[key]; ok {
			objects = append(objects, &harnessObject{obj{key: key, size: entry.size, mtime: entry.createdAt, isDir: strings.HasSuffix(key, "/")}})
			continue
		}
		objects = append(objects, &harnessObject{obj{key: key, mtime: time.Now(), isDir: strings.HasSuffix(key, delimiter)}})
	}
	return objects, hasMore, nextToken, nil
}

func (s *tieredHarnessStore) Copy(ctx context.Context, dst, src string) error {
	return notSupported
}

func (s *tieredHarnessStore) payloadLocked(entry harnessIndexEntry) (harnessPayload, bool) {
	if entry.tier == harnessTierSmall {
		payload, ok := s.smallPayloads[entry.generation]
		return payload, ok
	}
	payload, ok := s.largePayloads[entry.generation]
	return payload, ok
}

func (s *tieredHarnessStore) deletePayloadLocked(entry harnessIndexEntry) {
	if entry.tier == harnessTierSmall {
		delete(s.smallPayloads, entry.generation)
		return
	}
	delete(s.largePayloads, entry.generation)
}

func (s *tieredHarnessStore) activeGeneration(key string) (uint64, harnessTier, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.index[key]
	return entry.generation, entry.tier, ok
}

func (s *tieredHarnessStore) payloadCounts() (small, large int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.smallPayloads), len(s.largePayloads)
}

func (s *tieredHarnessStore) deleteActivePayloadForTest(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.index[key]
	if !ok {
		return
	}
	s.deletePayloadLocked(entry)
}

func (s *tieredHarnessStore) setFault(point harnessFault, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.faults, point)
		return
	}
	s.faults[point] = err
}

func readHarnessObject(t *testing.T, store ObjectStorage, key string) string {
	t.Helper()
	r, err := store.Get(context.Background(), key, 0, -1)
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

func TestTieredHarnessIndexVisibility(t *testing.T) {
	store := newTieredHarnessStore(4)
	if err := store.Put(context.Background(), "k", strings.NewReader("abc")); err != nil {
		t.Fatalf("put small: %v", err)
	}
	if got := readHarnessObject(t, store, "k"); got != "abc" {
		t.Fatalf("got %q", got)
	}
	generation, tier, ok := store.activeGeneration("k")
	if !ok || generation != 1 || tier != harnessTierSmall {
		t.Fatalf("active = (%d, %s, %v)", generation, tier, ok)
	}
	if small, large := store.payloadCounts(); small != 1 || large != 0 {
		t.Fatalf("payload counts = (%d, %d)", small, large)
	}
}

func TestTieredHarnessGenerationOverwriteAcrossTiers(t *testing.T) {
	store := newTieredHarnessStore(4)
	ctx := context.Background()
	cases := []struct {
		name string
		data string
		tier harnessTier
	}{
		{"small", "abc", harnessTierSmall},
		{"large", "abcde", harnessTierLarge},
		{"small-again", "xy", harnessTierSmall},
		{"large-again", "123456", harnessTierLarge},
	}
	for i, tc := range cases {
		if err := store.Put(ctx, "k", strings.NewReader(tc.data)); err != nil {
			t.Fatalf("%s put: %v", tc.name, err)
		}
		if got := readHarnessObject(t, store, "k"); got != tc.data {
			t.Fatalf("%s got %q", tc.name, got)
		}
		generation, tier, ok := store.activeGeneration("k")
		if !ok || generation != uint64(i+1) || tier != tc.tier {
			t.Fatalf("%s active = (%d, %s, %v)", tc.name, generation, tier, ok)
		}
		if small, large := store.payloadCounts(); small+large != 1 {
			t.Fatalf("%s left orphan payloads small=%d large=%d", tc.name, small, large)
		}
	}
}

func TestTieredHarnessCrashAfterIndexCommitKeepsNewGenerationVisible(t *testing.T) {
	store := newTieredHarnessStore(4)
	ctx := context.Background()
	if err := store.Put(ctx, "k", strings.NewReader("old")); err != nil {
		t.Fatalf("put old: %v", err)
	}
	store.setFault(faultAfterIndexCommit, errors.New("crash after index commit"))
	if err := store.Put(ctx, "k", strings.NewReader("new-large")); err == nil {
		t.Fatal("expected injected put error")
	}
	if got := readHarnessObject(t, store, "k"); got != "new-large" {
		t.Fatalf("got %q", got)
	}
	generation, tier, ok := store.activeGeneration("k")
	if !ok || generation != 2 || tier != harnessTierLarge {
		t.Fatalf("active = (%d, %s, %v)", generation, tier, ok)
	}
	if small, large := store.payloadCounts(); small != 1 || large != 1 {
		t.Fatalf("old payload should be orphaned for GC, small=%d large=%d", small, large)
	}
}

func TestTieredHarnessConcurrentPutPublishesOneCompleteGeneration(t *testing.T) {
	store := newTieredHarnessStore(4)
	ctx := context.Background()
	values := []string{"aaa", "bbbbbbbb", "cc", "dddddddddd"}
	var wg sync.WaitGroup
	for _, value := range values {
		value := value
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.Put(ctx, "k", strings.NewReader(value)); err != nil {
				t.Errorf("put %q: %v", value, err)
			}
		}()
	}
	wg.Wait()

	got := readHarnessObject(t, store, "k")
	valid := false
	for _, value := range values {
		if got == value {
			valid = true
			break
		}
	}
	if !valid {
		t.Fatalf("got partial or unknown value %q", got)
	}
	if small, large := store.payloadCounts(); small+large != 1 {
		t.Fatalf("expected only active payload after successful overwrites, small=%d large=%d", small, large)
	}
}

func TestTieredHarnessDeleteVisibility(t *testing.T) {
	store := newTieredHarnessStore(4)
	if err := store.Put(context.Background(), "k", strings.NewReader("abcde")); err != nil {
		t.Fatalf("put: %v", err)
	}
	store.setFault(faultAfterDeleteIndex, errors.New("crash after delete index"))
	if err := store.Delete(context.Background(), "k"); err == nil {
		t.Fatal("expected injected delete error")
	}
	if _, err := store.Head(context.Background(), "k"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("head after delete index = %v", err)
	}
	if small, large := store.payloadCounts(); small != 0 || large != 1 {
		t.Fatalf("payload should be orphaned for GC, small=%d large=%d", small, large)
	}
}

func TestTieredHarnessListUsesBinaryObjectKeyOrdering(t *testing.T) {
	store := newTieredHarnessStore(4)
	ctx := context.Background()
	keys := []string{"p/b", "p/a", "p/a\x00x", "p/a\xff", "q/a"}
	for _, key := range keys {
		if err := store.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	objs, _, _, err := store.List(ctx, "p/", "", "", "", 100, false)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	got := make([]string, 0, len(objs))
	for _, obj := range objs {
		got = append(got, obj.Key())
	}
	want := []string{"p/a", "p/a\x00x", "p/a\xff", "p/b"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("keys = %#v, want %#v", got, want)
	}
}

func TestTieredHarnessListSupportsTokenAndDelimiter(t *testing.T) {
	store := newTieredHarnessStore(4)
	ctx := context.Background()
	keys := []string{"p/a/1", "p/a/2", "p/b/1", "p/c"}
	for _, key := range keys {
		if err := store.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	first, hasMore, token, err := store.List(ctx, "p/", "", "", "/", 2, false)
	if err != nil {
		t.Fatalf("list first: %v", err)
	}
	if !hasMore || token != "p/b/" {
		t.Fatalf("page = hasMore %v token %q", hasMore, token)
	}
	if got := objectKeys(first); strings.Join(got, "|") != "p/a/|p/b/" {
		t.Fatalf("first keys = %#v", got)
	}
	second, hasMore, token, err := store.List(ctx, "p/", "", token, "/", 2, false)
	if err != nil {
		t.Fatalf("list second: %v", err)
	}
	if hasMore || token != "" {
		t.Fatalf("second page = hasMore %v token %q", hasMore, token)
	}
	if got := objectKeys(second); strings.Join(got, "|") != "p/c" {
		t.Fatalf("second keys = %#v", got)
	}
}

func TestTieredHarnessCrashAfterSmallPayloadBeforeIndexCommit(t *testing.T) {
	store := newTieredHarnessStore(4)
	store.setFault(faultAfterSmallBlobWrite, errors.New("crash before small index"))
	if err := store.Put(context.Background(), "k", strings.NewReader("abc")); err == nil {
		t.Fatal("expected injected put error")
	}
	if _, err := store.Head(context.Background(), "k"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("head after failed put = %v", err)
	}
	if small, large := store.payloadCounts(); small != 1 || large != 0 {
		t.Fatalf("payload should be invisible orphan for GC, small=%d large=%d", small, large)
	}
}

func TestTieredHarnessHeadDetectsMissingIndexedPayload(t *testing.T) {
	store := newTieredHarnessStore(4)
	if err := store.Put(context.Background(), "k", strings.NewReader("abc")); err != nil {
		t.Fatalf("put: %v", err)
	}
	store.deleteActivePayloadForTest("k")
	if _, err := store.Head(context.Background(), "k"); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("head missing indexed payload = %v, want corruption", err)
	}
}

func objectKeys(objects []Object) []string {
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.Key())
	}
	return keys
}

func TestTieredHarnessCopyUnsupported(t *testing.T) {
	store := newTieredHarnessStore(4)
	if err := store.Copy(context.Background(), "dst", "src"); !errors.Is(err, notSupported) {
		t.Fatalf("copy error = %v, want %v", err, notSupported)
	}
}

func TestTieredHarnessCrashBeforeIndexCommitLeavesPayloadInvisible(t *testing.T) {
	store := newTieredHarnessStore(4)
	store.setFault(faultAfterLargePayloadPut, errors.New("crash before index"))
	if err := store.Put(context.Background(), "k", strings.NewReader("abcde")); err == nil {
		t.Fatal("expected injected put error")
	}
	if _, err := store.Head(context.Background(), "k"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("head after failed put = %v", err)
	}
	if small, large := store.payloadCounts(); small != 0 || large != 1 {
		t.Fatalf("payload should be invisible orphan for GC, small=%d large=%d", small, large)
	}
}
