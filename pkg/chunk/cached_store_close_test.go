/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package chunk

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davies/groupcache/consistenthash"
	"github.com/juicedata/juicefs/pkg/compress"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/twmb/murmur3"
)

func TestCachedStoreCloseReleasesMemoryCachePages(t *testing.T) {
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(blob, Config{
		CacheDir:    "memory",
		CacheSize:   1 << 20,
		BlockSize:   1 << 20,
		Compress:    "none",
		MaxUpload:   1,
		MaxDownload: 1,
		BufferSize:  32 << 20,
	}, nil).(*cachedStore)

	p := NewOffPage(4096)
	store.bcache.cache("chunks/0/0/1_0_4096", p, true, false)
	p.Release()
	if used := store.UsedMemory(); used == 0 {
		t.Fatal("memory cache did not retain the test page")
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	if used := store.UsedMemory(); used != 0 {
		t.Fatalf("used memory after close = %d, want 0", used)
	}
}

func TestCachedStoreTryAcquireUploadSlotAfterClose(t *testing.T) {
	store := &cachedStore{
		currentUpload: make(chan struct{}, 1),
		done:          make(chan struct{}),
	}
	store.closed.Store(true)
	close(store.done)

	for i := 0; i < 100; i++ {
		if store.tryAcquireUploadSlot() {
			t.Fatal("tryAcquireUploadSlot acquired a slot after close")
		}
	}
	if got := len(store.currentUpload); got != 0 {
		t.Fatalf("upload slots after close = %d, want 0", got)
	}
}

type blockingPutStorage struct {
	object.ObjectStorage
	entered     chan struct{}
	release     chan struct{}
	deleteCount atomic.Int64
}

func (s *blockingPutStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	select {
	case <-s.entered:
	default:
		close(s.entered)
	}
	select {
	case <-s.release:
		return s.ObjectStorage.Put(ctx, key, in, getters...)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingPutStorage) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	s.deleteCount.Add(1)
	return s.ObjectStorage.Delete(ctx, key, getters...)
}

type closeTestCacheManager struct {
	uploadedCount    atomic.Int64
	removeStageCount atomic.Int64
}

func (m *closeTestCacheManager) cache(key string, p *Page, force, dropCache bool) {}

func (m *closeTestCacheManager) remove(key string, staging bool) {}

func (m *closeTestCacheManager) load(key string) (ReadCloser, error) {
	return nil, errNotCached
}

func (m *closeTestCacheManager) exist(key string) (string, bool) {
	return "", false
}

func (m *closeTestCacheManager) uploaded(key string, size int) {
	m.uploadedCount.Add(1)
}

func (m *closeTestCacheManager) stage(key string, data []byte, tierID uint8) (string, error) {
	return "", nil
}

func (m *closeTestCacheManager) removeStage(key string) error {
	m.removeStageCount.Add(1)
	return nil
}

func (m *closeTestCacheManager) stats() (int64, int64) {
	return 0, 0
}

func (m *closeTestCacheManager) usedMemory() int64 {
	return 0
}

func (m *closeTestCacheManager) isEmpty() bool {
	return false
}

func (m *closeTestCacheManager) getMetrics() *cacheManagerMetrics {
	return nil
}

func (m *closeTestCacheManager) close() {}

func newCloseTestCacheStore(id string) *cacheStore {
	s := &cacheStore{
		id:      id,
		dir:     id,
		pending: make(chan pendingFile, 1),
		pages:   make(map[string]*Page),
		done:    make(chan struct{}),
		opTs:    make(map[time.Duration]func() error),
	}
	s.state = newDCState(dcUnchanged, s)
	return s
}

func newCloseTestWritableCacheStore(t *testing.T, id string, pending int) *cacheStore {
	t.Helper()
	keys, err := NewKeyIndex(&Config{CacheEviction: Eviction2Random})
	if err != nil {
		t.Fatalf("new key index: %s", err)
	}
	s := newCloseTestCacheStore(id)
	s.capacity = 1 << 20
	s.pending = make(chan pendingFile, pending)
	s.keys = keys
	s.m = newCacheManagerMetrics(nil)
	return s
}

func newCloseTestDiskCacheManager(stores ...*cacheStore) *cacheManager {
	m := &cacheManager{
		consistentMap: consistenthash.New(100, murmur3.Sum32),
		storeMap:      make(map[string]*cacheStore, len(stores)),
		stores:        make([]*cacheStore, len(stores)),
		allStores:     make([]*cacheStore, 0, len(stores)),
		done:          make(chan struct{}),
	}
	for i, s := range stores {
		m.consistentMap.Add(s.id)
		m.storeMap[s.id] = s
		m.stores[i] = s
		m.allStores = append(m.allStores, s)
	}
	return m
}

func assertClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	default:
		t.Fatalf("%s is still open", name)
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("%s is still open", name)
	}
}

func waitForCachePage(t *testing.T, cache *cacheStore, key string) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		cache.Lock()
		_, ok := cache.pages[key]
		cache.Unlock()
		if ok {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("cache page %s was not inserted", key)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestCacheStoreForceCacheDoesNotDoubleReleasePageRemovedDuringClose(t *testing.T) {
	cache := newCloseTestWritableCacheStore(t, "cache", 0)
	key := "chunks/0/0/1_0_5"
	page := NewPage([]byte("hello"))

	done := make(chan struct{})
	go func() {
		cache.cache(key, page, true, false)
		close(done)
	}()
	waitForCachePage(t, cache, key)

	cache.Lock()
	cached := cache.pages[key]
	if cached != page {
		cache.Unlock()
		t.Fatalf("cached page = %p, want %p", cached, page)
	}
	close(cache.done)
	delete(cache.pages, key)
	atomic.AddInt64(&cache.totalPages, -int64(cap(page.Data)))
	page.Release()
	cache.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("force cache did not return after cache close")
	}
	if refs := atomic.LoadInt32(&page.refs); refs != 1 {
		t.Fatalf("page refs after close race = %d, want 1", refs)
	}
	if page.Data == nil {
		t.Fatal("page data was released by cache after ownership was removed")
	}
	page.Release()
}

func TestCacheManagerRemoveStoreClosesRemovedStore(t *testing.T) {
	removed := newCloseTestCacheStore("removed")
	active := newCloseTestCacheStore("active")
	m := newCloseTestDiskCacheManager(removed, active)

	m.removeStore(removed.id)
	assertClosed(t, removed.done, "removed store done")
	if got := m.length(); got != 1 {
		t.Fatalf("manager length after remove = %d, want 1", got)
	}

	m.close()
	assertClosed(t, active.done, "active store done")
	assertClosed(t, removed.done, "removed store done after manager close")
}

func TestCacheManagerCloseWaitsForRemovedStoreClose(t *testing.T) {
	removed := newCloseTestCacheStore("removed")
	m := newCloseTestDiskCacheManager(removed)

	releaseStoreClose := make(chan struct{})
	removed.wg.Add(1)
	go func() {
		<-removed.done
		<-releaseStoreClose
		removed.wg.Done()
	}()

	removeDone := make(chan struct{})
	go func() {
		m.removeStore(removed.id)
		close(removeDone)
	}()
	waitClosed(t, removed.done, "removed store done")

	managerCloseDone := make(chan struct{})
	go func() {
		m.close()
		close(managerCloseDone)
	}()

	select {
	case <-managerCloseDone:
		t.Fatal("manager close returned before removed store close completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseStoreClose)
	select {
	case <-managerCloseDone:
	case <-time.After(time.Second):
		t.Fatal("manager close did not return after removed store close completed")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("removeStore did not return after removed store close completed")
	}
}

func TestCachedStoreCloseWaitsForSliceUpload(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	blob := &blockingPutStorage{
		ObjectStorage: mem,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	store := NewCachedStore(blob, Config{
		CacheDir:    "memory",
		CacheSize:   1 << 20,
		BlockSize:   4,
		Compress:    "none",
		MaxUpload:   1,
		MaxDownload: 1,
		BufferSize:  32 << 20,
		PutTimeout:  time.Second,
	}, nil).(*cachedStore)

	s := sliceForWrite(1, store, 0)
	p := NewOffPage(4)
	copy(p.Data, []byte("test"))
	s.pages[0] = []*Page{p}
	s.length = 4
	s.upload(0)

	select {
	case <-blob.entered:
	case <-time.After(time.Second):
		t.Fatal("upload did not reach object storage")
	}

	done := make(chan error, 1)
	go func() {
		done <- store.Close()
	}()

	select {
	case err := <-done:
		t.Fatalf("Close returned before upload finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(blob.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %s", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after upload finished")
	}
}

func TestCachedStoreCloseDoesNotDeleteSuccessfulStagingUpload(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	blob := &blockingPutStorage{
		ObjectStorage: mem,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	bcache := &closeTestCacheManager{}
	key := "chunks/0/0/321_0_4"
	stagingPath := filepath.Join(t.TempDir(), key)
	if err := os.MkdirAll(filepath.Dir(stagingPath), 0700); err != nil {
		t.Fatalf("mkdir staging dir: %s", err)
	}
	if err := os.WriteFile(stagingPath, []byte("good"), 0600); err != nil {
		t.Fatalf("write staging file: %s", err)
	}
	item := &pendingItem{key: key, fpath: stagingPath, ts: time.Now()}
	item.uploading.Store(true)
	store := &cachedStore{
		storage:       blob,
		bcache:        bcache,
		conf:          Config{BlockSize: 4, CacheChecksum: CsNone, PutTimeout: time.Second},
		currentUpload: make(chan struct{}, 1),
		pendingKeys: map[string]*pendingItem{
			key: item,
		},
		done:       make(chan struct{}),
		compressor: compress.NewCompressor("none"),
	}
	store.initMetrics()
	store.wg.Add(1)
	go func() {
		defer store.wg.Done()
		store.uploadStagingFile(key, stagingPath)
	}()

	select {
	case <-blob.entered:
	case <-time.After(time.Second):
		t.Fatal("staging upload did not reach object storage")
	}

	done := make(chan error, 1)
	go func() {
		done <- store.Close()
	}()

	select {
	case err := <-done:
		t.Fatalf("Close returned before staging upload finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(blob.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %s", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after staging upload finished")
	}

	if got := blob.deleteCount.Load(); got != 0 {
		t.Fatalf("object delete count = %d, want 0", got)
	}
	if got := bcache.uploadedCount.Load(); got != 1 {
		t.Fatalf("uploaded count = %d, want 1", got)
	}
	if got := bcache.removeStageCount.Load(); got != 1 {
		t.Fatalf("removeStage count = %d, want 1", got)
	}
	in, err := mem.Get(context.Background(), key, 0, -1)
	if err != nil {
		t.Fatalf("uploaded object missing after close: %s", err)
	}
	defer in.Close()
	data, err := io.ReadAll(in)
	if err != nil {
		t.Fatalf("read uploaded object: %s", err)
	}
	if string(data) != "good" {
		t.Fatalf("uploaded object = %q, want %q", data, "good")
	}
}

func TestCachedStoreUploadAfterCloseReportsError(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(mem, Config{
		CacheDir:    "memory",
		CacheSize:   1 << 20,
		BlockSize:   4,
		Compress:    "none",
		MaxUpload:   1,
		MaxDownload: 1,
		BufferSize:  32 << 20,
	}, nil).(*cachedStore)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}

	s := sliceForWrite(1, store, 0)
	p := NewOffPage(4)
	copy(p.Data, bytes.Repeat([]byte{'x'}, 4))
	s.pages[0] = []*Page{p}
	s.length = 4
	s.upload(0)

	select {
	case err := <-s.errors:
		if err == nil {
			t.Fatal("upload after close returned nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("upload after close did not report an error")
	}
}
