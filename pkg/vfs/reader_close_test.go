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

package vfs

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

type closeReadMeta struct {
	meta.Meta
}

func (m closeReadMeta) Read(ctx meta.Context, inode Ino, indx uint32, slices *[]meta.Slice) syscall.Errno {
	*slices = []meta.Slice{{Id: 1, Size: 4, Len: 4}}
	return 0
}

type blockingCloseReader struct {
	enterOnce  sync.Once
	cancelOnce sync.Once
	entered    chan struct{}
	canceled   chan struct{}
}

func newBlockingCloseReader() *blockingCloseReader {
	return &blockingCloseReader{
		entered:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
}

func (r *blockingCloseReader) ReadAt(ctx context.Context, p *chunk.Page, off int) (int, error) {
	r.enterOnce.Do(func() { close(r.entered) })
	select {
	case <-ctx.Done():
		r.cancelOnce.Do(func() { close(r.canceled) })
		return 0, ctx.Err()
	case <-time.After(time.Second):
		return 0, errors.New("read was not canceled")
	}
}

type closeReadStore struct {
	cancelTestStore
	reader *blockingCloseReader
}

func (s *closeReadStore) NewReader(id uint64, length int) chunk.Reader {
	return s.reader
}

func TestDataReaderCloseInterruptsInFlightRead(t *testing.T) {
	reader := newBlockingCloseReader()
	r := NewDataReader(&Config{
		Meta: &meta.Config{Retries: 1},
		Chunk: &chunk.Config{
			BlockSize:  4,
			BufferSize: 1 << 20,
		},
	}, closeReadMeta{}, &closeReadStore{reader: reader}).(*dataReader)

	f := r.Open(1, 4)
	type result struct {
		n  int
		st syscall.Errno
	}
	done := make(chan result, 1)
	go func() {
		n, st := f.Read(meta.Background(), 0, make([]byte, 4))
		done <- result{n: n, st: st}
	}()

	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}

	if err := r.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	select {
	case <-reader.canceled:
	case <-time.After(time.Second):
		t.Fatal("underlying read was not canceled")
	}
	select {
	case got := <-done:
		if got.n != 0 || got.st != syscall.EBADF {
			t.Fatalf("read after close = (%d, %s), want (0, %s)", got.n, got.st, syscall.EBADF)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not return after close")
	}
}

func TestDataReaderCloseReleasesDelayedRetryReadBuffer(t *testing.T) {
	r := &dataReader{
		files: make(map[Ino]*fileReader),
		done:  make(chan struct{}),
	}
	f := &fileReader{
		r:      r,
		inode:  1,
		length: 4,
		refs:   1,
	}
	page := chunk.NewOffPage(4)
	s := &sliceReader{
		file:  f,
		block: &frange{off: 0, len: 4},
		state: NEW,
		page:  page,
		cond:  utils.NewCond(&f.Mutex),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.prev = &f.slices
	f.slices = s
	f.last = &s.next
	r.files[f.inode] = f
	readBufferUsed.Add(int64(cap(s.page.Data)))
	s.delay(time.Hour)

	if err := r.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	if page.Data != nil {
		t.Fatal("delayed retry page was not released during close")
	}
	if f.slices != nil {
		t.Fatal("delayed retry slice is still linked after close")
	}
	if got := r.files[f.inode]; got != nil {
		t.Fatalf("file reader still tracked after close: %p", got)
	}
}
