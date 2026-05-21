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
	"errors"
	"io"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
)

type cancelTestStore struct {
	usedMemory int64
}

func (s *cancelTestStore) NewReader(id uint64, length int) chunk.Reader { return nil }
func (s *cancelTestStore) NewWriter(id uint64, tierID uint8) chunk.Writer {
	return nil
}
func (s *cancelTestStore) Remove(id uint64, length int) error { return nil }
func (s *cancelTestStore) FillCache(id uint64, length uint32) error {
	return nil
}
func (s *cancelTestStore) EvictCache(id uint64, length uint32) error {
	return nil
}
func (s *cancelTestStore) CheckCache(id uint64, length uint32, handler func(exists bool, loc string, size int)) error {
	return nil
}
func (s *cancelTestStore) UsedMemory() int64 { return s.usedMemory }
func (s *cancelTestStore) UpdateLimit(upload, download int64) {
}
func (s *cancelTestStore) BlobStorage() object.ObjectStorage { return nil }

func TestFileWriterWriteCanceledWhileWaitingForSliceLimit(t *testing.T) {
	ctx := meta.NewContext(1, 0, []uint32{0})
	ctx.Cancel()

	f := &fileWriter{
		w: &dataWriter{
			store:      &cancelTestStore{},
			bufferSize: 1 << 20,
		},
		chunks: map[uint32]*chunkWriter{
			0: {slices: make([]*sliceWriter, 1000)},
		},
	}

	if st := writeWithTimeout(t, f, ctx); st != syscall.EINTR {
		t.Fatalf("Write status = %s, want %s", st, syscall.EINTR)
	}
}

func TestFileWriterWriteCanceledWhileThrottledByBuffer(t *testing.T) {
	ctx := meta.NewContext(1, 0, []uint32{0})
	ctx.Cancel()

	buf := utils.Alloc(4)
	defer utils.Free(buf)

	f := &fileWriter{
		w: &dataWriter{
			store:      &cancelTestStore{},
			bufferSize: 1,
		},
	}

	if st := writeWithTimeout(t, f, ctx); st != syscall.EINTR {
		t.Fatalf("Write status = %s, want %s", st, syscall.EINTR)
	}
}

func writeWithTimeout(t *testing.T, f *fileWriter, ctx meta.Context) syscall.Errno {
	t.Helper()
	done := make(chan syscall.Errno, 1)
	go func() {
		done <- f.Write(ctx, 0, nil)
	}()

	select {
	case st := <-done:
		return st
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Write did not return after context cancellation")
		return 0
	}
}

type blockingChunkWriter struct {
	abortOnce sync.Once
	aborted   chan struct{}
}

func newBlockingChunkWriter() *blockingChunkWriter {
	return &blockingChunkWriter{aborted: make(chan struct{})}
}

func (w *blockingChunkWriter) WriteAt(p []byte, off int64) (int, error) {
	return len(p), nil
}

func (w *blockingChunkWriter) ID() uint64 { return 1 }

func (w *blockingChunkWriter) SetID(id uint64) {}

func (w *blockingChunkWriter) SetWriteback(enabled bool) {}

func (w *blockingChunkWriter) FlushTo(offset int) error { return nil }

func (w *blockingChunkWriter) Finish(length int) error {
	<-w.aborted
	return errors.New("aborted")
}

func (w *blockingChunkWriter) Abort() {
	w.abortOnce.Do(func() {
		close(w.aborted)
	})
}

var _ chunk.Writer = (*blockingChunkWriter)(nil)
var _ io.WriterAt = (*blockingChunkWriter)(nil)

type recordingChunkWriter struct {
	data []byte
}

func (w *recordingChunkWriter) WriteAt(p []byte, off int64) (int, error) {
	end := int(off) + len(p)
	if end > len(w.data) {
		buf := make([]byte, end)
		copy(buf, w.data)
		w.data = buf
	}
	copy(w.data[off:], p)
	return len(p), nil
}

func (w *recordingChunkWriter) ID() uint64 { return 1 }

func (w *recordingChunkWriter) SetID(id uint64) {}

func (w *recordingChunkWriter) SetWriteback(enabled bool) {}

func (w *recordingChunkWriter) FlushTo(offset int) error { return nil }

func (w *recordingChunkWriter) Finish(length int) error { return nil }

func (w *recordingChunkWriter) Abort() {}

var _ chunk.Writer = (*recordingChunkWriter)(nil)
var _ io.WriterAt = (*recordingChunkWriter)(nil)

func TestDataWriterCloseAbortsTimedOutFlush(t *testing.T) {
	bw := newBlockingChunkWriter()
	w := &dataWriter{
		conf: &Config{Chunk: &chunk.Config{
			BlockSize:  1 << 20,
			BufferSize: 1 << 20,
			PutTimeout: 20 * time.Millisecond,
		}},
		store:      &cancelTestStore{},
		done:       make(chan struct{}),
		files:      make(map[Ino]*fileWriter),
		bufferSize: 1 << 20,
	}
	f := &fileWriter{
		w:      w,
		inode:  1,
		chunks: make(map[uint32]*chunkWriter),
	}
	f.flushcond = utils.NewCond(f)
	f.writecond = utils.NewCond(f)
	c := &chunkWriter{indx: 0, file: f}
	s := &sliceWriter{
		id:      1,
		chunk:   c,
		writer:  bw,
		slen:    1,
		notify:  utils.NewCond(f),
		started: time.Now(),
		lastMod: time.Now(),
	}
	c.slices = []*sliceWriter{s}
	f.chunks[c.indx] = c
	w.files[f.inode] = f

	done := make(chan error, 1)
	go func() {
		done <- w.Close()
	}()

	select {
	case err := <-done:
		if !errors.Is(err, syscall.EINTR) {
			t.Fatalf("Close error = %v, want %s", err, syscall.EINTR)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not abort a timed-out flush")
	}

	select {
	case <-bw.aborted:
	default:
		t.Fatal("Close did not abort the stuck slice writer")
	}
}

func TestClosedDataWriterOpenCloseDoesNotRemoveTrackedWriter(t *testing.T) {
	w := &dataWriter{
		store:      &cancelTestStore{},
		done:       make(chan struct{}),
		files:      make(map[Ino]*fileWriter),
		bufferSize: 1 << 20,
	}
	tracked := &fileWriter{
		w:      w,
		inode:  1,
		refs:   1,
		chunks: make(map[uint32]*chunkWriter),
	}
	tracked.flushcond = utils.NewCond(tracked)
	tracked.writecond = utils.NewCond(tracked)
	w.files[tracked.inode] = tracked
	w.closed.Store(true)

	closed := w.Open(tracked.inode, tracked.length, tracked.tierID)
	if _, ok := closed.(closedFileWriter); !ok {
		t.Fatalf("closed dataWriter Open returned %T, want closedFileWriter", closed)
	}
	if st := closed.Close(meta.Background()); st != syscall.EBADF {
		t.Fatalf("closed writer Close status = %s, want %s", st, syscall.EBADF)
	}
	if got := w.files[tracked.inode]; got != tracked {
		t.Fatalf("closed writer removed tracked writer: got %p, want %p", got, tracked)
	}
	if tracked.refs != 1 {
		t.Fatalf("tracked writer refs = %d, want 1", tracked.refs)
	}
}

func TestFileWriterWriteFailsWhenDataWriterClosesWhileWaitingForFlush(t *testing.T) {
	w := &dataWriter{
		store:      &cancelTestStore{},
		done:       make(chan struct{}),
		files:      make(map[Ino]*fileWriter),
		blockSize:  1 << 20,
		bufferSize: 1 << 20,
	}
	f := &fileWriter{
		w:            w,
		inode:        1,
		refs:         1,
		flushwaiting: 1,
		chunks:       make(map[uint32]*chunkWriter),
	}
	f.flushcond = utils.NewCond(f)
	f.writecond = utils.NewCond(f)
	writer := &recordingChunkWriter{}
	c := &chunkWriter{indx: 0, file: f}
	s := &sliceWriter{
		id:      1,
		chunk:   c,
		writer:  writer,
		notify:  utils.NewCond(f),
		started: time.Now(),
		lastMod: time.Now(),
	}
	c.slices = []*sliceWriter{s}
	f.chunks[c.indx] = c
	w.files[f.inode] = f

	done := make(chan syscall.Errno, 1)
	go func() {
		done <- f.Write(meta.Background(), 0, []byte("x"))
	}()
	waitForWriteWaiting(t, f)

	w.closed.Store(true)
	w.closeTrackedWriters()

	select {
	case st := <-done:
		if st != syscall.EBADF {
			t.Fatalf("Write status = %s, want %s", st, syscall.EBADF)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Write did not return after dataWriter close")
	}
	if s.slen != 0 {
		t.Fatalf("slice length = %d, want 0", s.slen)
	}
	if len(writer.data) != 0 {
		t.Fatalf("writer received %d bytes after close, want 0", len(writer.data))
	}
}

func TestFileWriterWriteFailsWhenDataWriterClosesBeforeFlushWaitEnds(t *testing.T) {
	w := &dataWriter{
		store:      &cancelTestStore{},
		done:       make(chan struct{}),
		files:      make(map[Ino]*fileWriter),
		blockSize:  1 << 20,
		bufferSize: 1 << 20,
	}
	f := &fileWriter{
		w:            w,
		inode:        1,
		refs:         1,
		flushwaiting: 1,
		chunks:       make(map[uint32]*chunkWriter),
	}
	f.flushcond = utils.NewCond(f)
	f.writecond = utils.NewCond(f)
	writer := &recordingChunkWriter{}
	c := &chunkWriter{indx: 0, file: f}
	s := &sliceWriter{
		id:      1,
		chunk:   c,
		writer:  writer,
		notify:  utils.NewCond(f),
		started: time.Now(),
		lastMod: time.Now(),
	}
	c.slices = []*sliceWriter{s}
	f.chunks[c.indx] = c
	w.files[f.inode] = f

	done := make(chan syscall.Errno, 1)
	go func() {
		done <- f.Write(meta.Background(), 0, []byte("x"))
	}()
	waitForWriteWaiting(t, f)

	f.Lock()
	w.closed.Store(true)
	f.flushwaiting = 0
	f.writecond.Broadcast()
	f.Unlock()

	select {
	case st := <-done:
		if st != syscall.EBADF {
			t.Fatalf("Write status = %s, want %s", st, syscall.EBADF)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Write did not return after dataWriter close")
	}
	if s.slen != 0 {
		t.Fatalf("slice length = %d, want 0", s.slen)
	}
	if len(writer.data) != 0 {
		t.Fatalf("writer received %d bytes after close, want 0", len(writer.data))
	}
}

func waitForWriteWaiting(t *testing.T, f *fileWriter) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		f.Lock()
		waiting := f.writewaiting
		f.Unlock()
		if waiting > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Write did not start waiting for flush")
}
