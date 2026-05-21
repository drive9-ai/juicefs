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

package fs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

func TestFileSystemCloseCascadesToWriter(t *testing.T) {
	fs := createTestFS(t)
	defer fs.m.Shutdown() //nolint:errcheck

	ctx := meta.NewContext(1, 0, []uint32{0})
	f, errno := fs.Create(ctx, "/x", 0644, 0)
	if errno != 0 {
		t.Fatalf("create: %s", errno)
	}

	if err := fs.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("second close: %s", err)
	}
	if _, errno := f.Pwrite(ctx, []byte("x"), 0); errno != syscall.EBADF {
		t.Fatalf("pwrite after close = %s, want %s", errno, syscall.EBADF)
	}
}

func TestFlushLogReturnsWhenBufferClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("open access log: %s", err)
	}

	fs := &FileSystem{
		checkAccessFile: time.Hour,
		rotateAccessLog: 1 << 60,
	}
	logBuffer := make(chan string, 1)
	fs.wg.Add(1)
	done := make(chan struct{})
	go func() {
		fs.flushLog(f, logBuffer, path)
		close(done)
	}()

	logBuffer <- "line\n"
	close(logBuffer)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flushLog did not return after log buffer close")
	}

	waited := make(chan struct{})
	go func() {
		fs.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("filesystem wait group did not observe flushLog exit")
	}
}
