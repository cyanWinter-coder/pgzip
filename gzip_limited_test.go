// Copyright 2014 Klaus Post. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"bytes"
	stdgzip "compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLimitedData(n int) []byte {
	rng := rand.New(rand.NewSource(42))
	// Semi-compressible data.
	b := make([]byte, n)
	for i := range b {
		if i%7 == 0 {
			b[i] = byte(rng.Intn(256))
		} else {
			b[i] = byte(i >> 8)
		}
	}
	return b
}

func stdlibReadAll(t *testing.T, b []byte) []byte {
	t.Helper()
	r, err := stdgzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("stdlib gzip.NewReader: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("stdlib ReadAll: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("stdlib Close: %v", err)
	}
	return out
}

// TestLimitedRoundTripLimits writes the same payload with a variety of
// limit configurations, including minimal capacity, and verifies the
// output is readable by the standard library.
func TestLimitedRoundTripLimits(t *testing.T) {
	data := testLimitedData(3*defaultBlockSize + 12345)
	configs := []Limits{
		{}, // all defaults
		{MaxInflightBlocks: 1, MaxQueuedBlocks: 0, MaxPendingBytes: defaultBlockSize},
		{MaxInflightBlocks: 2, MaxQueuedBlocks: 1},
		{BlockSize: 1 << 16, MaxInflightBlocks: 3, MaxQueuedBlocks: 2, MaxPendingBytes: 4 << 16},
		{BlockSize: 1 << 16, MaxInflightBlocks: 1, MaxQueuedBlocks: 0, MaxPendingBytes: 1 << 16},
	}
	for i, lim := range configs {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewLimitedWriter(&buf, DefaultCompression, lim)
			if err != nil {
				t.Fatalf("NewLimitedWriter: %v", err)
			}
			// Write in odd-sized chunks to cross block boundaries.
			off := 0
			for off < len(data) {
				n := 100000
				if n > len(data)-off {
					n = len(data) - off
				}
				if _, err := w.Write(data[off : off+n]); err != nil {
					t.Fatalf("Write: %v", err)
				}
				off += n
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			got := stdlibReadAll(t, buf.Bytes())
			if !bytes.Equal(got, data) {
				t.Fatalf("round trip mismatch: got %d bytes, want %d", len(got), len(data))
			}
			st := w.Stats()
			if st.PendingBlocks != 0 || st.PendingBytes != 0 {
				t.Fatalf("pending after close: %+v", st)
			}
			if st.CanceledBlocks != 0 {
				t.Fatalf("canceled blocks on clean run: %+v", st)
			}
			if st.WrittenBlocks != st.SubmittedBlocks {
				t.Fatalf("written %d != submitted %d", st.WrittenBlocks, st.SubmittedBlocks)
			}
		})
	}
}

// TestLimitedEmpty verifies an empty payload forms a valid gzip stream.
func TestLimitedEmpty(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewLimitedWriter(&buf, DefaultCompression, Limits{MaxInflightBlocks: 1})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := stdlibReadAll(t, buf.Bytes()); len(got) != 0 {
		t.Fatalf("got %d bytes, want 0", len(got))
	}
}

// TestLimitedFlush verifies Flush makes data available to a reader without
// closing the stream, and that more data can be written afterwards.
func TestLimitedFlush(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewLimitedWriter(&buf, DefaultCompression, Limits{MaxInflightBlocks: 2, MaxQueuedBlocks: 1})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	part1 := testLimitedData(100000)
	part2 := testLimitedData(200000)
	if _, err := w.Write(part1); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Stream must not be closed: write more.
	if _, err := w.Write(part2); err != nil {
		t.Fatalf("Write after Flush: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := stdlibReadAll(t, buf.Bytes())
	want := append(append([]byte{}, part1...), part2...)
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip mismatch: got %d, want %d", len(got), len(want))
	}
}

// TestLimitedResetAfterClose verifies Reset reuses the writer for a new
// stream on a new output.
func TestLimitedResetAfterClose(t *testing.T) {
	w, err := NewLimitedWriter(io.Discard, BestSpeed, Limits{MaxInflightBlocks: 2})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	data1 := testLimitedData(1 << 20)
	data2 := testLimitedData(500000)
	var buf1 bytes.Buffer
	w.Reset(&buf1)
	if _, err := w.Write(data1); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var buf2 bytes.Buffer
	w.Reset(&buf2)
	if _, err := w.Write(data2); err != nil {
		t.Fatalf("Write after Reset: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close after Reset: %v", err)
	}
	if got := stdlibReadAll(t, buf1.Bytes()); !bytes.Equal(got, data1) {
		t.Fatal("stream 1 mismatch")
	}
	if got := stdlibReadAll(t, buf2.Bytes()); !bytes.Equal(got, data2) {
		t.Fatal("stream 2 mismatch")
	}
	if w.UncompressedSize() != len(data2) {
		t.Fatalf("UncompressedSize = %d, want %d", w.UncompressedSize(), len(data2))
	}
}

// blockingWriter blocks each Write until a token is available on release
// or done is closed. Tokens can be pre-loaded since release is buffered.
// Accepted bytes are recorded and can be read back with Bytes.
type blockingWriter struct {
	release chan struct{}
	done    chan struct{}
	writes  int64
	bytes   int64

	mu  sync.Mutex
	buf bytes.Buffer
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{release: make(chan struct{}, 1024), done: make(chan struct{})}
}

func (b *blockingWriter) Write(p []byte) (int, error) {
	select {
	case <-b.release:
	case <-b.done:
		return 0, errors.New("blockingWriter: abandoned")
	}
	atomic.AddInt64(&b.writes, 1)
	atomic.AddInt64(&b.bytes, int64(len(p)))
	b.mu.Lock()
	b.buf.Write(p)
	b.mu.Unlock()
	return len(p), nil
}

func (b *blockingWriter) Writes() int64 { return atomic.LoadInt64(&b.writes) }

func (b *blockingWriter) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

func (b *blockingWriter) allow(n int) {
	for i := 0; i < n; i++ {
		b.release <- struct{}{}
	}
}

// TestLimitedCancel verifies that Cancel unblocks a blocked Write, that
// nothing more is written to the old stream once the generation has been
// drained, and that the writer can be reused after Reset.
func TestLimitedCancel(t *testing.T) {
	bw := newBlockingWriter()
	defer close(bw.done)
	bw.allow(1) // let the gzip header through
	w, err := NewLimitedWriter(bw, DefaultCompression, Limits{
		MaxInflightBlocks: 1,
		MaxQueuedBlocks:   2,
		MaxPendingBytes:   4 * defaultBlockSize,
	})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	data := testLimitedData(8 * defaultBlockSize)
	errCh := make(chan error, 1)
	go func() {
		_, err := w.Write(data)
		errCh <- err
	}()
	// Let the first blocks get accepted and the output writer block.
	time.Sleep(200 * time.Millisecond)
	w.Cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("Write error = %v, want ErrCanceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write did not return after Cancel")
	}
	// Allow any in-flight block write to finish, then drain the
	// generation completely via Reset. After Reset returns, nothing may
	// write to the old stream anymore.
	bw.allow(16)
	var buf bytes.Buffer
	w.Reset(&buf)
	writesAfterReset := bw.Writes()
	time.Sleep(100 * time.Millisecond)
	if got := bw.Writes(); got != writesAfterReset {
		t.Fatalf("writes to old stream after cancel: %d -> %d", writesAfterReset, got)
	}
	st := w.Stats()
	if st.WrittenBlocks+st.CanceledBlocks != st.SubmittedBlocks {
		t.Fatalf("stats inconsistent: %+v", st)
	}
	if st.CanceledBlocks == 0 {
		t.Fatalf("expected canceled blocks, got %+v", st)
	}
	if st.PendingBlocks != 0 || st.PendingBytes != 0 {
		t.Fatalf("pending after reset: %+v", st)
	}

	// Reuse on a new stream.
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write after Reset: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close after Reset: %v", err)
	}
	if got := stdlibReadAll(t, buf.Bytes()); !bytes.Equal(got, data) {
		t.Fatal("round trip mismatch after Cancel+Reset")
	}
}

// failAfterWriter fails with errInject after byteLimit bytes.
type failAfterWriter struct {
	byteLimit int
	written   int
	errInject error
}

func (f *failAfterWriter) Write(p []byte) (int, error) {
	if f.written+len(p) > f.byteLimit {
		n := f.byteLimit - f.written
		f.written = f.byteLimit
		return n, f.errInject
	}
	f.written += len(p)
	return len(p), nil
}

// TestLimitedWriteError verifies a downstream write error stops compression
// and is returned verbatim by Flush/Close.
func TestLimitedWriteError(t *testing.T) {
	errInject := errors.New("injected downstream failure")
	fw := &failAfterWriter{byteLimit: 100, errInject: errInject}
	w, err := NewLimitedWriter(fw, DefaultCompression, Limits{MaxInflightBlocks: 4, MaxQueuedBlocks: 4})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	data := testLimitedData(2 * defaultBlockSize)
	_, werr := w.Write(data)
	if err := w.Flush(); err != nil && werr == nil {
		werr = err
	}
	if err := w.Close(); err != nil && werr == nil {
		werr = err
	}
	if !errors.Is(werr, errInject) {
		t.Fatalf("got error %v, want injected error", werr)
	}
	// Later calls must keep returning the original cause.
	if err := w.Flush(); !errors.Is(err, errInject) {
		t.Fatalf("Flush after error = %v, want injected error", err)
	}
	st := w.Stats()
	if st.PendingBlocks != 0 || st.PendingBytes != 0 {
		t.Fatalf("pending after failed close: %+v", st)
	}
}

// TestLimitedWriteContextCancel verifies a per-call context cancels a
// blocked WriteContext without breaking the stream.
func TestLimitedWriteContextCancel(t *testing.T) {
	bw := newBlockingWriter()
	defer close(bw.done)
	bw.allow(1) // let the gzip header through
	w, err := NewLimitedWriter(bw, DefaultCompression, Limits{
		MaxInflightBlocks: 1,
		MaxQueuedBlocks:   0,
		MaxPendingBytes:   defaultBlockSize,
	})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	data := testLimitedData(4 * defaultBlockSize)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	n, err := w.WriteContext(ctx, data)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WriteContext error = %v, want DeadlineExceeded", err)
	}
	// Unblock downstream; accepted data must still form a valid stream.
	bw.allow(16)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st := w.Stats()
	if st.PendingBlocks != 0 {
		t.Fatalf("pending after close: %+v", st)
	}
	// The accepted prefix must be readable by the standard library.
	got := stdlibReadAll(t, bw.Bytes())
	if !bytes.Equal(got, data[:n]) {
		t.Fatalf("accepted prefix mismatch: got %d bytes, want %d", len(got), n)
	}
}

// TestLimitedBackpressure verifies the pending-byte limit is never exceeded.
func TestLimitedBackpressure(t *testing.T) {
	const maxPending = defaultBlockSize * 2
	bw := newBlockingWriter()
	defer close(bw.done)
	w, err := NewLimitedWriter(bw, DefaultCompression, Limits{
		MaxInflightBlocks: 2,
		MaxQueuedBlocks:   2,
		MaxPendingBytes:   maxPending,
	})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	data := testLimitedData(6 * defaultBlockSize)
	var maxSeen int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			st := w.Stats()
			if st.PendingBytes > maxSeen {
				maxSeen = st.PendingBytes
			}
			time.Sleep(time.Millisecond)
		}
	}()
	// Slowly release the downstream writer while Write applies backpressure.
	go func() {
		for i := 0; i < 64; i++ {
			bw.allow(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(done)
	wg.Wait()
	if maxSeen > maxPending {
		t.Fatalf("pending bytes exceeded limit: %d > %d", maxSeen, maxPending)
	}
}

// TestLimitedWriteAfterClose verifies writes after Close are rejected.
func TestLimitedWriteAfterClose(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewLimitedWriter(&buf, DefaultCompression, Limits{})
	if err != nil {
		t.Fatalf("NewLimitedWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Write([]byte("nope")); err == nil {
		t.Fatal("Write after Close: expected error")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("double Close: %v", err)
	}
}

// TestLimitedInvalidLimits verifies limit validation.
func TestLimitedInvalidLimits(t *testing.T) {
	if _, err := NewLimitedWriter(io.Discard, 100, Limits{}); err == nil {
		t.Fatal("expected error for invalid level")
	}
	if _, err := NewLimitedWriter(io.Discard, DefaultCompression, Limits{BlockSize: 100}); err == nil {
		t.Fatal("expected error for tiny block size")
	}
	if _, err := NewLimitedWriter(io.Discard, DefaultCompression, Limits{MaxInflightBlocks: -1}); err == nil {
		t.Fatal("expected error for negative inflight")
	}
	if _, err := NewLimitedWriter(io.Discard, DefaultCompression, Limits{MaxQueuedBlocks: -1}); err == nil {
		t.Fatal("expected error for negative queue")
	}
}

// TestLimitedDefaultWriterUnchanged verifies the classic Writer is
// unaffected by the new code and still round-trips.
func TestLimitedDefaultWriterUnchanged(t *testing.T) {
	data := testLimitedData(1 << 20)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := stdlibReadAll(t, buf.Bytes()); !bytes.Equal(got, data) {
		t.Fatal("default Writer round trip mismatch")
	}
}
