// Copyright 2014 Klaus Post. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgzip

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/flate"
)

// ErrCanceled is returned by LimitedWriter methods after Cancel has been
// called and the current generation has been aborted.
var ErrCanceled = errors.New("gzip: writer canceled")

// errGenDone is an internal sentinel signaling that the current generation
// was canceled. It is never returned to callers; it is translated to the
// recorded error (or ErrCanceled) instead.
var errGenDone = errors.New("gzip: generation done")

// Limits configures the adaptive backpressure of a LimitedWriter.
//
// All limits are applied per call: a Write (or Flush/Close) that would
// exceed any of the limits blocks until capacity is available, applying
// backpressure to the caller instead of queuing unbounded work.
type Limits struct {
	// BlockSize is the number of uncompressed bytes per compression block.
	// If zero, defaultBlockSize (1 MB) is used.
	BlockSize int

	// MaxInflightBlocks is the maximum number of blocks being compressed
	// concurrently. If zero, runtime.GOMAXPROCS(0) is used.
	MaxInflightBlocks int

	// MaxQueuedBlocks is the maximum number of accepted blocks waiting to
	// be picked up by a compressor, on top of MaxInflightBlocks.
	// If zero, no queue is allowed and Write blocks until a compressor
	// slot is free.
	MaxQueuedBlocks int

	// MaxPendingBytes is the maximum number of uncompressed bytes that may
	// be accepted but not yet written to the output. If zero, it defaults
	// to BlockSize * (MaxInflightBlocks + MaxQueuedBlocks). Values smaller
	// than BlockSize are raised to BlockSize so a single block always fits.
	MaxPendingBytes int
}

func (l *Limits) normalize() error {
	if l.BlockSize == 0 {
		l.BlockSize = defaultBlockSize
	}
	if l.BlockSize <= tailSize {
		return fmt.Errorf("gzip: block size cannot be less than or equal to %d", tailSize)
	}
	if l.MaxInflightBlocks == 0 {
		l.MaxInflightBlocks = runtime.GOMAXPROCS(0)
	}
	if l.MaxInflightBlocks < 0 {
		return errors.New("gzip: MaxInflightBlocks cannot be negative")
	}
	if l.MaxQueuedBlocks < 0 {
		return errors.New("gzip: MaxQueuedBlocks cannot be negative")
	}
	if l.MaxPendingBytes < 0 {
		return errors.New("gzip: MaxPendingBytes cannot be negative")
	}
	if l.MaxPendingBytes == 0 {
		l.MaxPendingBytes = l.BlockSize * (l.MaxInflightBlocks + l.MaxQueuedBlocks)
	}
	if l.MaxPendingBytes < l.BlockSize {
		l.MaxPendingBytes = l.BlockSize
	}
	return nil
}

// totalBlocks is the maximum number of outstanding blocks.
func (l *Limits) totalBlocks() int {
	return l.MaxInflightBlocks + l.MaxQueuedBlocks
}

// LimitedStats reports a snapshot of a LimitedWriter's block accounting.
// Counters are cumulative across Reset generations; pending values reflect
// the current generation only.
type LimitedStats struct {
	// SubmittedBlocks is the number of blocks accepted by Write/Flush/Close.
	SubmittedBlocks int64
	// StartedBlocks is the number of blocks picked up by a compressor.
	// SubmittedBlocks - StartedBlocks are accepted but not yet started.
	StartedBlocks int64
	// WrittenBlocks is the number of blocks fully written to the output.
	WrittenBlocks int64
	// CanceledBlocks is the number of accepted blocks discarded without
	// being (fully) written, due to cancellation or a downstream error.
	CanceledBlocks int64
	// PendingBlocks is the number of accepted blocks not yet resolved.
	PendingBlocks int64
	// PendingBytes is the number of uncompressed bytes accepted but not
	// yet resolved.
	PendingBytes int64
	// WrittenBytes is the number of compressed block bytes written to the
	// output (excluding header and trailer).
	WrittenBytes int64
}

// LimitedWriter is an io.WriteCloser like Writer, but with per-call
// adaptive limits on concurrency, queue depth and pending bytes.
//
// Unlike Writer, which always allows up to GOMAXPROCS blocks in flight,
// a LimitedWriter applies backpressure to the caller as soon as any of
// its Limits is reached. This keeps CPU and memory usage bounded for
// bursty archival workloads without slowing down single large streams
// more than the configured limits require.
//
// A LimitedWriter must not be copied after first use. Methods must not be
// called concurrently, except Cancel and Stats, which may be called from
// any goroutine.
type LimitedWriter struct {
	Header
	w      io.Writer
	level  int
	limits Limits

	wroteHeader   bool
	closed        bool
	currentBuffer []byte
	prevTail      []byte
	digest        hash.Hash32
	size          int
	buf           [10]byte

	errMu sync.RWMutex
	err   error

	gen atomic.Pointer[limitedGen]

	dictFlatePool sync.Pool
	dstPool       sync.Pool

	// Statistics, accessed atomically.
	submittedBlocks int64
	startedBlocks   int64
	writtenBlocks   int64
	canceledBlocks  int64
	pendingBlocks   int64
	pendingBytes    int64
	writtenBytes    int64
}

// limitedGen is one generation of the compression pipeline. A generation
// shares a single cancellation boundary: blocked writes, Flush, Reset and
// Cancel all observe the same context.
type limitedGen struct {
	ctx    context.Context
	cancel context.CancelFunc

	jobs     chan *limitedBlock
	res      chan *limitedResult
	blockSem chan struct{}
	byteSem  *weightedSem
	wg       sync.WaitGroup

	submitted int64 // atomic
	handled   int64 // atomic

	progMu sync.Mutex
	progCh chan struct{}
}

type limitedBlock struct {
	seq   int
	data  []byte
	tail  []byte
	final bool
}

type limitedResult struct {
	seq  int
	size int // uncompressed size of the source block
	buf  []byte
}

// weightedSem is a cancellable, broadcast-based weighted semaphore.
type weightedSem struct {
	mu     sync.Mutex
	free   int
	notify chan struct{}
}

func newWeightedSem(n int) *weightedSem {
	return &weightedSem{free: n, notify: make(chan struct{})}
}

// acquire takes n units, blocking until available, ctx is done, or done is
// closed. It returns ctx.Err() on caller cancellation and errGenDone when
// done fires.
func (s *weightedSem) acquire(n int, ctx context.Context, done <-chan struct{}) error {
	if n <= 0 {
		return nil
	}
	for {
		s.mu.Lock()
		if s.free >= n {
			s.free -= n
			s.mu.Unlock()
			return nil
		}
		ch := s.notify
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return errGenDone
		}
	}
}

func (s *weightedSem) release(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.free += n
	close(s.notify)
	s.notify = make(chan struct{})
	s.mu.Unlock()
}

// NewLimitedWriter returns a new LimitedWriter with the given compression
// level and limits. See NewWriterLevel for valid level values.
//
// The zero Limits value behaves like a default Writer: 1 MB blocks and up
// to GOMAXPROCS blocks in flight.
func NewLimitedWriter(w io.Writer, level int, limits Limits) (*LimitedWriter, error) {
	if level < ConstantCompression || level > BestCompression {
		return nil, fmt.Errorf("gzip: invalid compression level: %d", level)
	}
	if err := limits.normalize(); err != nil {
		return nil, err
	}
	z := &LimitedWriter{
		w:      w,
		level:  level,
		limits: limits,
		digest: crc32.NewIEEE(),
	}
	z.Header = Header{OS: 255}
	z.dstPool.New = func() any {
		return make([]byte, 0, limits.BlockSize+(limits.BlockSize>>4))
	}
	z.dictFlatePool.New = func() any {
		// The output is replaced by ResetDict before first use;
		// io.Discard guarantees nothing is ever written to a stale stream.
		f, _ := flate.NewWriterDict(io.Discard, level, nil)
		return f
	}
	return z, nil
}

// Stats returns a snapshot of the writer's block accounting. It is safe to
// call from any goroutine.
func (z *LimitedWriter) Stats() LimitedStats {
	return LimitedStats{
		SubmittedBlocks: atomic.LoadInt64(&z.submittedBlocks),
		StartedBlocks:   atomic.LoadInt64(&z.startedBlocks),
		WrittenBlocks:   atomic.LoadInt64(&z.writtenBlocks),
		CanceledBlocks:  atomic.LoadInt64(&z.canceledBlocks),
		PendingBlocks:   atomic.LoadInt64(&z.pendingBlocks),
		PendingBytes:    atomic.LoadInt64(&z.pendingBytes),
		WrittenBytes:    atomic.LoadInt64(&z.writtenBytes),
	}
}

// pushError records the first error and cancels the current generation, so
// blocked writes, flushers and compressors all observe the same failure.
func (z *LimitedWriter) pushError(err error) {
	z.errMu.Lock()
	if z.err != nil {
		z.errMu.Unlock()
		return
	}
	z.err = err
	z.errMu.Unlock()
	if g := z.gen.Load(); g != nil {
		g.cancel()
	}
}

func (z *LimitedWriter) checkError() error {
	z.errMu.RLock()
	err := z.err
	z.errMu.RUnlock()
	return err
}

// genError returns the recorded error after the generation was canceled.
func (z *LimitedWriter) genError() error {
	if err := z.checkError(); err != nil {
		return err
	}
	return ErrCanceled
}

// Cancel aborts the current generation: blocked Write/Flush calls return,
// queued blocks are discarded and nothing more is written to the current
// output stream. The writer can be reused for a new stream with Reset.
// It is safe to call Cancel from any goroutine.
func (z *LimitedWriter) Cancel() {
	z.pushError(ErrCanceled)
}

// newLimitedGen creates a generation with fresh permits derived from the
// configured limits.
func (z *LimitedWriter) newLimitedGen() *limitedGen {
	ctx, cancel := context.WithCancel(context.Background())
	total := z.limits.totalBlocks()
	return &limitedGen{
		ctx:      ctx,
		cancel:   cancel,
		jobs:     make(chan *limitedBlock, total),
		res:      make(chan *limitedResult, total),
		blockSem: make(chan struct{}, total),
		byteSem:  newWeightedSem(z.limits.MaxPendingBytes),
		progCh:   make(chan struct{}),
	}
}

// startGen writes the gzip header (once) and starts the pipeline for a new
// generation. Must only be called from the caller's goroutine.
func (z *LimitedWriter) startGen() error {
	if z.gen.Load() != nil {
		return nil
	}
	if err := z.checkError(); err != nil {
		return err
	}
	if !z.wroteHeader {
		z.wroteHeader = true
		if err := z.writeGzipHeader(); err != nil {
			z.pushError(err)
			return err
		}
	}
	g := z.newLimitedGen()
	z.gen.Store(g)
	for i := 0; i < z.limits.MaxInflightBlocks; i++ {
		g.wg.Add(1)
		go z.genWorker(g)
	}
	g.wg.Add(1)
	go z.genWriter(g)
	z.currentBuffer = z.dstPool.Get().([]byte)[:0]
	return nil
}

// teardownGen cancels the generation, waits for its goroutines and recycles
// any blocks that were accepted but never resolved, keeping statistics
// accurate. After teardownGen returns, nothing will write to the old stream.
func (z *LimitedWriter) teardownGen(g *limitedGen) {
	g.cancel()
	g.wg.Wait()
	// Drain jobs never picked up by a worker.
	for {
		select {
		case b := <-g.jobs:
			size := len(b.data)
			z.recycleBlock(b)
			atomic.AddInt64(&z.canceledBlocks, 1)
			atomic.AddInt64(&z.pendingBlocks, -1)
			atomic.AddInt64(&z.pendingBytes, -int64(size))
		default:
			goto drainRes
		}
	}
drainRes:
	// Drain results never consumed by the output writer.
	for {
		select {
		case r := <-g.res:
			if r.buf != nil {
				z.dstPool.Put(r.buf)
			}
			atomic.AddInt64(&z.canceledBlocks, 1)
			atomic.AddInt64(&z.pendingBlocks, -1)
			atomic.AddInt64(&z.pendingBytes, -int64(r.size))
		default:
			return
		}
	}
}

// writeGzipHeader writes the GZIP header. Called once per stream.
func (z *LimitedWriter) writeGzipHeader() error {
	z.buf[0] = gzipID1
	z.buf[1] = gzipID2
	z.buf[2] = gzipDeflate
	z.buf[3] = 0
	if z.Extra != nil {
		z.buf[3] |= 0x04
	}
	if z.Name != "" {
		z.buf[3] |= 0x08
	}
	if z.Comment != "" {
		z.buf[3] |= 0x10
	}
	put4(z.buf[4:8], uint32(z.ModTime.Unix()))
	if z.level == BestCompression {
		z.buf[8] = 2
	} else if z.level == BestSpeed {
		z.buf[8] = 4
	} else {
		z.buf[8] = 0
	}
	z.buf[9] = z.OS
	if _, err := z.w.Write(z.buf[0:10]); err != nil {
		return err
	}
	if z.Extra != nil {
		if len(z.Extra) > 0xffff {
			return errors.New("gzip.Write: Extra data is too large")
		}
		put2(z.buf[0:2], uint16(len(z.Extra)))
		if _, err := z.w.Write(z.buf[0:2]); err != nil {
			return err
		}
		if _, err := z.w.Write(z.Extra); err != nil {
			return err
		}
	}
	if z.Name != "" {
		if err := z.writeGzipString(z.Name); err != nil {
			return err
		}
	}
	if z.Comment != "" {
		if err := z.writeGzipString(z.Comment); err != nil {
			return err
		}
	}
	return nil
}

// writeGzipString writes a UTF-8 string s in GZIP's format (NUL-terminated
// ISO 8859-1).
func (z *LimitedWriter) writeGzipString(s string) error {
	needconv := false
	for _, v := range s {
		if v == 0 || v > 0xff {
			return errors.New("gzip.Write: non-Latin-1 header string")
		}
		if v > 0x7f {
			needconv = true
		}
	}
	var err error
	if needconv {
		b := make([]byte, 0, len(s))
		for _, v := range s {
			b = append(b, byte(v))
		}
		_, err = z.w.Write(b)
	} else {
		_, err = io.WriteString(z.w, s)
	}
	if err != nil {
		return err
	}
	z.buf[0] = 0
	_, err = z.w.Write(z.buf[0:1])
	return err
}

// Write writes a compressed form of p to the underlying io.Writer. It is
// equivalent to WriteContext with a background context.
func (z *LimitedWriter) Write(p []byte) (int, error) {
	return z.WriteContext(context.Background(), p)
}

// WriteContext writes p like Write, but the call can be interrupted by ctx
// while it is blocked on the configured limits. Bytes already accepted
// remain part of the stream and will be flushed by a later call; the
// returned count always reflects the accepted bytes.
//
// The call blocks, applying backpressure, whenever the number of
// outstanding blocks, the queue depth or the total pending uncompressed
// bytes reach the configured limits.
func (z *LimitedWriter) WriteContext(ctx context.Context, p []byte) (int, error) {
	if err := z.checkError(); err != nil {
		return 0, err
	}
	if z.closed {
		return 0, errors.New("gzip: write after close")
	}
	if z.gen.Load() == nil {
		if err := z.startGen(); err != nil {
			return 0, err
		}
	}
	total := 0
	for len(p) > 0 {
		n := z.limits.BlockSize - len(z.currentBuffer)
		if n > len(p) {
			n = len(p)
		}
		z.digest.Write(p[:n])
		z.currentBuffer = append(z.currentBuffer, p[:n]...)
		z.size += n
		total += n
		p = p[n:]
		if len(z.currentBuffer) == z.limits.BlockSize {
			if err := z.submitBlock(ctx, false); err != nil {
				return total, err
			}
		}
	}
	return total, z.checkError()
}

// submitBlock hands the current buffer to the compression pipeline. It
// blocks until block and byte permits are available, the caller's context
// is canceled, or the generation is aborted.
func (z *LimitedWriter) submitBlock(ctx context.Context, final bool) error {
	g := z.gen.Load()
	data := z.currentBuffer
	size := len(data)

	// Limit concurrent + queued blocks.
	select {
	case g.blockSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-g.ctx.Done():
		return z.genError()
	}
	// Limit total pending uncompressed bytes.
	if err := g.byteSem.acquire(size, ctx, g.ctx.Done()); err != nil {
		<-g.blockSem
		if errors.Is(err, errGenDone) {
			return z.genError()
		}
		return err
	}

	// Chain the dictionary tail from the previous block, exactly like Writer.
	tail := z.prevTail
	if size > tailSize {
		buf := z.dstPool.Get().([]byte) // Put in compressBlockData/recycleBlock
		buf = append(buf[:0], data[size-tailSize:]...)
		z.prevTail = buf
	} else {
		z.prevTail = nil
	}

	seq := int(atomic.AddInt64(&g.submitted, 1) - 1)
	atomic.AddInt64(&z.submittedBlocks, 1)
	atomic.AddInt64(&z.pendingBlocks, 1)
	atomic.AddInt64(&z.pendingBytes, int64(size))
	// Never blocks: a held block permit guarantees a free slot.
	g.jobs <- &limitedBlock{seq: seq, data: data, tail: tail, final: final}

	z.currentBuffer = z.dstPool.Get().([]byte)[:0]
	return nil
}

// genWorker compresses blocks until the generation is canceled.
func (z *LimitedWriter) genWorker(g *limitedGen) {
	defer g.wg.Done()
	for {
		select {
		case b := <-g.jobs:
			atomic.AddInt64(&z.startedBlocks, 1)
			r := &limitedResult{seq: b.seq, size: len(b.data)}
			if g.ctx.Err() == nil {
				r.buf = z.compressBlockData(b)
			} else {
				z.recycleBlock(b)
			}
			// Never blocks: capacity matches the number of block permits.
			g.res <- r
		case <-g.ctx.Done():
			return
		}
	}
}

// compressBlockData compresses a single block and returns the compressed
// bytes, or nil if compression failed (the error is pushed).
func (z *LimitedWriter) compressBlockData(b *limitedBlock) []byte {
	buf := z.dstPool.Get().([]byte) // Put in handleResult/teardownGen
	dest := bytes.NewBuffer(buf[:0])

	compressor := z.dictFlatePool.Get().(*flate.Writer) // Put below
	compressor.ResetDict(dest, b.tail)
	compressor.Write(b.data)
	z.dstPool.Put(b.data) // Get in Write/submitBlock
	b.data = nil

	if err := compressor.Flush(); err != nil {
		z.dictFlatePool.Put(compressor)
		if b.tail != nil {
			z.dstPool.Put(b.tail)
		}
		z.pushError(err)
		return nil
	}
	if b.final {
		if err := compressor.Close(); err != nil {
			z.dictFlatePool.Put(compressor)
			if b.tail != nil {
				z.dstPool.Put(b.tail)
			}
			z.pushError(err)
			return nil
		}
	}
	z.dictFlatePool.Put(compressor)
	if b.tail != nil {
		z.dstPool.Put(b.tail) // Get in submitBlock
	}
	return dest.Bytes()
}

// recycleBlock returns an uncompressed block's buffers to the pool.
func (z *LimitedWriter) recycleBlock(b *limitedBlock) {
	if b.data != nil {
		z.dstPool.Put(b.data)
		b.data = nil
	}
	if b.tail != nil {
		z.dstPool.Put(b.tail)
		b.tail = nil
	}
}

// genWriter writes results to the output in strict sequence order. A block
// is either written in full or not at all; once the generation is canceled
// or an error was pushed, nothing more is written to the stream.
func (z *LimitedWriter) genWriter(g *limitedGen) {
	defer g.wg.Done()
	pending := make(map[int]*limitedResult)
	next := 0
	for {
		if r, ok := pending[next]; ok {
			delete(pending, next)
			next++
			z.handleResult(g, r)
			continue
		}
		select {
		case r := <-g.res:
			pending[r.seq] = r
		case <-g.ctx.Done():
			// Discard everything already received; the rest is
			// drained by teardownGen.
			for _, r := range pending {
				z.handleResult(g, r)
			}
			return
		}
	}
}

// handleResult writes a single block result to the output, or discards it
// if the generation has failed or been canceled. It releases the block's
// permits and records progress in all cases.
func (z *LimitedWriter) handleResult(g *limitedGen, r *limitedResult) {
	defer func() {
		g.byteSem.release(r.size)
		<-g.blockSem
		atomic.AddInt64(&z.pendingBlocks, -1)
		atomic.AddInt64(&z.pendingBytes, -int64(r.size))
		atomic.AddInt64(&g.handled, 1)
		g.progMu.Lock()
		close(g.progCh)
		g.progCh = make(chan struct{})
		g.progMu.Unlock()
	}()
	if r.buf == nil {
		// Compression failed or was skipped; the error was pushed.
		atomic.AddInt64(&z.canceledBlocks, 1)
		return
	}
	if g.ctx.Err() != nil || z.checkError() != nil {
		z.dstPool.Put(r.buf)
		atomic.AddInt64(&z.canceledBlocks, 1)
		return
	}
	err := writeFull(z.w, r.buf)
	z.dstPool.Put(r.buf)
	if err != nil {
		atomic.AddInt64(&z.canceledBlocks, 1)
		z.pushError(err)
		return
	}
	atomic.AddInt64(&z.writtenBlocks, 1)
	atomic.AddInt64(&z.writtenBytes, int64(len(r.buf)))
}

// waitProgress blocks until at least target blocks of the generation have
// been resolved, the caller's context is done, or the generation aborts.
func (z *LimitedWriter) waitProgress(ctx context.Context, g *limitedGen, target int64) error {
	for {
		// Read the signal channel first, then re-check the counters:
		// handleResult increments handled before swapping progCh, so if
		// the swap already happened the updated handled is visible here.
		g.progMu.Lock()
		ch := g.progCh
		g.progMu.Unlock()
		if atomic.LoadInt64(&g.handled) >= target {
			return nil
		}
		if err := z.checkError(); err != nil {
			return err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-g.ctx.Done():
			return z.genError()
		}
	}
}

// Flush flushes any pending compressed data to the underlying writer.
//
// Flush submits the currently buffered data as a block and waits until all
// previously accepted blocks have been written to the output. It does not
// close the stream; more data may be written afterwards. If the underlying
// writer returns an error, Flush returns that error.
func (z *LimitedWriter) Flush() error {
	if err := z.checkError(); err != nil {
		return err
	}
	if z.closed {
		return nil
	}
	if z.gen.Load() == nil {
		if err := z.startGen(); err != nil {
			return err
		}
	}
	g := z.gen.Load()
	if err := z.submitBlock(context.Background(), false); err != nil {
		return err
	}
	if err := z.waitProgress(context.Background(), g, atomic.LoadInt64(&g.submitted)); err != nil {
		return err
	}
	return z.checkError()
}

// Close closes the LimitedWriter, flushing any unwritten data to the
// underlying io.Writer and writing the GZIP footer. It does not close the
// underlying io.Writer.
//
// The current generation is always drained before Close returns, even if
// the stream has failed, so no goroutines or buffers are leaked and the
// statistics remain accurate.
func (z *LimitedWriter) Close() error {
	if z.closed {
		return z.checkError()
	}
	z.closed = true
	if z.gen.Load() == nil {
		if err := z.startGen(); err != nil {
			return err
		}
	}
	g := z.gen.Load()
	// Submit the final block; on an aborted generation this returns an
	// error which is reported below after cleanup.
	if err := z.submitBlock(context.Background(), true); err == nil {
		z.waitProgress(context.Background(), g, atomic.LoadInt64(&g.submitted))
	}
	z.teardownGen(g)
	z.gen.Store(nil)
	if z.currentBuffer != nil {
		z.dstPool.Put(z.currentBuffer)
		z.currentBuffer = nil
	}
	if z.prevTail != nil {
		z.dstPool.Put(z.prevTail)
		z.prevTail = nil
	}
	if err := z.checkError(); err != nil {
		return err
	}
	put4(z.buf[0:4], z.digest.Sum32())
	put4(z.buf[4:8], uint32(z.size))
	if err := writeFull(z.w, z.buf[0:8]); err != nil {
		z.pushError(err)
		return err
	}
	return nil
}

// Reset discards the LimitedWriter's state and makes it equivalent to the
// result of NewLimitedWriter with the same level and limits, but writing to
// w instead. Any in-flight generation is canceled and fully drained before
// resources are reused, so nothing is written to the old stream afterwards.
func (z *LimitedWriter) Reset(w io.Writer) {
	if g := z.gen.Load(); g != nil {
		z.teardownGen(g)
		z.gen.Store(nil)
	}
	z.errMu.Lock()
	z.err = nil
	z.errMu.Unlock()
	z.w = w
	z.closed = false
	z.wroteHeader = false
	z.size = 0
	z.digest.Reset()
	z.Header = Header{OS: 255}
	z.buf = [10]byte{}
	if z.currentBuffer != nil {
		z.dstPool.Put(z.currentBuffer)
		z.currentBuffer = nil
	}
	if z.prevTail != nil {
		z.dstPool.Put(z.prevTail)
		z.prevTail = nil
	}
}

// UncompressedSize returns the number of bytes accepted by the current
// stream.
func (z *LimitedWriter) UncompressedSize() int {
	return z.size
}

// writeFull writes all of b, looping on short writes, and returns the first
// error encountered.
func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
