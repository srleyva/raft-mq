// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/rangedel"
)

func memTableEntrySize(keyBytes, valueBytes int) uint32 {
	return arenaskl.MaxNodeSize(uint32(keyBytes)+8, uint32(valueBytes))
}

// A memTable implements an in-memory layer of the LSM. A memTable is mutable,
// but append-only. Records are added, but never removed. Deletion is supported
// via tombstones, but it is up to higher level code (see Iterator) to support
// processing those tombstones.
//
// A memTable is implemented on top of a lock-free arena-backed skiplist. An
// arena is a fixed size contiguous chunk of memory (see
// Options.MemTableSize). A memTable's memory consumtion is thus fixed at
// the time of creation (with the exception of the cached fragmented range
// tombstones). The arena-backed skiplist provides both forward and reverse
// links which makes forward and reverse iteration the same speed.
//
// A batch is "applied" to a memTable in a two step process: prepare(batch) ->
// apply(batch). memTable.prepare() is not thread-safe and must be called with
// external sychronization. Preparation reserves space in the memTable for the
// batch. Note that we pessimistically compute how much space a batch will
// consume in the memTable (see memTableEntrySize and
// Batch.memTableSize). Preparation is an O(1) operation. Applying a batch to
// the memTable can be performed concurrently with other apply
// operations. Applying a batch is an O(n logm) operation where N is the number
// of records in the batch and M is the number of records in the memtable. The
// commitPipeline serializes batch preparation, and allows batch application to
// proceed concurrently.
//
// It is safe to call get, apply, newIter, and newRangeDelIter concurrently.
type memTable struct {
	cmp         Compare
	equal       Equal
	skl         arenaskl.Skiplist
	rangeDelSkl arenaskl.Skiplist
	// emptySize is the amount of allocated space in the arena when the memtable
	// is empty.
	emptySize uint32
	// reserved tracks the amount of space used by the memtable, both by actual
	// data stored in the memtable as well as inflight batch commit
	// operations. This value is incremented pessimistically by prepare() in
	// order to account for the space needed by a batch.
	reserved uint32
	// readerRefs tracks the read references on the memtable. The two sources of
	// reader references are DB.mu.mem.queue and readState.memtables. The memory
	// reserved by the memtable in the cache is released when the reader refs
	// drop to zero. When the reader refs drops to zero, the writer refs will
	// already be zero because the memtable will have been flushed and that only
	// occurs once the writer refs drops to zero.
	readerRefs int32
	// writerRefs tracks the write references on the memtable. The two sources of
	// writer references are the memtable being on DB.mu.mem.queue and from
	// inflight mutations that have reserved space in the memtable but not yet
	// applied. The memtable cannot be flushed to disk until the writer refs
	// drops to zero.
	writerRefs int32
	flushedCh  chan struct{}
	// flushManual indicates whether a manual flush was forced on this memtable.
	flushManual bool
	tombstones  rangeTombstoneCache
	logNum      uint64
	logSize     uint64
	// Closure to invoke to release memory accounting bytes.
	releaseMemAccountingBytes func()
}

// newMemTable returns a new MemTable of the specified size. If size is zero,
// Options.MemTableSize is used instead.
func newMemTable(o *Options, size int, memAccountingBytes *int64) *memTable {
	o = o.EnsureDefaults()
	m := &memTable{
		cmp:        o.Comparer.Compare,
		equal:      o.Comparer.Equal,
		readerRefs: 1,
		writerRefs: 1,
		flushedCh:  make(chan struct{}),
	}

	if size == 0 {
		size = o.MemTableSize
	}

	if memAccountingBytes != nil {
		atomic.AddInt64(memAccountingBytes, int64(size))
		releaseAccountingReservation := o.Cache.Reserve(size)
		m.releaseMemAccountingBytes = func() {
			atomic.AddInt64(memAccountingBytes, -int64(size))
			releaseAccountingReservation()
		}
	}

	arena := arenaskl.NewArena(uint32(size), 0)
	m.skl.Reset(arena, m.cmp)
	m.rangeDelSkl.Reset(arena, m.cmp)
	m.emptySize = arena.Size()
	return m
}

func (m *memTable) readerRef() {
	switch v := atomic.AddInt32(&m.readerRefs, 1); {
	case v <= 1:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

func (m *memTable) readerUnref() {
	switch v := atomic.AddInt32(&m.readerRefs, -1); {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		if m.releaseMemAccountingBytes == nil {
			panic(fmt.Sprintf("pebble: memtable reservation already released"))
		}
		m.releaseMemAccountingBytes()
		m.releaseMemAccountingBytes = nil
	}
}

func (m *memTable) writerRef() {
	switch v := atomic.AddInt32(&m.writerRefs, 1); {
	case v <= 1:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

func (m *memTable) writerUnref() bool {
	switch v := atomic.AddInt32(&m.writerRefs, -1); {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		return true
	default:
		return false
	}
}

func (m *memTable) flushed() chan struct{} {
	return m.flushedCh
}

func (m *memTable) manualFlush() bool {
	return m.flushManual
}

func (m *memTable) readyForFlush() bool {
	return atomic.LoadInt32(&m.writerRefs) == 0
}

func (m *memTable) logInfo() (uint64, uint64) {
	return m.logNum, m.logSize
}

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
func (m *memTable) get(key []byte) (value []byte, err error) {
	it := m.skl.NewIter(nil, nil)
	ikey, val := it.SeekGE(key)
	if ikey == nil {
		return nil, ErrNotFound
	}
	if !m.equal(key, ikey.UserKey) {
		return nil, ErrNotFound
	}
	switch ikey.Kind() {
	case InternalKeyKindDelete, InternalKeyKindSingleDelete:
		return nil, ErrNotFound
	default:
		return val, nil
	}
}

// Prepare reserves space for the batch in the memtable and references the
// memtable preventing it from being flushed until the batch is applied. Note
// that prepare is not thread-safe, while apply is. The caller must call
// writerUnref() after the batch has been applied.
func (m *memTable) prepare(batch *Batch) error {
	avail := m.availBytes()
	if batch.memTableSize > avail {
		return arenaskl.ErrArenaFull
	}
	m.reserved += batch.memTableSize

	m.writerRef()
	return nil
}

func (m *memTable) apply(batch *Batch, seqNum uint64) error {
	var ins arenaskl.Inserter
	var tombstoneCount uint32
	startSeqNum := seqNum
	for r := batch.Reader(); ; seqNum++ {
		kind, ukey, value, ok := r.Next()
		if !ok {
			break
		}
		var err error
		ikey := base.MakeInternalKey(ukey, seqNum, kind)
		switch kind {
		case InternalKeyKindRangeDelete:
			err = m.rangeDelSkl.Add(ikey, value)
			tombstoneCount++
		case InternalKeyKindLogData:
			// Don't increment seqNum for LogData, since these are not applied
			// to the memtable.
			seqNum--
		default:
			err = ins.Add(&m.skl, ikey, value)
		}
		if err != nil {
			return err
		}
	}
	if seqNum != startSeqNum+uint64(batch.Count()) {
		panic(fmt.Errorf("pebble: inconsistent batch count: %d vs %d",
			seqNum, startSeqNum+uint64(batch.Count())))
	}
	if tombstoneCount != 0 {
		m.tombstones.invalidate(tombstoneCount)
	}
	return nil
}

// newIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (m *memTable) newIter(o *IterOptions) internalIterator {
	return m.skl.NewIter(o.GetLowerBound(), o.GetUpperBound())
}

func (m *memTable) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	return m.skl.NewFlushIter(bytesFlushed)
}

func (m *memTable) newRangeDelIter(*IterOptions) internalIterator {
	tombstones := m.tombstones.get(m)
	if tombstones == nil {
		return nil
	}
	return rangedel.NewIter(m.cmp, tombstones)
}

func (m *memTable) availBytes() uint32 {
	a := m.skl.Arena()
	if atomic.LoadInt32(&m.writerRefs) == 1 {
		// If there are no other concurrent apply operations, we can update the
		// reserved bytes setting to accurately reflect how many bytes of been
		// allocated vs the over-estimation present in memTableEntrySize.
		m.reserved = a.Size()
	}
	return a.Capacity() - m.reserved
}

func (m *memTable) inuseBytes() uint64 {
	return uint64(m.skl.Size() - m.emptySize)
}

func (m *memTable) totalBytes() uint64 {
	return uint64(m.skl.Arena().Capacity())
}

func (m *memTable) close() error {
	return nil
}

// empty returns whether the MemTable has no key/value pairs.
func (m *memTable) empty() bool {
	return m.skl.Size() == m.emptySize
}

// A rangeTombstoneFrags holds a set of fragmented range tombstones generated
// at a particular "sequence number" for a memtable. Rather than use actual
// sequence numbers, this cache uses a count of the number of range tombstones
// in the memTable. Note that the count of range tombstones in a memTable only
// ever increases, which provides a monotonically increasing sequence.
type rangeTombstoneFrags struct {
	count      uint32
	once       sync.Once
	tombstones []rangedel.Tombstone
}

// get retrieves the fragmented tombstones, populating them if necessary. Note
// that the populated tombstone fragments may be built from more than f.count
// memTable range tombstones, but that is ok for correctness. All we're
// requiring is that the memTable contains at least f.count range
// tombstones. This situation can occur if there are multiple concurrent
// additions of range tombstones and a concurrent reader. The reader can load a
// tombstoneFrags and populate it even though is has been invalidated
// (i.e. replaced with a newer tombstoneFrags).
func (f *rangeTombstoneFrags) get(m *memTable) []rangedel.Tombstone {
	f.once.Do(func() {
		frag := &rangedel.Fragmenter{
			Cmp: m.cmp,
			Emit: func(fragmented []rangedel.Tombstone) {
				f.tombstones = append(f.tombstones, fragmented...)
			},
		}
		it := m.rangeDelSkl.NewIter(nil, nil)
		for key, val := it.First(); key != nil; key, val = it.Next() {
			frag.Add(*key, val)
		}
		frag.Finish()
	})
	return f.tombstones
}

// A rangeTombstoneCache is used to cache a set of fragmented tombstones. The
// cache is invalidated whenever a tombstone is added to a memTable, and
// populated when empty when a range-del iterator is created.
type rangeTombstoneCache struct {
	count uint32
	frags unsafe.Pointer
}

// Invalidate the current set of cached tombstones, indicating the number of
// tombstones that were added.
func (c *rangeTombstoneCache) invalidate(count uint32) {
	newCount := atomic.AddUint32(&c.count, count)
	var frags *rangeTombstoneFrags

	for {
		oldPtr := atomic.LoadPointer(&c.frags)
		if oldPtr != nil {
			oldFrags := (*rangeTombstoneFrags)(oldPtr)
			if oldFrags.count >= newCount {
				// Someone else invalidated the cache before us and their invalidation
				// subsumes ours.
				break
			}
		}
		if frags == nil {
			frags = &rangeTombstoneFrags{count: newCount}
		}
		if atomic.CompareAndSwapPointer(&c.frags, oldPtr, unsafe.Pointer(frags)) {
			// We successfully invalidated the cache.
			break
		}
		// Someone else invalidated the cache. Loop and try again.
	}
}

func (c *rangeTombstoneCache) get(m *memTable) []rangedel.Tombstone {
	frags := (*rangeTombstoneFrags)(atomic.LoadPointer(&c.frags))
	if frags == nil {
		return nil
	}
	return frags.get(m)
}
