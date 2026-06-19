# How Pebble Uses Memory Arenas

This document describes how [Pebble](https://github.com/cockroachdb/pebble)
(CockroachDB's storage engine) uses memory arenas and related allocation
amortization techniques. It was written while evaluating whether the same ideas
apply to DRPC's encode/decode paths; the DRPC-specific takeaways are at the end.

All file/line references are to the Pebble tree at
`github.com/cockroachdb/pebble` as of mid-2026.

---

## 1. Core idea

An **arena** is a single large, pre-allocated block of memory from which many
small objects are sub-allocated by simply bumping an offset. Individual objects
are never freed; instead the *entire* arena is freed (or reset) at once when all
of its objects are known to be dead simultaneously.

This trades flexibility for speed and density:

- **Allocation is a pointer/offset bump**, not a trip through the general
  allocator. In Pebble's skiplist arena it is a single atomic add, making it
  lock-free.
- **No per-object free** — there is no free list, no fragmentation bookkeeping,
  and no GC pressure from millions of tiny objects.
- **References are offsets, not pointers.** Pebble's skiplist arena addresses
  objects by a `uint32` offset into the backing buffer rather than a Go pointer.
  This halves pointer size on 64-bit platforms and, critically, means the
  backing buffer contains **no Go pointers**, so the garbage collector never has
  to scan it.

The defining constraint: **arenas only work when the contained objects share a
lifetime.** You must be able to say "all of these are dead now" and drop the
whole block. If objects have independent, staggered lifetimes, an arena is the
wrong tool (you would either leak or be unable to free anything).

Pebble actually uses a *family* of allocation-amortizing tools, from a true
offset arena to simpler chunk/pool allocators. They are described below in order
of "most arena-like" to "least."

---

## 2. The skiplist arena (`internal/arenaskl`)

This is the canonical "arena" in Pebble: the backing store for the memtable's
lock-free skiplist.

### Structure

```go
// internal/arenaskl/arena.go
type Arena struct {
    n   atomic.Uint64 // bump pointer: next free offset
    buf []byte        // fixed backing store
}

const nodeAlignment = 4

func NewArena(buf []byte) *Arena {
    a := &Arena{buf: buf}
    // Offset 0 is reserved as a "nil" sentinel, so allocation starts at 1.
    a.n.Store(1)
    return a
}
```

The arena is **fixed-size** — it never grows. `buf` is supplied at construction.

### Allocation: a single atomic add

```go
// internal/arenaskl/arena.go
func (a *Arena) alloc(size, alignment, overflow uint32) (uint32, error) {
    origSize := a.n.Load()
    if int(origSize) > len(a.buf) {
        return 0, ErrArenaFull
    }
    padded := uint64(size) + uint64(alignment) - 1
    newSize := a.n.Add(padded)           // <-- lock-free bump
    if newSize+uint64(overflow) > uint64(len(a.buf)) {
        return 0, ErrArenaFull           // <-- arena is full
    }
    offset := (uint32(newSize) - size) & ^(alignment - 1) // align down
    return offset, nil
}
```

Multiple goroutines can allocate concurrently with no lock — the `atomic.Add`
serializes the bump pointer. When the buffer is exhausted, `alloc` returns
`ErrArenaFull` rather than growing.

### Offsets instead of pointers

Objects are referenced by offset and converted to/from pointers only on access:

```go
// internal/arenaskl/arena.go
func (a *Arena) getBytes(offset, size uint32) []byte {
    if offset == 0 { return nil }            // 0 == nil sentinel
    return a.buf[offset : offset+size : offset+size]
}

func (a *Arena) getPointer(offset uint32) unsafe.Pointer {
    if offset == 0 { return nil }
    return unsafe.Pointer(&a.buf[offset])
}
```

A skiplist node stores its key, value, and tower links inline in the arena, and
its forward/back links are `atomic.Uint32` **offsets**, not pointers:

```go
// internal/arenaskl/node.go
type links struct {
    nextOffset atomic.Uint32
    prevOffset atomic.Uint32
}

func newRawNode(arena *Arena, height, keySize, valueSize uint32) (*node, error) {
    // Truncate the tower to the node's actual height to save space — unused
    // upper tower levels are never allocated.
    unusedSize := uint32((maxHeight - int(height)) * linksSize)
    nodeSize := uint32(maxNodeSize) - unusedSize

    nodeOffset, err := arena.alloc(nodeSize+keySize+valueSize, nodeAlignment, unusedSize)
    if err != nil {
        return nil, err
    }
    nd := (*node)(arena.getPointer(nodeOffset))
    nd.keyOffset = nodeOffset + nodeSize   // key/value live right after the node
    nd.keySize = keySize
    nd.valueSize = valueSize
    return nd, nil
}
```

Because the whole node — header, links, key bytes, value bytes — lives in one
contiguous arena slot addressed by offsets, the GC sees the memtable as a single
`[]byte` with zero internal pointers, regardless of how many millions of entries
it holds.

---

## 3. When Pebble uses the arena

The skiplist arena backs the **memtable** (and its companion range-del / range-key
skiplists). The lifecycle maps perfectly onto the arena constraint:

1. A new memtable allocates one fixed-size arena buffer.
2. Every `Set`/`Merge`/`Delete` writes a skiplist node into the arena.
3. When the arena fills up (`ErrArenaFull`), the memtable is marked immutable and
   the engine rotates to a fresh memtable.
4. The immutable memtable is flushed to an SSTable on disk.
5. Once flushed, **every node dies at the same instant** — the entire arena
   buffer is released in one operation.

```go
// mem_table.go — the arena's backing buffer is a single manual allocation
func (m *memTable) init(opts memTableOptions) {
    ...
    if m.arenaBuf.Data() == nil {
        m.arenaBuf = manual.New(manual.MemTable, uintptr(opts.size)) // one big block
    }
    arena := arenaskl.NewArena(m.arenaBuf.Slice())
    m.skl.Reset(arena, m.cmp)         // point + range skiplists share one arena
    m.rangeDelSkl.Reset(arena, m.cmp)
    m.rangeKeySkl.Reset(arena, m.cmp)
    m.reserved = arena.Size()
}

// freed wholesale once the memtable has been flushed
func (m *memTable) free() {
    manual.Free(manual.MemTable, m.arenaBuf)
    m.arenaBuf = manual.Buf{}
}
```

`ErrArenaFull` is the signal that drives memtable rotation:

```go
// mem_table.go
func (m *memTable) prepare(batch *Batch) error {
    ...
    if m.reserved > m.totalBytes() {
        return arenaskl.ErrArenaFull
    }
    ...
}
// db.go: callers treat ErrArenaFull as "switch to a new memtable", not an error
```

This is the textbook arena use case: a huge number of small, immutable objects
(skiplist nodes) created over a bounded window, all becoming garbage at the same
moment (flush).

---

## 4. How it benefits Pebble

- **GC pressure elimination.** A memtable can hold millions of entries. As
  individual Go objects that would be millions of GC-scanned allocations. As one
  pointer-free `[]byte` arena, the GC effectively ignores it. This is the single
  biggest win.
- **Allocation speed.** Each node allocation is one atomic add — no allocator
  lock, no size-class lookup, no zeroing of a fresh object.
- **Lock-free concurrency.** Because allocation is just `atomic.Add` on the bump
  pointer and links are CAS'd `uint32` offsets, many writers insert into the
  skiplist concurrently without a mutex.
- **Memory density / accounting.** Nodes are packed contiguously with their keys
  and values, and the tower is truncated to actual height. A memtable's memory
  footprint is fixed and known up front (the arena size), which makes write
  back-pressure and memory budgeting precise.
- **Cheap bulk free.** Reclaiming a flushed memtable is a single `free()` of the
  backing buffer instead of waiting for the GC to collect millions of objects.

---

## 5. Related allocation tools in Pebble

Pebble layers several non-arena allocators that share the "amortize many small
allocations" goal. They are worth knowing because they are *more* applicable than
the skiplist arena to systems with staggered object lifetimes.

### 5.1 `internal/manual` — off-heap, purpose-tagged blocks

The arena's backing buffer (and block-cache memory) is allocated off the Go heap
via C `malloc`/`free`, tagged by purpose for accounting:

```go
// internal/manual/manual.go
type Purpose uint8
const ( _ Purpose = iota; BlockCacheMap; BlockCacheEntry; BlockCacheData; MemTable; NumPurposes )

// internal/manual/manual_cgo.go
func New(purpose Purpose, n uintptr) Buf { /* C.calloc, tracked per purpose */ }
func Free(purpose Purpose, b Buf)        { /* C.free, tracked per purpose */ }
```

Keeping large, long-lived buffers off the Go heap further reduces GC scan work
and gives Pebble exact per-purpose memory metrics (`GetMetrics()`).

### 5.2 `internal/rawalloc` — un-zeroed Go allocation

```go
// internal/rawalloc/rawalloc.go
// New returns a []byte whose backing memory is UNINITIALIZED (skips zeroing).
func New(len, cap int) []byte {
    ptr := mallocgc(uintptr(cap), nil, false) // needzero=false
    return unsafe.Slice((*byte)(ptr), cap)[:len]
}
```

Used when the caller will immediately overwrite the buffer, avoiding the implicit
zeroing that `make([]byte, n)` performs.

### 5.3 `internal/bytealloc` — chunk allocator with reset

The closest analogue to an arena for *staggered-but-batched* lifetimes. Many
small `[]byte`s are carved from progressively larger chunks; the whole thing is
reset and reused once the batch is done.

```go
// internal/bytealloc/bytealloc.go
type A []byte // cap() = total memory, len() = amount handed out

func (a A) Alloc(n int) (A, []byte) {
    if cap(a)-len(a) < n {
        a = a.reserve(n) // grows exponentially up to 512 KB, via rawalloc.New
    }
    p := len(a)
    return a[:p+n], a[p : p+n : p+n]
}

func (a *A) Reset() { *a = (*a)[:0] } // reuse the chunk for the next batch
```

### 5.4 `sstable/block.TempBuffer` — pooled scratch for (de)compression

A `sync.Pool` of reusable buffers used to hold compressed/decompressed block
data, with a cap on retained size to avoid pinning occasional huge buffers:

```go
// sstable/block/physical.go
type TempBuffer struct{ b []byte }

var tempBufferPool = sync.Pool{
    New: func() any { return &TempBuffer{b: make([]byte, 0, tempBufferInitialSize)} },
}
const tempBufferInitialSize   = 32 * 1024
const tempBufferMaxReusedSize = 256 * 1024

func (tb *TempBuffer) Release() {
    // Don't return oversized buffers to the pool — let them be GC'd.
    if tb.b != nil && len(tb.b) < tempBufferMaxReusedSize {
        tb.b = tb.b[:0]
        tempBufferPool.Put(tb)
    }
}
```

Compressors are written to *append into a caller-supplied destination*, so the
pooled `TempBuffer` is reused across operations rather than reallocated:

```go
// internal/compression/compression.go (interface shape)
type Compressor interface {
    Compress(dst, src []byte) ([]byte, Setting) // appends into dst
    Close()
}
type Decompressor interface {
    DecompressInto(buf, compressed []byte) error // into preallocated buf
    DecompressedLen(b []byte) (int, error)
    Close()
}
```

---

## 6. Choosing the right tool (summary)

| Tool | Lifetime pattern | Frees | Concurrency | Pebble use |
|------|------------------|-------|-------------|------------|
| `arenaskl.Arena` | all objects die together | whole arena at once | lock-free (atomic bump + CAS offsets) | memtable skiplist nodes |
| `manual` | long-lived large blocks | explicit `Free`, per-purpose | n/a | arena backing buffer, block cache |
| `rawalloc` | immediately overwritten | GC | n/a | backing for `bytealloc` |
| `bytealloc.A` | many small, same batch | `Reset` + reuse | single-owner | batch/iter scratch |
| `TempBuffer` pool | transient per-op scratch | back to `sync.Pool` | pool-safe | block (de)compression |

The decision tree: **all dead at once → arena; same-batch → bytealloc; transient
per-op → sync.Pool/TempBuffer; long-lived large → manual.**

---

## 7. Relevance to DRPC

DRPC message buffers do **not** match the skiplist-arena lifetime model: messages
arrive and are consumed individually, with staggered (overlapping) lifetimes —
exactly the case an offset arena cannot serve. So `arenaskl` is the *wrong*
borrow.

The applicable Pebble ideas are the lighter-weight ones:

- **`sync.Pool` of reusable `[]byte`** (cf. `TempBuffer`) — DRPC already does this
  with `drpcstream.BufferPool` behind the receive ring buffer. The
  `drpc-alloc-opt` work extended it to the `Invoke` marshal buffer and to the
  packet assembler (eliminating a second receive-side copy via buffer-ownership
  transfer).
- **Append-into-`dst` + capped-size pool return** (cf. `TempBuffer.Release`'s
  `tempBufferMaxReusedSize` guard and the `Compressor.Compress(dst, src)` shape)
  — the pattern to adopt when DRPC compression lands: compress/decompress into a
  pooled scratch buffer, and don't return oversized buffers to the pool.
- **`bytealloc.A`-style chunk allocation** — useful only if a future code path
  accumulates many small same-lifetime allocations that are released together.

In short: borrow Pebble's *buffer-reuse discipline*, not its skiplist arena.
