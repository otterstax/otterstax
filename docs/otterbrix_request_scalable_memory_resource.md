# One shared `synchronized_pool_resource` serializes concurrent statements: allocation-heavy plans run at a fraction of the CPU

**Version:** otterbrix 1.0.0b2-rc-3 (recipe revision `87ab253cd44870f2caf7c739e3e85ae6`).

## Summary

An otterbrix instance allocates everything — every vector, validity mask, string and
operator state of every statement — from one memory resource,
`base_otterbrix_t::resource` (`integration/cpp/base_spaces.hpp:85`). Outside sanitizer
builds that resource is a single `std::pmr::synchronized_pool_resource` (`core/pmr.hpp:26`).

`synchronized_pool_resource` guards its pools with one reader/writer lock. Plans that run on
several threads at once all take that lock for their allocations and frees, so:

- an allocation-heavy statement (a JOIN over tens of thousands of rows) does not scale with
  threads;
- the process stays at a small fraction of the cores while the other threads wait on the
  lock;
- a better `malloc` underneath (jemalloc, mimalloc) does not help, because the contended
  lock is the pool's own, above `malloc`.

## Reproduction

The workload is eight client sessions looping one JOIN, each side 60 000 rows, with a
one-row answer (OtterStax, which embeds otterbrix through `wrapper_dispatcher_t`):

```sql
SELECT count(*) FROM <a> JOIN <b> ON a.id = b.id   -- 60 000 ⋈ 60 000, BIGINT keys
```

**Where the time goes.** Under that load the process used about 1.7 of 14 available cores,
and throughput stayed flat however many sessions were added. A sample of every thread's
stack (`gdb`, 12 snapshots) shows the threads that are neither idle nor computing waiting
here:

```text
__pthread_rwlock_wrlock_full64
___pthread_rwlock_wrlock
std::pmr::synchronized_pool_resource::do_allocate(unsigned long, unsigned long)
components::vector::validity_data_t::validity_data_t(std::pmr::memory_resource*, unsigned long*, unsigned long)
components::vector::validity_mask_t::operator=(components::vector::validity_mask_t const&)
components::vector::vector_t::slice(components::vector::vector_t const&, unsigned long, unsigned long)
...
```

| Blocked in `synchronized_pool_resource` (samples) | Allocating from |
|---|---|
| 26 | `services::collection::executor` running the JOIN |
| 23 | the embedding server, building input chunks on the same resource |

They are reached from `vector_buffer_t::vector_buffer_t`, `vector_buffer_t::~vector_buffer_t`,
`validity_data_t::validity_data_t` and `logical_value_t` string construction: the per-chunk
allocations of every operator.

**One statement at a time is faster than several at once.** With the plans serialized (one
statement in the engine at a time) the same eight sessions complete about 15 statements/s.
Run concurrently, they complete about 8/s at the same CPU. Contention on the lock costs more
than the extra threads gain.

**Moving the embedder's own allocations off the shared resource multiplies throughput.** The
embedder decodes its input rows and builds chunks; giving that work `new_delete_resource()`
instead of the engine's resource took the same load from about 8 to about 55 statements/s.
The JOIN itself, still on the engine's resource, then allocated without competing with the
decode.

**A different `malloc` alone changes nothing.** Preloading jemalloc or mimalloc under the
unchanged server left throughput at about 8/s.

## Allocator measurements

The same load (eight sessions, the 60k ⋈ 60k JOIN as "wide"), plus a small JOIN (5 000 rows
per side, string columns) as "small", two passes per image:

- "today": OtterStax builds its input chunks on `dispatcher()->resource()`.
- "decode off": OtterStax decodes and builds chunks on `new_delete_resource()`.
- jemalloc 5.2 and mimalloc 2.0 were preloaded (`LD_PRELOAD`) under an unchanged binary.

| Image | Wide throughput (8 sessions) | Wide ×8 | Small ×8 |
|---|---|---|---|
| today, glibc | 7.9–8.1/s | 858–951 ms | 154–177 ms |
| today + jemalloc / + mimalloc | 7.4–7.9/s | 916–992 ms | 180–208 ms |
| decode off, glibc | 52–55/s | 177–182 ms | 183–256 ms |
| decode off + jemalloc | 62–64/s | 169–177 ms | 193–210 ms |
| decode off + mimalloc | 39–41/s | 211–216 ms | 143–147 ms |
| one statement at a time in the engine (glibc) | 13.7–15.4/s | 595 ms | 156 ms |

A light statement (an aggregate answered by a remote backend, 31–48 ms at ×8) and every
single-session time are unchanged across all of them.

What the table shows:
- **Swapping `malloc` alone does nothing.** With the embedder still allocating from the
  engine's pool, both libraries leave throughput at about 7.5/s. The contended lock is the
  `synchronized_pool_resource`'s, which sits above `malloc`.
- **Once the embedder's traffic is off the shared pool, the `malloc` matters.**
  - mimalloc is the only setup where nothing regresses. The small JOIN drops to 143–147 ms,
    and the wide JOIN gets about 5× today's throughput. That fits mimalloc's cheap
    cross-thread frees: chunks are built on one thread and freed on another.
  - jemalloc is fastest on the wide JOIN (about 8× today), but the small JOIN stays 10–30%
    above today.
- **The small ×8 figures are noisy,** with ranges of 20–70 ms between passes; only the
  mimalloc row is consistently good.
- **The engine's own allocations stay on the shared pool in every row,** so concurrent plans
  still contend with each other inside the engine.

## Root cause

1. `core/pmr.hpp:26` makes `otterbrix_resource` a `std::pmr::synchronized_pool_resource`.
2. `base_otterbrix_t` owns exactly one, `resource` (`integration/cpp/base_spaces.hpp:85`).
   `dispatcher()->resource()` hands it to the embedder, and every manager, executor and
   operator allocates from it.
3. libstdc++'s `synchronized_pool_resource` takes its `shared_mutex` exclusively whenever a
   thread's pools need replenishing, and for every allocation larger than its largest pool
   (which goes upstream under the lock). Concurrent plans allocating many chunks hit the
   exclusive path continuously.

The engine already replaces the pool where it gets in the way: under TSAN with a
`new_delete_resource()` wrapper (`tsan_resource_t`, `base_spaces.hpp:74`), and under ASAN
with `resource_tracer_t` (`core/pmr.hpp:24`).

## Impact

- Any embedder that runs statements concurrently pays this cost on allocation-heavy plans:
  JOINs, GROUP BY over wide inputs, large inserts.
- The process cannot use more than about two cores' worth of work on such plans, regardless
  of the hardware.
- Embedders that share the resource for their own work (decoding input, building chunks,
  as `dispatcher()->resource()` invites) make it worse, and have no way to give the engine
  a different resource.

## Request

Any of these would remove the cap; they are listed from least to most work.

1. **Let the embedder choose the resource.** For example, a `base_otterbrix_t` constructor
   that takes a `std::pmr::memory_resource*` (or a factory), defaulting to today's pool. An
   embedder can then pass `new_delete_resource()` over a thread-caching `malloc`
   (mimalloc, jemalloc), or its own per-thread scheme. This is the same substitution the
   engine already makes under TSAN.
2. **Use a scalable default.** For example, `new_delete_resource()` and document that the
   process should link a thread-caching `malloc`. Or per-thread
   `unsynchronized_pool_resource`s, with cross-thread frees handled.
3. **Per-statement or per-operator arenas** for the data a plan builds and drops together:
   a `monotonic_buffer_resource` per pipeline, released when the statement ends. That
   removes most per-object allocation and free traffic altogether.

## What OtterStax does in the meantime

OtterStax has moved its own work off the shared pool:
- backend rows are decoded and their chunks built on a resource of its own (the "data
  resource", `ComponentManager`'s customization point, `new_delete_resource()` by default)
  instead of `dispatcher()->resource()`, which takes that traffic off the engine's lock;
- the connection pools — their request state, the per-request wait ticket, the generated SQL
  text and the transfer requests — live on the same data resource, not on the engine's pool,
  since many Workers' statements allocate there at once;
- the decoders write a STRING cell straight into the vector (`vector_t::set_value(row,
  std::string_view)`), which bypasses `logical_value_t` and its heap string (below).

The engine's own allocations stay on the shared pool, so concurrent JOINs inside the engine
still contend with each other. That part needs one of the changes above.

## Related: allocations a string value makes, and an allocator a type keeps

Two further engine behaviours bear on where memory comes from; neither needs the pool change,
both would ideally be covered by the same "the embedder's resource is used" rule.

- **A string `logical_value_t` allocates on the global heap, whatever resource it is given.**
  Its constructors from `std::string`, `std::pmr::string` and `std::string_view`
  (`components/types/logical_value.hpp`) store the text as `heap_new<std::string>(...)`: one
  heap block for the `std::string` object and one for its buffer once the text is past the
  short-string size. The `resource` argument is kept but not used for the text. Every string a
  translator or a plan parameter builds through a value object therefore costs two global
  allocations before the vector copies the text into its own string buffer. OtterStax writes its
  decoded STRING cells through the typed `vector_t::set_value(row, std::string_view)` instead,
  which copies once into the vector's buffer on the chunk's resource; nested values (a STRUCT
  field, a LIST element) still have to go through a value object. Asked: build the text on the
  value's `resource` (a `std::pmr::string`), or let `set_value` of a nested value take views.
- **A STRUCT type keeps the allocator of the field vector it was built from.**
  `struct_logical_type_extension` copies `fields` with `fields.get_allocator()`
  (`components/types/types.cpp`, `fields_(fields, fields.get_allocator())`), so the type points
  into whatever resource the caller used — a scratch arena the caller releases leaves the type
  dangling. OtterStax keeps every vector it hands `create_struct` on a long-lived resource for
  that reason (`utility/stack_arena.hpp` documents it). Asked: copy with the type's own
  resource, or document that the fields' resource must outlive the type.
