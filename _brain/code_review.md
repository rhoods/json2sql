[
  {
    "file": "src/db/ddl.rs",
    "line": 197,
    "summary": "Hardcoded `SET maintenance_work_mem = '1GB'` is fatal if it fails and causes OOM with high parallelism",
    "failure_scenario": "With parallel=8, up to 8 constraint connections each request 1 GB of sort memory simultaneously. On a server with <8 GB RAM, PostgreSQL OOM-kills its own backend mid-Phase D after all data is already loaded. The error propagates via `?` (line 219) through `try_join_all` and aborts `add_constraints` entirely, leaving tables with data but no primary keys and no clean way to recover without re-importing."
  },
  {
    "file": "src/db/ddl.rs",
    "line": 198,
    "summary": "`SET synchronous_commit = off` is now applied to constraint connections — new durability regression for Phase D",
    "failure_scenario": "Previously `apply_constraints_chunk` had no session SETs. Now `ALTER TABLE ADD PRIMARY KEY` commits may not be WAL-flushed before `Ok` is returned. A server crash in the ~200 ms asynchronous commit window silently rolls back the constraint creation; the tool has already returned success and there is no detection or re-runability guard. This is new exposure introduced by this commit — Phase A/B COPY connections already had this risk, but Phase D constraints did not."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 332,
    "summary": "`distribute_sinks` weights tables by `row_count` instead of `bytes_buffered`, defeating load-balancing for wide-row tables",
    "failure_scenario": "`bytes_buffered` is the established size signal — `trigger_budget_flush` (line 155) uses it as the threshold for interim COPYs. A root table with 1,000 JSON-blob rows (100 MB of COPY data, `bytes_buffered=100MB`, `row_count=1000`) is treated as lighter than a narrow child with 500,000 rows (10 MB, `row_count=500000`). Bin-packing stacks the heavy 100 MB table with other tables on one connection while the nominal weight signals it as light, making Phase B wall time again dominated by a single overloaded connection — the exact problem the greedy algorithm was meant to fix."
  },
  {
    "file": "src/db/copy_sink.rs",
    "line": 146,
    "summary": "`cleanup_spill_file` silently swallows deletion errors with no log output, allowing orphaned temp files to accumulate invisibly",
    "failure_scenario": "An EACCES, read-only tmpfs mount, or path-change bug causes `remove_file` to fail on every call throughout an entire 70 GB run. The operator sees nothing — no warning, no error. This matches the documented observation in `_brain/performance_issue.md`: 109 GB of temp files remaining while the source file was only 70 GB. Adding `eprintln!` on failure (the project's only logging mechanism) would surface this in one line and uses the established pattern already present throughout the codebase."
  },
  {
    "file": "src/db/copy_sink.rs",
    "line": 689,
    "summary": "Test `stream_file_reads_all_bytes_in_chunks` tests the removed `vec![0u8]`-based read loop, not the live `BytesMut::read_buf` implementation",
    "failure_scenario": "The test at line 689 manually reimplements the old `file.read(&mut buf)` + fixed-vec pattern that was deleted from `stream_file_chunks`. It does not call `stream_file_chunks`. A regression in the new `BytesMut::read_buf`-based implementation (wrong EOF detection, partial-read handling, etc.) would not be caught by this test — it passes regardless of how `stream_file_chunks` behaves. The live implementation is tested separately at line 884, but the dead test gives false confidence in coverage."
  },
  {
    "file": "src/db/copy_sink.rs",
    "line": 364,
    "summary": "`stream_file_chunks` allocates a fresh 4 MiB `BytesMut` inside the loop on every iteration instead of reusing one buffer",
    "failure_scenario": "A 70 GB spill file requires ~17,500 iterations at 4 MB/read; `buf.freeze()` transfers ownership, preventing reuse, so a new 4 MB allocation is made each time. Under memory pressure or with OS short-reads (common from cold page cache: 128–512 KB chunks), iteration count and allocation count both increase further. The old `vec![0u8; 4MB]` outside the loop made 1 allocation total. The zero-copy benefit (`Bytes::copy_from_slice` avoided) is real but minor; the allocator churn across 251 tables is measurable on the 70 GB import already observed to take >150 minutes."
  },
  {
    "file": "src/db/copy_sink.rs",
    "line": 131,
    "summary": "`verify_spill_file_exists` TOCTOU: if the file disappears after the check, `stream_file_chunks`'s `File::open` error has no table or path context",
    "failure_scenario": "The pre-flight check returns a clear `\"spill file missing before COPY: {path}\"` message. But if the file exists at `try_exists` time and is deleted before `File::open` runs inside `stream_file_chunks` (external process, OS temp-cleaner, another j2s instance), the error surfaces as a bare `J2sError::Io(NotFound)` with no indication of which table or spill file is affected. Adding the same context message to the `NotFound` arm inside `stream_file_chunks` would close the gap; alternatively the pre-flight error message benefit is already lost on the race path."
  },
  {
    "file": "src/db/copy_sink.rs",
    "line": 302,
    "summary": "`let _ = std::mem::take(&mut self.pending)` uses `let _` on a value with no return — misleads readers into thinking the result is meaningful",
    "failure_scenario": "`Vec::drop` returns `()` and carries no `#[must_use]`. The conventional `let _ =` idiom in Rust signals \"intentionally ignoring a Result or must-use value.\" Future readers are confused about whether the old `Vec`'s destruction has observable side effects. `std::mem::take(&mut self.pending);` is the correct form — the drop happens implicitly and the intent (free the allocation) is communicated by `take` alone."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 331,
    "summary": "`unwrap_or(0)` on the minimum-index search masks an unreachable panic rather than making the invariant explicit",
    "failure_scenario": "If `parallel == 0` (validated away by `validate_run_params`), `loads` is empty, `min_by_key` returns `None`, `unwrap_or(0)` returns 0 — but `batches[0]` panics on an empty vec one line later. The `unwrap_or(0)` creates an illusion of a safe default while actually deferring the panic. Using `.expect(\"parallel >= 1 enforced by validate_run_params\")` documents the invariant; if validation is ever relaxed, the panic fires at the right site with a clear message instead of an index out-of-bounds."
  },
  {
    "file": "src/db/ddl.rs",
    "line": 195,
    "summary": "`constraint_session_sqls()` is a runtime function for compile-time-constant data; should be `const` or a `static`",
    "failure_scenario": "The function body is two string literals with no runtime computation. Declared as `fn`, the 2-element `[&'static str; 2]` array is stack-allocated on every call to `apply_constraints_chunk`. Declaring it as `const fn constraint_session_sqls()` or `const CONSTRAINT_SESSION_SQLS: [&'static str; 2]` moves the data to the read-only segment, makes the intent (fixed configuration, not derived values) explicit, and eliminates the misleading implication that the strings might change between calls."
  }
]