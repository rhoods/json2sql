[
  {
    "file": "src/pass2/runner.rs",
    "line": 287,
    "summary": "Deadlock: workers spin in pause_flag loop without checking error_flag; flusher exits via ? without clearing pause_flag",
    "failure_scenario": "Any PG error during RAM-pressure flush sets error_flag but never reaches pause_flag.store(false) at line 179. All workers remain in the yield_now() spin forever — the program hangs indefinitely."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 597,
    "summary": "Flusher handle leak: anomaly_writer_handle error returns before flusher_handle.await, abandoning the flusher task and its PG connection",
    "failure_scenario": "If the anomaly writer errors (disk full, etc.), run() returns at line 599 while flusher_handle is dropped without being awaited. The flusher task continues in the background with its PG connection open, silently losing unflushed rows."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 607,
    "summary": "Wrong error surfaced when both worker and flusher fail: worker's generic 'flusher reported a fatal PG error' wins over the actual PG error in flusher_result",
    "failure_scenario": "first_error (the generic worker abort string) is returned at line 607 before flusher_result is examined at line 609, discarding the actual PostgreSQL error text that identifies the failing table and SQL."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 80,
    "summary": "Pass2Error progress event never emitted by flush_table_to_pg on PG failure; UI loses real-time error notification",
    "failure_scenario": "progress_event::Pass2Error exists to notify the UI per failing table. The new runner sets error_flag and returns Err but never sends this event, so the UI stays in loading state until the entire run() resolves — users see no error until the full import ends."
  },
  {
    "file": "json2sql-ui/src/screens/setup.rs",
    "line": 571,
    "summary": "Dead UI: 'Temp directory' section claims Pass 2 writes temp files and shows disk-space warnings, but Pass2Config has no temp_dir and the diskless pipeline never uses it",
    "failure_scenario": "Users configure a temp dir and receive disk-space warnings for a path that is silently ignored. The UI description 'Pass 2 writes all rows to temporary files' is factually false after the diskless migration."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 498,
    "summary": "RAM watermark validation missing: ram_high_watermark > 1.0 is accepted silently; backpressure never activates",
    "failure_scenario": "ram_used_ratio() is capped at 1.0 by the OS. With ram_high_watermark=2.0, ratio > ram_high_watermark is never true, pause_flag stays false, and RAM grows unbounded until OOM."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 201,
    "summary": "mem_flush_threshold_bytes=Some(0) not rejected; every row becomes a discrete flush causing severe channel contention",
    "failure_scenario": "With threshold=0, line 161 evaluates 0 >= 0 = true on every message, triggering flush_table_to_pg per row. The flusher's channel saturates, workers block on flush_tx.send, and throughput collapses to the rate of individual PG COPY round-trips."
  },
  {
    "file": "src/pass2/runner.rs",
    "line": 147,
    "summary": "RAM-pressure relief flushes only the single largest buffer per 1-second tick; wide schemas may never drain fast enough to unpause workers",
    "failure_scenario": "With 100 tables each at 64 MiB, flusher drains 64 MiB/s while workers have accumulated GiBs and are spinning paused. Flushing one table/tick cannot relieve the watermark quickly enough — the pause becomes permanent."
  },
  {
    "file": "src/db/copy_sink.rs",
    "line": 128,
    "summary": "flush_mem_sink_to_pg sends the full buffer as one Bytes chunk; no chunked streaming means a RAM spike proportional to threshold × worker_count",
    "failure_scenario": "At 8 workers each sending 64 MiB simultaneously, up to 512 MiB sits in async write buffers awaiting PG acknowledgment — the opposite of the RAM relief the watermark system intends. The old TempFileSink streamed in 4 MiB chunks to bound this."
  }
]
Summary of 9 confirmed/plausible findings:

#	Severity	File	What
1	Critical	runner.rs:287	Deadlock — pause loop never exits on flusher error (pause_flag never cleared)
2	Critical	runner.rs:597	Flusher leak — anomaly_writer early return drops JoinHandle without await
3	High	runner.rs:607	Wrong error returned — generic worker message hides real PG error
4	High	runner.rs:80	Pass2Error event never sent — UI regression, no real-time error notification
5	High	setup.rs:571	Dead UI — temp_dir section + disk-space warnings for a field that's no longer used
6	Medium	runner.rs:498	Missing watermark range validation — ram_high > 1.0 silently disables backpressure
7	Medium	runner.rs:201	mem_flush_threshold=0 not rejected — each row becomes a separate COPY
8	Medium	runner.rs:147	One table flushed per 1s tick — wide schemas may never drain under RAM pressure
9	Medium	copy_sink.rs:128	No chunked streaming — large buffers spike RSS proportional to threshold × workers
The two critical bugs (#1 and #2) interact: a PG error during RAM pressure triggers the deadlock AND leaves the flusher running detached. Fix #1 first by checking error_flag inside the pause spin loop, and fix the missing pause_flag.store(false) on the error exit path of run_flusher.