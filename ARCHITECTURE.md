# Architecture

```mermaid
flowchart LR
  A[HTTP / CSV / JSONL / entry point sources] --> B[bounded asyncio queue]
  B --> C[Pydantic validation]
  C -->|invalid| D[batch dead-letter staging]
  C --> E[record-local map / filter or whole-run transforms]
  E --> F[batch staging on output filesystem]
  F --> G[prepared output manifest]
  G --> H[atomic per-file promotion]
  H --> I[committed batch manifest]
  I --> J[atomic source checkpoint]
  J --> K[atomic summary, completed run marker]
```

## Async engine and memory

Source producers and the queue consumer run in an `asyncio.TaskGroup`. The bounded queue applies backpressure. HTTP sources share one `aiohttp.ClientSession` with connector limits. Source errors are isolated unless fail-fast is selected. Cancellation closes the HTTP client and stops producers; batches already committed remain available for recovery.

No-transform, mapping-only, and filtering-only pipelines process records as they arrive and keep current records bounded by the queue and sink batch size. Mapping and filtering are record-local and preserve the consumer arrival order. Deduplication, aggregation, and CPU fingerprinting currently retain the valid input for whole-run behavior; those pipelines use input-proportional memory.

## Output commit and checkpoint flow

For every output batch, DATAPIPE writes immutable output and dead-letter files under `<output>/.datapipe/staging/<batch-id>`. It atomically persists a prepared manifest entry containing file paths and source cursor watermarks. It promotes each file with same-filesystem `os.replace`, marks the batch committed, then advances the atomic checkpoint. Each file promotion is atomic; a group spanning partitions is not. If a process stops during promotion, the next startup sees the prepared entry, skips destinations already present, promotes remaining staged files, commits the journal, and reconciles checkpoint cursors. If the failure predates the prepared manifest entry, staged artifacts are orphaned and cleaned; the source cursor remains behind and input is replayed.

The manifest is `<output>/.datapipe/manifest.json`. It is config-signature bound and records batches plus run states (`running`, `interrupted`, `completed`). The summary is atomically replaced at run completion; the run is marked completed only after that write. The public `dead-letter.jsonl` file is rebuilt atomically from committed immutable dead-letter batches at successful run completion and startup recovery. Old dead-letter data is preserved when the manifest is initialized over an existing output directory.

Delivery is **at least once**, not exactly once. Output files can be visible in partition paths before every file in a prepared multi-partition batch has been promoted. Consumers requiring complete batch boundaries must use the manifest; there is not yet a dedicated manifest-aware reader. A crash after batch commits but before summary completion leaves the run marked interrupted, while committed data and cursors remain. Directory fsync is not available consistently across platforms, so sudden power loss guarantees depend on the filesystem. Multiple DATAPIPE processes must not share an output root because the journal is not protected by an inter-process lock.

## Signals and shutdown

The CLI installs SIGINT and SIGTERM handlers where Python permits, cancels the pipeline task, restores previous handlers, and exits with `128 + signal number`. The engine propagates cancellation through its TaskGroup and closes HTTP resources. Work submitted to `asyncio.to_thread` may finish while `asyncio.run` shuts down its executor; a batch is either journaled and recoverable or left as uncommitted staging data. POSIX subprocess tests send real SIGINT and SIGTERM. On Windows, Ctrl+C can be delivered to an attached console process; `TerminateProcess`-style SIGTERM cannot be caught and relies on next-run recovery.

## Plugins

Third-party source classes are discovered through the `datapipe.sources` entry-point group and registered in `SourceRegistry`. Configure the entry-point name as `type` and constructor arguments under `settings`. A resumable plugin accepts a `cursor` constructor keyword, exposes a cursor watermark that advances to the next item only as each item is yielded, and resumes from that watermark. Plugins without that constructor argument can run, but cannot be resumed by DATAPIPE. Static end-of-source cursors are unsafe for small batch sizes.
