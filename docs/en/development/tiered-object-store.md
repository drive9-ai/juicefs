# Tiered Object Store for Drive9/JuiceFS

## Background
Drive9 wants configurable tiered storage where small data lands in TiDB and large data lands in S3, without breaking JuiceFS POSIX semantics, restore semantics, or GC/fsck correctness.

The implementation must not split files in Drive9 overlay/control-plane code. JuiceFS must remain the only filesystem engine and POSIX source of truth.

## Repository boundary
This issue is a **Drive9 tracking/design issue** for the tiered-storage requirement and acceptance gates.

Actual storage-engine implementation belongs in one of these places:

1. `db9-ai/juicefs` fork, or
2. upstream JuiceFS if accepted there.

`drive9-ai/drive9` must not implement file-size routing in overlay/FUSE/control-plane code. Drive9 changes, if any, are limited to selecting/configuring a JuiceFS binary/storage backend after the backend exists.

## Decision
Implement tiering below JuiceFS POSIX/metadata/chunk/cache layers, at the `object.ObjectStorage` boundary.

```text
JuiceFS VFS / metadata / chunk / cache       unchanged
  -> object.ObjectStorage                    unchanged interface
  -> TieredObjectStore                       new backend
       -> TiDBSmallStore                     small object payload + index
       -> S3LargeStore                       large object payload
```

P0 chooses **object-size tiering**, not strict file-size tiering.

- On `Put(key, bytes)`, if stored object size <= `small_threshold`, write payload to TiDB.
- If stored object size > `small_threshold`, write payload to S3.
- `Get/Head/List/Delete` consult a durable object index; they do not infer visibility from raw TiDB/S3 payload existence.

This preserves JuiceFS file semantics because JuiceFS still owns inode/path/rename/truncate/fsync/cache/chunk/slice behavior.

## Non-goals
- No Drive9 overlay-level file-size routing.
- No Drive9 control-plane routing based on path or file size.
- No strict file-size tiering in P0.
- No direct modification of JuiceFS inode/path/slice metadata to inline file bytes.
- No migration of existing volume data in P0.
- No per-client threshold override.

Strict file-size tiering can only be considered as a separate design because it requires JuiceFS metadata/chunk-layer awareness and a migration state machine for file small->large/large->small transitions.

## Naming
Do not use `jfs_*` table names. The persisted protocol should not be locked to JuiceFS.

If Drive9 maintains the backend, use:

```text
drive9_object_index
drive9_object_blob
drive9_object_gc_queue
drive9_object_meta
```

If the feature is prepared for upstream JuiceFS, use neutral names:

```text
tiered_object_index
tiered_object_blob
tiered_object_gc_queue
tiered_object_meta
```

Fields must use generic object-store names, not JuiceFS-specific terms:

```text
tenant_id / volume_id / object_key / generation / tier / size / checksum / state / updated_at
```

No `inode`, `path`, `chunk`, or `slice` fields belong in this schema.

## Object key representation and ordering
The index stores the **raw object key bytes exactly as passed to the tiered `ObjectStorage` implementation**.

Rules:

- `object_key` is a binary value, not a text value.
- SQL schema must use binary comparison (`VARBINARY`/`BLOB` primary key or explicit binary collation). Do not use default TiDB/MySQL text collation.
- Prefix, marker, `startAfter`, pagination, and ordering are bytewise lexicographic over raw `object_key` bytes.
- `List(prefix, marker, token, delimiter, limit)` must match JuiceFS object-store semantics using bytewise ordering.
- S3 payload keys may use an encoded/debuggable representation of `object_key`, but that encoding must never be used for index ordering.

This prevents SQL collation from changing object-listing semantics.

## Data model

### `drive9_object_index`
The index is the source of visibility truth.

Columns:
- `tenant_id` or `namespace_id`
- `volume_id`
- `object_key` as binary bytes
- `generation`
- `tier`: `tidb` or `s3`
- `size`
- `checksum`
- `state`: `active`, `deleting`
- `payload_ref`: TiDB blob generation or S3 immutable object key
- `updated_at`

Primary key:

```text
(volume_id, object_key)
```

Unique payload identity:

```text
(volume_id, object_key, generation)
```

### `drive9_object_blob`
Only stores small object payloads.

Columns:
- `volume_id`
- `object_key` as binary bytes
- `generation`
- `data`
- `size`
- `checksum`
- `created_at`

Primary key:

```text
(volume_id, object_key, generation)
```

### S3 payload keys
Use immutable generation keys, not mutable overwrite keys.

Canonical active payload pattern:

```text
objects/<volume_id>/<generation>
```

Optional debug suffix may be added after generation, but generation must be globally unique for the volume.

Temporary keys are only for multipart/partial staging:

```text
tmp/<volume_id>/<uuid>
```

Temporary keys are never referenced by active index rows. A large-object Put is visible only after the index points to the immutable generation key.

## Object operation semantics

### Put small object
1. Read/buffer the object payload.
2. Calculate `size`, volume-fixed `checksum`, and new monotonic/unique `generation`.
3. In one TiDB transaction:
   - insert `drive9_object_blob(volume_id, object_key, generation, data, checksum)`;
   - atomically update `drive9_object_index` to point to `(tier='tidb', generation, payload_ref=generation, state='active')`;
   - enqueue old payload cleanup if old generation exists.
4. Commit makes the new version visible.
5. Cleanup old S3/TiDB payload asynchronously.

### Put large object
1. Stream payload to an immutable S3 generation key: `objects/<volume_id>/<generation>`.
2. Verify size/checksum.
3. In one TiDB transaction:
   - atomically update `drive9_object_index` to point to `(tier='s3', generation, payload_ref=s3_generation_key, state='active')`;
   - enqueue old payload cleanup if old generation exists.
4. Commit makes the new version visible.
5. Cleanup old payload asynchronously.

If S3 upload succeeds but index transaction fails, the S3 generation object is an orphan and must be cleaned by GC. It was never visible because the index never referenced it.

### Multipart staging
P0 may report multipart unsupported if JuiceFS block writes remain bounded by JuiceFS block size.

If multipart is enabled later:

1. multipart parts upload to `tmp/<volume_id>/<upload_id>/...` keys;
2. `CompleteUpload` assembles/copies to an immutable generation key;
3. only after assembly and checksum verification does the index point to the generation key;
4. tmp keys are GC-able staging payloads, not active payloads.

### Concurrent Put linearization
Concurrent Put to the same `(volume_id, object_key)` must be linearized by the index transaction.

P0 rule: **last committed index transaction wins**.

Required behavior:

- Each Put writes a unique payload generation before attempting index commit.
- Index update happens in a serializable transaction or equivalent compare-and-swap loop.
- Cleanup queue must record only the previous generation observed by the winning transaction.
- Cleanup must never delete the current generation. Before deleting any payload, the cleaner re-reads index and verifies `(object_key, generation)` is not active.
- Loser/orphan generations are invisible and later cleaned by GC.

Tests must cover concurrent small/small, small/large, large/small, and large/large Puts. Readers may see the old complete generation or the last committed complete generation, never an uncommitted/partial generation.

### Get / Head
1. Read active index row by `(volume_id, object_key)`.
2. If not found, return not-exist.
3. If tier is `tidb`, read exact `(object_key, generation)` from `drive9_object_blob`.
4. If tier is `s3`, read exact `payload_ref` from S3.
5. Validate size/checksum when feasible.
6. Missing indexed payload is corruption, not a normal 404.

### Delete
1. In TiDB transaction, remove or tombstone the active index row and enqueue cleanup.
2. After commit, object is invisible to JuiceFS.
3. Payload cleanup may run asynchronously.
4. Cleanup failure leaves an orphan; it must be recoverable by GC.

### List(prefix, marker, delimiter, limit)
List must use `drive9_object_index` as the source of truth.

Rules:

- Return active index rows only.
- Sort by raw binary `object_key`.
- Implement `startAfter` / marker / token semantics deterministically.
- Implement delimiter semantics to match JuiceFS object storage expectations.
- Deduplicate by primary key.
- Do not scan S3 and TiDB payload stores to construct visible List results.

Raw S3/TiDB payloads without index are orphans and belong to GC/fsck, not normal List.

### Copy
P0 does **not** support object-store `Copy`. `TieredObjectStore.Copy` returns unsupported.

Reason: a correct cross-tier Copy needs a generation-snapshot contract and source-generation lifetime protection. A naive read-source then Put-destination is not acceptable because source overwrite/delete during Copy makes the destination semantics ambiguous and may race with GC of the source generation.

Before enabling Copy in a later PR, the design must define one of these contracts:

1. generation-snapshot Copy: Copy first reads source index `(tier, generation, checksum, size)` and copies that exact generation to destination, while GC cannot delete that generation until Copy completes; or
2. explicit unsupported behavior where all callers tolerate `notSupported`.

P0 selects option 2. Implementation PRs must verify current JuiceFS mount/write/gc/fsck paths work with Copy unsupported for this backend, or keep the backend disabled until that is proven.

## File small->large behavior
P0 does not track file size. It tracks object size.

When a file grows:

- JuiceFS metadata/chunk layer decides which slices/blocks represent the file.
- Small existing objects may remain in TiDB.
- New larger objects may land in S3.
- Reads are composed by JuiceFS from whatever object keys its metadata references.
- Old unreferenced objects are removed by JuiceFS GC through `Delete` or detected as leaked by object listing.

This avoids a file-level migration state machine.

If a later rewrite overwrites the same object key with a different size, the Put overwrite path must handle:

- small -> large
- large -> small
- small -> small
- large -> large

Readers may see either the old complete generation or the new complete generation, never a partial generation.

## GC/fsck design
There are two GC layers.

### JuiceFS GC/fsck
Existing JuiceFS `gc` and `fsck` continue to call the object storage interface:

- `List/ListAll` sees visible index rows.
- `Head` validates indexed payload existence.
- `Delete` makes indexed object invisible and schedules payload cleanup.

### Tiered backend GC/fsck
The tiered backend also needs its own object-payload consistency checker.

It must detect:

- index row -> missing TiDB blob/S3 object: corruption;
- TiDB blob -> no active index: orphan, delete after grace period;
- S3 generation/temp object -> no active index: orphan, delete after grace period;
- old generation not referenced by current index: cleanup candidate;
- index state `deleting` older than grace period: retry cleanup;
- checksum/size mismatch: corruption;
- duplicate active generation for one key: corruption.

GC must never delete an active payload referenced by index.

### Time and grace-period safety
GC decisions use persisted `created_at` / `updated_at` timestamps plus a conservative grace period.

- Default grace period must be large enough to tolerate clock skew and long-running uploads.
- If process monotonic time is unavailable across restarts, wall-clock timestamps are advisory and GC must bias toward not deleting recent payloads.
- Active index references always override age-based cleanup.

## Crash recovery gates
Fault injection tests must cover:

- small Put: blob write before index commit;
- large Put: S3 generation upload before index commit;
- overwrite: index committed before old payload cleanup;
- Delete: index removed before payload cleanup;
- cleanup queue item processed partially;
- process restart during GC.

Recovery invariants:

- active index generation is readable;
- unindexed payload is invisible;
- old generation is invisible;
- orphan payload is eventually GC-able;
- missing indexed payload is reported as corruption.

## Multi-client invariants
All clients mounting the same volume must use identical tiered backend config:

- same volume id;
- same threshold;
- same checksum algorithm;
- same TiDB/S3 endpoints;
- same schema version.

Different thresholds for the same volume must fail closed at mount/format/config load.

Multi-client tests must verify:

- client A writes small, client B reads;
- client A overwrites small -> large, client B reads new data after normal JuiceFS cache invalidation;
- client A deletes, client B sees not-exist after metadata invalidation;
- concurrent overwrite does not expose partial payload.

## Configuration
Volume-level config:

```text
storage = tiered
bucket = tiered://<volume-id>
tiered-small-backend = mysql/tidb/tikv DSN
tiered-large-backend = s3 bucket URL
tiered-small-threshold = 64KiB / 256KiB / 1MiB
tiered-gc-grace = 24h
tiered-checksum = crc32c or sha256
tiered-schema-version = 1
```

Constraints:

- threshold is volume-level fixed config;
- checksum algorithm is volume-level fixed config;
- threshold must be capped by the supported TiDB/TiKV blob payload size and by an operational safety max;
- threshold changes after volume creation are rejected in P0;
- rebalancing existing objects is a separate offline tool.

## PR split

### PR 1 — design + test harness
- Add design document.
- Add fault-injectable in-memory `TieredObjectStore` tests.
- Validate Put/Get/Head/Delete/List semantics independent of JuiceFS.

### PR 2 — TiDB/MySQL small payload + index store
- Implement `drive9_object_index`, `drive9_object_blob`, cleanup queue.
- Cover transaction semantics and small-object overwrite cases.

### PR 3 — S3 large payload adapter
- Implement S3 immutable generation upload path.
- Cover large-object Put/Get/Head/Delete and orphan generation cleanup.

### PR 4 — combined TieredObjectStore backend
- Register storage backend.
- Enforce fixed volume config and threshold.
- Implement List/ListAll, Copy fallback, Delete cleanup queue.

### PR 5 — gc/fsck command or admin path
- Add tiered backend fsck/gc checker.
- Cover orphan/corruption cases and cleanup grace.

### PR 6 — integration benchmark/e2e
- Compare S3+writeback, whole-volume TiDB/TiKV, and tiered backend.
- Test with git clone/status/build small-file workload.
- Test multi-client behavior.

## Acceptance criteria
- No Drive9 overlay/control-plane code decides file tier.
- JuiceFS POSIX metadata/chunk/cache semantics remain unchanged.
- `object_key` ordering is raw bytewise/binary and List-compatible.
- Large-object Put uses immutable generation keys; temp keys are staging-only.
- P0 Copy returns unsupported; no naive read-source then Put-destination Copy is allowed.
- Concurrent Put is linearized by index transaction; cleanup never deletes active generation.
- `List` returns index-visible objects only with stable ordering/pagination.
- Delete visibility is index-driven.
- small->large and large->small overwrites are generation-safe.
- Crash tests prove no partial generation becomes visible.
- GC/fsck detects corruption and removes orphans safely.
- Multi-client tests pass.
- Benchmark demonstrates measurable value over tuned JuiceFS S3/writeback before production adoption.
