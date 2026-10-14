# Mooncake Master Mock Client Design

Date: 2026-10-14

Status: design for review

## Context

The HA lab archive `ha-lab-preserve-202610.tar.zst` was verified with the
provided SHA-256 file and inspected after decompression. Its test records now
use the current names `madsys-01`, `madsys-03`, and `madsys-02` for the former
`qhjf-03`, `qhjf-04`, and `qhjf-06`. Future test commands, reports, and
question/answer records use only the new names. Node IDs `03`, `04`, and `06`
remain as historical process and script identifiers and must be mapped
explicitly to the new names.

Before this design document was created, the current branch had no uncommitted
source changes. `HEAD` is
`1543e389` (`[Store][HA] Preserve HA instrumentation and test support`). That
commit adds client-side batch put timing and metadata-only put support, extends
HA OpLog batch encode/decode/backend timing, and expands the HA end-to-end
client and batch snapshot/provider tests. The mock client should reuse these
interfaces and metrics without changing their semantics.

## Goals and non-goals

The tool is a standalone C++ mock client that exercises the Mooncake master
control plane at high rate. It registers synthetic client segments and runs
Put, Upsert, GetReplicaList, Remove, Ping, mount/remount, disconnect, and
reconnect flows. A Python layer creates configurations, launches multiple
instances, scrapes master metrics, and produces an auditable report.

The first version does not exercise payload transfer, TransferEngine, real
memory, SSD, replication data movement, or client-to-client traffic. A passing
run proves only that master control-plane metadata and the observed control
protocol are correct. Every result includes:

```text
data_plane_not_exercised=true
```

The target is approximately 100,000 logical object operations per second. The
primary throughput unit is logical object operations per second; expanded RPC
attempts, RPC calls, batch requests, and batch items are reported separately.

## Options considered

1. A Python-only client would be quick to change but would spend too much CPU
   in the interpreter and make a 100k-object-operation target unreliable.
2. Extending the existing end-to-end client would reuse more code, but its
   data-plane lifecycle and one-shot command modes make high-rate control-plane
   fault tests difficult to isolate.
3. The selected design is an independent C++ executable linked with
   `mooncake_store`, configured by versioned JSON, with Python orchestration and
   analysis. It keeps the hot path native while keeping test matrices and
   reports easy to change.

## Architecture

Add a separately gated target under `mooncake-store/tools/mock_client/`.
`mooncake-store/CMakeLists.txt` will add this subdirectory when
`BUILD_MOCK_CLIENT=ON`; it must not depend on the existing `STORE_USE_ETCD`
gate used by the unrelated OpLog inspector. The executable links the existing
`mooncake_store` library and uses the repository's JSON dependency.

The C++ executable is split into small units:

- `config`: parse `schema_version`, validate ranges, apply defaults, and
  reject ambiguous settings before connecting;
- `master_session`: create a `MasterClient`, connect, mount synthetic
  segments, run Ping, handle `NEED_REMOUNT` with `ReMountSegment`, and perform
  unmount/disconnect/reconnect transitions;
- `operation_executor`: implement single and batch Put/Upsert/Get/Remove and
  their Start/End/Revoke rounds; every network attempt receives a request and
  attempt ID;
- `shadow_model`: store only key existence, generation, logical size, owning
  mock client/segment, expected replica count, descriptor summary, and an
  optional deterministic checksum;
- `fault_scheduler`: apply configured pre-call delay, post-call delay,
  response drop, disconnect windows, reconnect backoff, and error-class
  triggers;
- `workload_controller`: low, medium, full, and adaptive modes, concurrency,
  batching, pacing, and backpressure;
- `event_sink`: append JSONL events and periodic aggregate snapshots without
  blocking the operation workers.

The Python package beside the executable provides `run.py`, `metrics.py`, and
`report.py`. It expands a client matrix, starts/stops processes, captures raw
master `/metrics`, `/health`, `/role`, and `/version` responses, detects master
process/leader epochs, and emits a machine-readable report plus a concise
human summary.

## Synthetic segments

The configuration supplies each client with a UUID, segment UUID, logical name,
capacity, protocol, transport endpoint, host ID, and synthetic base address.
The executable never dereferences a synthetic address. The default deployment
must use the master's offset allocator path, with nonzero, aligned,
non-overlapping addresses. Mounting still creates the master's allocator
metadata and allows PutStart to allocate descriptor offsets; no client buffer
is allocated or written.

Before a run, a low-speed capability probe mounts one segment and performs one
metadata-only Put/End/Get/Remove cycle. If the configured master allocator
requires a real mapped buffer or the probe shows an address dereference, the
run stops with an explicit `metadata_only_unsupported` result. It must not
silently claim to be data-plane-free. Cachelib-backed deployments are not a
supported first-version configuration.

## Session and operation protocol

Each client follows this state machine:

```text
connect -> mount -> ping loop -> workload
                     |             |
                     +-- remount <-+-- NEED_REMOUNT
workload -> drain/reconcile -> unmount -> disconnect
```

Mutation operations are multi-round. A successful Put or Upsert consists of
Start followed by End for each accepted item. Failed Start items are revoked
when the result is known. A transport timeout or disconnect after a mutation is
an unknown result: the tool does not replay that mutation automatically. It
freezes that key, queries metadata, and marks the action confirmed, absent, or
unknown before allowing a later mutation for the key.

GetReplicaList and BatchGetReplicaList are control-plane validation operations.
Validation calls use `purpose=validation` in the event log and never count as
workload logical operations. Remove confirmation accepts `OBJECT_NOT_FOUND` or
an explicitly recorded transitional state according to the configured settle
policy.

## Deterministic metadata model

Keys are derived from a fixed seed, namespace, client index, and monotonically
assigned logical index. Logical size, generation, and an optional checksum are
derived from the same inputs. The checksum is a reproducibility aid; it is
metadata carried through `ObjectMeta` and returned by master, not a payload
integrity proof.

After every known successful mutation, the shadow model records the expected
state. Validation compares master results with the model:

- object existence and generation/checksum;
- logical size where exposed by the descriptor;
- replica count and `ReplicaStatus`;
- segment name/ID, transport endpoint, and descriptor variant;
- client/host ownership summary when available.

The model is bounded by a configurable maximum key count and can evict old
entries only when the run is configured as a non-validation pressure test.
Validation runs retain all touched keys until final reconciliation.

## Configuration

The JSON schema starts at version 1 and contains:

- `run`: run ID, seed, namespace, duration, output paths, and validation
  policy;
- `master`: RPC address or HA address set, metrics endpoints, request timeout,
  and scrape interval;
- `clients`: count, UUIDs, host IDs, synthetic segments, reconnect policy, and
  per-client seed offsets;
- `replication`: memory/NoF/DFS replica counts, pin policy, preferred segment
  policy, and logical object size distribution;
- `workload`: `low`, `medium`, `full`, or `adaptive`, target logical ops/s,
  concurrency, batch limits, operation selection/mix, and validation sampling;
- `faults`: deterministic schedules for delays, response drops, disconnects,
  remounts, and injected error responses;
- `report`: JSONL path, raw metrics path, scrape interval, and exit policy.

The workload must explicitly select which logical operations are enabled. The
selection is configuration-driven and may also be overridden by a command-line
profile name for repeatable runs. Each entry has an operation name, an optional
weight, a single-object or batch form, and batch size bounds. Supported logical
operation names are `put`, `upsert`, `get`, and `remove`; `put` and `upsert`
may use either their single RPC or batch RPC form, while `get` and `remove`
may likewise select single or batch form. Session traffic (`ping`, mount,
remount, and unmount) is always controlled separately and is not part of the
logical operation mix.

For example, these are valid profiles:

```json
{"operations":[{"name":"put","form":"single","weight":1}]}
{"operations":[{"name":"put","form":"batch","weight":1,
  "batch_size":{"min":128,"max":128}}]}
{"operations":[
  {"name":"put","form":"single","weight":3},
  {"name":"remove","form":"single","weight":1}]}
```

An empty operation list is invalid. Weights are normalized among enabled
entries; a fixed sequence or deterministic seed may be used when exact replay
is required. A batch entry consumes one scheduling decision but contributes
one logical operation per item. The report preserves the selected operation
set and effective weights, so a run using only Put, only BatchPut, or a Put /
Remove mixture cannot be confused with a default mixed workload. Validation
reads may be enabled independently and are labeled `purpose=validation`.

The workload profiles have fixed semantics:

- `low`: bounded rate and concurrency for feature and fault smoke tests;
- `medium`: fixed concurrency/rate for stable comparisons;
- `full`: launch as fast as possible up to configured concurrency and batch
  limits, recording backpressure rather than hiding it;
- `adaptive`: default 100k-object-operation probe. It increases pressure during
  a healthy window and reduces rate/concurrency on
  `NO_AVAILABLE_HANDLE`, `TASK_PENDING_LIMIT_EXCEEDED`,
  `UNAVAILABLE_IN_CURRENT_STATUS`, `RPC_TIMEOUT`, or `RPC_FAIL`; it ramps up
  again only after a recovery window.

An example configuration is kept with the tool and must include explicit
`madsys-01`, `madsys-03`, and `madsys-02` host names. Operational configs and
generated records must not introduce `qhjf-*` aliases; the old names may occur
only in migration-history explanations.

## Event log and counters

Each JSONL event contains at least:

```json
{
  "run_id": "...",
  "client_id": "...",
  "request_id": "...",
  "attempt_id": 1,
  "purpose": "workload|validation|session",
  "rpc": "BatchPutStart",
  "logical_op": "put|upsert|get|remove|none",
  "batch_items": 128,
  "result": "ok|application_error|transport_error|unknown",
  "error_code": "OK",
  "latency_us": 1234
}
```

The aggregate report distinguishes `logical_ops`, `rpc_attempts`,
`rpc_calls_with_response`, `batch_requests`, `batch_items`, successful items,
failed items, and unknown attempts. Validation and session traffic are shown
separately.

## Master metrics reconciliation

The Python runner captures raw Prometheus text before the workload, at a fixed
interval, after writes stop, and after final reconciliation. It also captures
the endpoint role and health at each scrape. The parser treats a missing zero
counter as zero, detects counter decreases, and starts a new process epoch on
master restart or failover. Raw snapshots are retained so reports can be
recomputed later.

The first-version mapping is explicit:

| Client RPC | Master counter family | Unit |
| --- | --- | --- |
| PutStart, UpsertStart | `master_put_start_*` | one request/item for single; batch items in `master_batch_put_start_items_total` |
| PutEnd, UpsertEnd | `master_put_end_*` / `master_batch_put_end_*` | request/item as above |
| PutRevoke, UpsertRevoke | `master_put_revoke_*` / `master_batch_put_revoke_*` | request/item as above |
| GetReplicaList | `master_get_replica_list_*` | one key |
| BatchGetReplicaList | `master_batch_get_replica_list_*` | request and item counters |
| Remove | `master_remove_*` | one key |
| BatchRemove | `master_remove_*` | master adds one item per key; no separate batch request counter |
| Ping/mount/remount/unmount | corresponding session counters | one RPC |

Upsert intentionally shares the Put metric families in the current master
implementation. BatchRemove intentionally shares the Remove item counter, so
the client event log remains the source for batch-call counts.

For each method and epoch, the report compares client-derived expected counts
with master counter deltas:

- `exact_match`: every sent attempt has a response, the scrape interval has no
  external traffic, and all mapped deltas match;
- `within_unknown_bound`: one or more transport outcomes are unknown; the
  master delta is within `[known_lower_bound, known_lower_bound + unknown]`;
- `indeterminate_external_traffic`: another client or service may have changed
  the process-level counters, or a counter reset/epoch boundary cannot be
  isolated.

An application-level error response still proves that master received and
counted the request. A timeout, connection loss, or injected response drop does
not: it contributes to the upper bound until reconciliation. Mutation replay
is forbidden until reconciliation, preventing duplicate master operations.

Exact equality is therefore promised only on a dedicated test window with no
other master traffic, complete snapshots, no unknown attempts, and stable
master epochs. Shared-cluster results remain useful but are reported as
non-attributable ranges. The report separately includes master HA/OpLog,
snapshot, queue-depth, latency, CPU/RSS, and error counters when present; an
unavailable optional metric is recorded as `not_exported`, never as zero.

## Fault injection and exit codes

Faults are scheduled by deterministic event number or monotonic time. Delay and
drop hooks surround the RPC call; disconnect faults close the session and keep
the configured key cohort frozen until reconnect and reconciliation. Reconnect
uses bounded exponential backoff and remounts the exact segment descriptors.

The controller records every reduction/increase decision, trigger error, queue
depth, and recovery window. It does not convert master backpressure into a
client success.

The executable exits with:

- `0`: control-plane validation and requested reconciliation pass;
- `2`: invalid configuration or unable to establish the required session;
- `3`: run completed but master/client count is indeterminate;
- `4`: control-plane metadata validation failed;
- `5`: internal event/report output failure.

## Testing and rollout

Implementation is staged so the high-rate path is not introduced before its
state model is testable:

1. Unit tests for JSON validation, deterministic key/checksum derivation,
   shadow-model transitions, metric mapping, and adaptive-controller decisions.
2. Python tests for Prometheus parsing, counter resets, epoch stitching,
   unknown-result bounds, and report classification.
3. A local/mock-master integration test for connect, mount/remount, all
   multi-round mutations, batch partial results, and remove reconciliation.
4. A low-speed real-master smoke run using one client and one synthetic
   segment, followed by a two-client batch run.
5. HA tests for disconnect/reconnect, remount after Ping status, leader change,
   and process-epoch metric stitching.
6. Performance runs that increase batch size and concurrency gradually toward
   100k logical operations per second while preserving exact or bounded
   reconciliation evidence.

The first real-master gate is a metadata-only capability probe. A performance
result is invalid if that probe, final shadow validation, or the master counter
reconciliation fails. The existing archive playbooks remain the source for
HA-specific operational ordering; the new tool adds control-plane load and
does not replace their provider/data-plane tests.
