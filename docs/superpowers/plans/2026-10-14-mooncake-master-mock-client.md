# Mooncake Master Mock Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a configurable, metadata-only C++ mock client and Python runner that can drive selected Mooncake master operations, inject delay/failure/disconnect scenarios, validate control-plane metadata, and reconcile client records with master metrics up to approximately 100,000 logical object operations per second.

**Architecture:** Compile a reusable `mock_client_core` C++ library from focused configuration, deterministic-data, shadow-model, master-session, operation, fault, workload, and event components. Link a thin `mooncake_master_mock_client` executable to that library. Use Python modules to launch multiple clients, scrape master admin endpoints, preserve raw evidence, and classify exact, bounded, or indeterminate counter reconciliation.

**Tech Stack:** C++17/20 as required by the existing Mooncake build, `mooncake_store::MasterClient`, JsonCpp, gflags, glog, GoogleTest, Python 3 standard library, pytest, Prometheus text parsing implemented for this tool, and the existing CMake/pre-commit toolchain.

**Spec:** `docs/superpowers/specs/2026-10-14-mooncake-master-mock-client-design.md`

## Global Constraints

- The first version exercises only master control-plane metadata; every report must contain `data_plane_not_exercised=true`.
- The primary throughput unit is logical object operations per second; RPC attempts, responses, batch requests, and batch items are separate counters.
- Workload operation selection is explicit. Supported logical names are `put`, `upsert`, `get`, and `remove`, each with `single` or `batch` form where supported; an empty selection is invalid.
- A batch scheduling decision contributes one logical operation per item, and the report must retain the selected operation set and effective weights.
- Upsert uses the master Put metric families; BatchRemove uses the master Remove item counter and has no separate master batch request counter.
- Exact client/master equality is reported only with a dedicated no-external-traffic window, complete metric snapshots, stable master epochs, and no unknown transport outcomes.
- Unknown mutation results must be reconciled before the key is mutated again; the client must never blindly replay an ambiguous mutation.
- Synthetic segment addresses are never dereferenced by the client. The first run must capability-probe the configured master allocator and fail explicitly if metadata-only operation is unsupported.
- New configurations, commands, examples, and generated records use `madsys-01`, `madsys-03`, and `madsys-02`; `qhjf-*` appears only in migration-history explanations.
- CMake must gate the tool with `BUILD_MOCK_CLIENT`; it must not inherit the unrelated `STORE_USE_ETCD` tools gate.
- Each task ends with focused tests, `git diff --check`, and a focused commit. Run pre-commit on touched files before handoff.

---

### Task 1: Add the build target and validated workload-selection contract

**Files:**
- Create: `mooncake-store/tools/mock_client/CMakeLists.txt`
- Create: `mooncake-store/tools/mock_client/include/mock_client/config.h`
- Create: `mooncake-store/tools/mock_client/src/config.cpp`
- Create: `mooncake-store/tools/mock_client/include/mock_client/operation_selection.h`
- Create: `mooncake-store/tools/mock_client/src/operation_selection.cpp`
- Create: `mooncake-store/tools/mock_client/tests/config_test.cpp`
- Create: `mooncake-store/tools/mock_client/tests/operation_selection_test.cpp`
- Modify: `CMakeLists.txt` to add `option(BUILD_MOCK_CLIENT "Build the master mock client" OFF)`
- Modify: `mooncake-store/CMakeLists.txt` to add the mock-client subdirectory when `BUILD_MOCK_CLIENT` is enabled

**Interfaces:**
- Produces `mock_client_core`, a static library consumed by later C++ tasks.
- Produces `mooncake_master_mock_client`, initially a minimal executable that parses `--config` and exits after validation.
- `enum class LogicalOperation { Put, Upsert, Get, Remove };`
- `enum class OperationForm { Single, Batch };`
- `struct BatchSizeRange { uint32_t min; uint32_t max; };`
- `struct OperationSelection { LogicalOperation operation; OperationForm form; uint32_t weight; BatchSizeRange batch_size; };`
- `std::string ToString(LogicalOperation)` and `std::string ToString(OperationForm)` provide stable report names.
- `tl::expected<std::vector<OperationSelection>, ConfigError> ParseOperationSelection(const Json::Value&)` rejects unknown names/forms, zero weights, invalid ranges, empty lists, and batch-only fields on a single form.
- `tl::expected<std::vector<OperationSelection>, ConfigError> ParseOperationOverride(std::string_view)` accepts comma-separated entries such as `put:single=1,put:batch=3,remove:single=1`.
- `MockClientConfig::LoadFile(path, optional_operation_override)` parses `schema_version == 1`, master endpoints, client/segment definitions, workload profile, faults, output paths, and the selected operations.

- [ ] **Step 1: Write failing selection and config tests.** Cover `put:single`, `put:batch`, `remove:single`, mixed weights, deterministic ordering, empty lists, unknown operation/form, zero weight, `min > max`, missing `batch_size` for a batch entry, invalid schema version, missing master endpoint, duplicate client UUID, and duplicate segment name.

```cpp
TEST(OperationSelectionTest, ParsesMixedSingleAndBatchEntries) {
    auto result = ParseOperationOverride("put:single=3,put:batch=2,remove:single=1");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 3);
    EXPECT_EQ(result->at(1).form, OperationForm::Batch);
    EXPECT_EQ(result->at(1).weight, 2);
}

TEST(OperationSelectionTest, RejectsEmptySelection) {
    EXPECT_FALSE(ParseOperationOverride("").has_value());
}
```

- [ ] **Step 2: Run the focused test target to confirm failure.**

Run: `cmake --build build --target mock_client_core_test -j2`

Expected: FAIL because the selection/config implementation and target do not yet exist.

- [ ] **Step 3: Implement the parser, validation, stable enum names, and minimal executable.** Use JsonCpp `CharReaderBuilder`/`parseFromStream` and `writeString` patterns already used in the repository. Store the normalized operation vector exactly as supplied after validation; do not silently add default operations when the JSON list is present. Add a `--operations` gflags override that replaces, rather than merges with, the JSON list.

- [ ] **Step 4: Run the focused tests and config smoke.**

Run: `cmake -S . -B build -DBUILD_MOCK_CLIENT=ON -DBUILD_UNIT_TESTS=ON -DWITH_STORE=ON`

Run: `cmake --build build --target mock_client_core_test mooncake_master_mock_client -j2`

Expected: all parser tests pass; an example `put`-only config validates; an empty operation list exits with code `2`.

- [ ] **Step 5: Commit the build and contract.**

```bash
git add CMakeLists.txt mooncake-store/CMakeLists.txt mooncake-store/tools/mock_client
git diff --cached --check
git commit -m "[Store] Add mock client workload contract"
```

### Task 2: Implement deterministic keys and the control-plane shadow model

**Files:**
- Create: `mooncake-store/tools/mock_client/include/mock_client/deterministic_data.h`
- Create: `mooncake-store/tools/mock_client/src/deterministic_data.cpp`
- Create: `mooncake-store/tools/mock_client/include/mock_client/shadow_model.h`
- Create: `mooncake-store/tools/mock_client/src/shadow_model.cpp`
- Create: `mooncake-store/tools/mock_client/tests/deterministic_data_test.cpp`
- Create: `mooncake-store/tools/mock_client/tests/shadow_model_test.cpp`
- Modify: `mooncake-store/tools/mock_client/CMakeLists.txt` to add these sources/tests

**Interfaces:**
- `struct ObjectIdentity { std::string key; uint64_t generation; uint64_t logical_size; std::optional<uint64_t> checksum; };`
- `ObjectIdentity MakeObjectIdentity(uint64_t seed, std::string_view namespace_name, uint32_t client_index, uint64_t object_index, uint64_t generation, uint64_t logical_size);`
- `std::string DeterministicKey(...)`, `uint64_t DeterministicSize(...)`, and `uint64_t DeterministicChecksum(...)` are pure functions and use fixed-width serialization so results are stable across processes.
- `struct ReplicaSummary { ReplicaStatus status; std::string segment_name; std::string segment_endpoint; uint64_t descriptor_id; };`
- `struct ShadowEntry { ObjectIdentity identity; bool exists; uint32_t expected_replica_count; std::string owner_client_id; std::string owner_segment; std::vector<ReplicaSummary> replicas; bool uncertain; };`
- `class ShadowModel` exposes `ApplyPut`, `ApplyUpsert`, `ApplyRemove`, `MarkUnknown`, `Lookup`, `KeysForValidation`, and `ReconcileReplicaList(key, GetReplicaListResponse)`.
- Applying a known successful mutation is the only operation that advances the expected generation. `MarkUnknown` freezes the key and prevents another mutation until `ResolveUnknown` records an observed state.

- [ ] **Step 1: Write failing deterministic and lifecycle tests.** Assert same seed/namespace/index gives identical key/size/checksum, changed generation changes checksum, different namespaces cannot collide in the generated key prefix, Put creates an entry, Upsert increments generation, Remove marks absent, and unknown entries reject subsequent mutation.

```cpp
TEST(ShadowModelTest, UnknownMutationMustBeResolvedBeforeNextMutation) {
    ShadowModel model;
    const auto id = MakeObjectIdentity(7, "run-a", 0, 11, 1, 4096);
    model.MarkUnknown(id.key);
    EXPECT_FALSE(model.CanMutate(id.key));
    model.ResolveUnknown(id.key, ShadowObservation::Absent);
    EXPECT_TRUE(model.CanMutate(id.key));
}
```

- [ ] **Step 2: Run the focused tests to confirm failure.**

Run: `cmake --build build --target mock_client_core_test -j2`

Expected: the new tests fail until the pure generators and model transitions are implemented.

- [ ] **Step 3: Implement deterministic generation and shadow transitions.** Use the existing end-to-end client's reproducible seed approach as reference, but keep this model payload-free. Normalize descriptor data to summaries rather than retaining allocator buffers or transport objects.

- [ ] **Step 4: Run focused tests and an address-sanitized core test when available.**

Run: `cmake --build build --target mock_client_core_test -j2 && ctest --test-dir build -R mock_client_core_test --output-on-failure`

Expected: deterministic and lifecycle tests pass with no allocations that depend on real segment memory.

- [ ] **Step 5: Commit the deterministic model.**

```bash
git add mooncake-store/tools/mock_client
git diff --cached --check
git commit -m "[Store] Add mock client shadow model"
```

### Task 3: Add the master session, API adapter, and multi-round operations

**Files:**
- Create: `mooncake-store/tools/mock_client/include/mock_client/master_api.h`
- Create: `mooncake-store/tools/mock_client/src/master_client_api.cpp`
- Create: `mooncake-store/tools/mock_client/include/mock_client/master_session.h`
- Create: `mooncake-store/tools/mock_client/src/master_session.cpp`
- Create: `mooncake-store/tools/mock_client/include/mock_client/operation_executor.h`
- Create: `mooncake-store/tools/mock_client/src/operation_executor.cpp`
- Create: `mooncake-store/tools/mock_client/tests/operation_executor_test.cpp`
- Create: `mooncake-store/tools/mock_client/tests/master_session_test.cpp`
- Modify: `mooncake-store/tools/mock_client/CMakeLists.txt`

**Interfaces:**
- `class MasterApi` defines the narrow adapter used by the executor: `Connect`, `MountSegment`, `ReMountSegment`, `UnmountSegment`, `Ping`, `PutStart`, `PutEnd`, `PutRevoke`, `UpsertStart`, `UpsertEnd`, `UpsertRevoke`, `GetReplicaList`, `BatchGetReplicaList`, `Remove`, and `BatchRemove`. Each method returns the exact `tl::expected`/vector type from `MasterClient`.
- `class MasterClientApi final : public MasterApi` owns a `mooncake::MasterClient`, forwards calls, and exposes per-RPC method names for event logging. It must not add retry semantics around mutation calls.
- `struct SyntheticSegmentConfig` maps one JSON segment to `mooncake::Segment`; validation checks nonzero base, positive size, alignment, unique name/ID, and non-overlap within a client configuration.
- `class MasterSession` exposes `ConnectAndMount`, `PingOnce`, `HandlePingStatus`, `ReconnectAndRemount`, `Unmount`, and `Disconnect`. A `NEED_REMOUNT` status calls `ReMountSegment` with the exact original descriptors.
- `struct OperationRequest { OperationSelection selection; std::vector<ObjectIdentity> objects; std::string purpose; }`.
- `struct OperationResult { std::string request_id; uint64_t logical_ops; uint64_t batch_items; std::vector<ItemResult> items; bool transport_unknown; }`.
- `OperationExecutor::Execute(const OperationRequest&)` maps `put:single`/`put:batch`, `upsert:single`/`upsert:batch`, `get:single`/`get:batch`, and `remove:single`/`remove:batch` to the corresponding master calls. Put/Upsert performs Start then End; failed known starts are revoked; ambiguous mutation results mark every affected key unknown.
- The executor applies `ReplicaType::MEMORY`, configurable replica counts, tenant ID, and preferred segment fields from `MockClientConfig` without creating `Client` data-plane objects.

- [ ] **Step 1: Write fake-API tests for all selected operation forms.** The fake API should record call order and arguments. Assert single Put is `PutStart -> PutEnd`, partial batch Put ends only accepted items and revokes failed starts, Upsert uses Upsert methods, batch Get emits one batch call with N items, single Remove emits one call per object, and an injected timeout marks keys unknown without replay.

```cpp
TEST(OperationExecutorTest, BatchPutUsesStartEndAndRevokePerResult) {
    RecordingMasterApi api;
    api.batch_put_start_results = {ok_descriptors(), unexpected(ErrorCode::NO_AVAILABLE_HANDLE)};
    OperationExecutor executor(api, test_config(), test_shadow_model());
    auto result = executor.Execute(BatchRequest("put", {object_a, object_b}, 2));
    EXPECT_EQ(api.calls, (std::vector<std::string>{"BatchPutStart", "BatchPutEnd", "BatchPutRevoke"}));
    EXPECT_EQ(result.logical_ops, 2);
    EXPECT_TRUE(executor.shadow_model().Lookup(object_b.key)->uncertain == false);
}
```

- [ ] **Step 2: Run the fake-API tests to confirm failure.**

Run: `cmake --build build --target mock_client_core_test -j2`

Expected: operation executor tests fail because the adapter, session, and multi-round sequencing are not implemented.

- [ ] **Step 3: Implement the adapter and session.** Use `MasterClient` methods and UUID generation already used by `master_bench.cpp`. Keep connection/reconnect policy outside the adapter. Make session teardown wait for unmount completion and report unmount errors instead of hiding them.

- [ ] **Step 4: Implement operation sequencing and shadow updates.** For each response record per-item errors; never convert `RPC_TIMEOUT` or `RPC_FAIL` into a successful mutation. For `GetReplicaList`, compare the response against the shadow model and return a validation mismatch item rather than aborting the entire batch.

- [ ] **Step 5: Add the metadata-only capability probe.** The executable must mount one configured segment, run one Put/End/Get/Remove cycle, and emit `metadata_only_unsupported` with exit code `2` if the master rejects synthetic metadata or the response proves that the configured allocator cannot support the mode. Do not enable Cachelib for this path.

- [ ] **Step 6: Run focused unit tests and compile the executable.**

Run: `cmake --build build --target mock_client_core_test mooncake_master_mock_client -j2 && ctest --test-dir build -R mock_client_core_test --output-on-failure`

Expected: fake-API sequencing, remount, and unknown-result tests pass; executable links against `mooncake_store`.

- [ ] **Step 7: Commit the master operation layer.**

```bash
git add mooncake-store/tools/mock_client
git diff --cached --check
git commit -m "[Store] Add mock master operation executor"
```

### Task 4: Add event logging, faults, pacing, and explicit workload scheduling

**Files:**
- Create: `mooncake-store/tools/mock_client/include/mock_client/event_sink.h`
- Create: `mooncake-store/tools/mock_client/src/event_sink.cpp`
- Create: `mooncake-store/tools/mock_client/include/mock_client/fault_scheduler.h`
- Create: `mooncake-store/tools/mock_client/src/fault_scheduler.cpp`
- Create: `mooncake-store/tools/mock_client/include/mock_client/workload_controller.h`
- Create: `mooncake-store/tools/mock_client/src/workload_controller.cpp`
- Create: `mooncake-store/tools/mock_client/tests/fault_scheduler_test.cpp`
- Create: `mooncake-store/tools/mock_client/tests/workload_controller_test.cpp`
- Modify: `mooncake-store/tools/mock_client/src/main.cpp`
- Modify: `mooncake-store/tools/mock_client/CMakeLists.txt`

**Interfaces:**
- `struct Event` contains `run_id`, `client_id`, `request_id`, `attempt_id`, `purpose`, `rpc`, `logical_op`, `operation_form`, `batch_items`, `result`, `error_code`, monotonic start/end, wall timestamp, and optional fault/action fields.
- `class EventSink` provides thread-safe `Append(Event)`, `AppendAggregate(AggregateSnapshot)`, `Flush`, and `Close`; writes one JSON object per line and reports I/O failure to the caller.
- `class FaultScheduler` consumes deterministic time/event-number rules and exposes `BeforeCall`, `AfterCall`, `ShouldDisconnect`, `ReconnectDelay`, and `ShouldDropResponse`. It records the rule ID that fired and never fabricates an application success.
- `class WorkloadController` takes `std::vector<OperationSelection>`, an `OperationExecutor`, `MasterSession`, `ShadowModel`, `FaultScheduler`, and `EventSink`; `Run()` returns `RunSummary` with logical ops, RPCs, unknowns, backpressure decisions, and selected-operation totals.
- Scheduler selection uses a seeded weighted sequence. `put:single` means a one-key Put lifecycle; `put:batch` groups between configured batch bounds; `put+remove` can be represented by two weighted entries. A fixed sequence file, when configured, takes precedence over random weighted selection.
- Adaptive control reacts to `NO_AVAILABLE_HANDLE`, `TASK_PENDING_LIMIT_EXCEEDED`, `UNAVAILABLE_IN_CURRENT_STATUS`, `RPC_TIMEOUT`, and `RPC_FAIL` by reducing concurrency/rate, then ramps after a configured healthy window. Low/medium/full modes do not silently adapt; they only record errors and configured limits.

- [ ] **Step 1: Write event, fault, and scheduler tests.** Verify JSONL fields, concurrent append ordering by sequence number, deterministic weighted selection, fixed sequence replay, single-only and batch-only profiles, Put/Remove mixtures, delay/drop/disconnect schedules, no blind mutation retry, and adaptive rate reduction/recovery.

```cpp
TEST(WorkloadControllerTest, BatchPutOnlyProducesNoRemoveOrSinglePut) {
    auto selection = ParseOperationOverride("put:batch=1").value();
    FakeExecutor executor;
    WorkloadController controller(selection, executor, test_config(), ...);
    controller.RunFor(std::chrono::milliseconds(100));
    EXPECT_EQ(executor.count("put:batch"), controller.summary().logical_ops);
    EXPECT_EQ(executor.count("remove:single"), 0);
    EXPECT_EQ(executor.count("put:single"), 0);
}
```

- [ ] **Step 2: Run the tests to confirm failure.**

Run: `cmake --build build --target mock_client_core_test -j2`

Expected: event/fault/workload tests fail until the scheduler and sinks exist.

- [ ] **Step 3: Implement JSONL event output and non-blocking aggregate accounting.** Keep per-call output bounded and flush at configurable intervals; a failed sink must terminate the run with exit code `5` rather than lose audit evidence.

- [ ] **Step 4: Implement deterministic operation selection and the four profiles.** Include the effective selection in the start event and final summary. Count a batch as one RPC request but one logical operation per item. Keep session Ping/remount events separate from workload totals.

- [ ] **Step 5: Implement faults and adaptive control.** Wrap calls with pre/post delay and response-drop decisions, close/reconnect sessions for disconnect windows, freeze affected mutation keys, and invoke reconciliation before allowing those keys back into the scheduler.

- [ ] **Step 6: Run focused tests and command-line validation.**

Run: `cmake --build build --target mock_client_core_test mooncake_master_mock_client -j2 && ctest --test-dir build -R mock_client_core_test --output-on-failure`

Run: `build/mooncake-store/tools/mock_client/mooncake_master_mock_client --help`

Expected: all scheduler/fault tests pass; help lists `--config`, `--operations`, `--client-index`, and `--duration-sec`; invalid operation overrides exit `2`.

- [ ] **Step 7: Commit workload execution.**

```bash
git add mooncake-store/tools/mock_client
git diff --cached --check
git commit -m "[Store] Add mock client workload and fault control"
```

### Task 5: Implement Python orchestration, master metrics parsing, and reconciliation reports

**Files:**
- Create: `mooncake-store/tools/mock_client/python/mooncake_mock/__init__.py`
- Create: `mooncake-store/tools/mock_client/python/mooncake_mock/metrics.py`
- Create: `mooncake-store/tools/mock_client/python/mooncake_mock/reconcile.py`
- Create: `mooncake-store/tools/mock_client/python/mooncake_mock/report.py`
- Create: `mooncake-store/tools/mock_client/python/mooncake_mock/run.py`
- Create: `mooncake-store/tools/mock_client/python/tests/test_metrics.py`
- Create: `mooncake-store/tools/mock_client/python/tests/test_reconcile.py`
- Create: `mooncake-store/tools/mock_client/python/tests/test_report.py`
- Create: `mooncake-store/tools/mock_client/examples/put-only.json`
- Create: `mooncake-store/tools/mock_client/examples/batch-put-only.json`
- Create: `mooncake-store/tools/mock_client/examples/put-remove-mixed.json`
- Modify: `mooncake-store/tools/mock_client/README.md` with runner usage and report interpretation

**Interfaces:**
- `parse_prometheus(text: str) -> dict[str, float]` parses counters, gauges, and labeled samples without treating missing optional metrics as zero.
- `scrape_master(base_url: str) -> MasterSnapshot` captures `/metrics`, `/health`, `/role`, and `/version`, retaining raw response text and wall/monotonic timestamps.
- `detect_epoch(previous: MasterSnapshot | None, current: MasterSnapshot) -> EpochEvent` detects counter decreases, process restart evidence, role changes, and scrape gaps.
- `EXPECTED_COUNTERS` maps client event categories to `master_*` counter/item families, including the explicit Upsert/BatchRemove mappings from the spec.
- `reconcile_events(events, before, after, epoch) -> ReconciliationResult` computes sent, acknowledged, known-failed, unknown, master delta, lower/upper bounds, and classification (`exact_match`, `within_unknown_bound`, or `indeterminate_external_traffic`).
- `run.py` accepts `--config`, `--binary`, `--output-dir`, and optional `--operations` override; it starts one process per configured client, continuously scrapes all configured master metrics endpoints, sends SIGTERM for orderly drain, and retains child stdout/stderr.
- `report.py` writes `report.json` and `report.txt`, including selected operations/effective weights, `data_plane_not_exercised`, client totals, master counter deltas, unknown bounds, epochs, validation mismatches, and unavailable metrics as `not_exported`.

- [ ] **Step 1: Write Python parser/reconciliation tests.** Include labeled Prometheus samples, comments, omitted-zero counters, counter reset, leader switch, scrape gap, exact single Put, batch Put request/item mapping, Upsert-to-Put mapping, BatchRemove-to-Remove mapping, application errors, and timeout unknown bounds.

```python
def test_batch_remove_uses_remove_item_delta_without_batch_request_counter():
    events = [{"rpc": "BatchRemove", "batch_items": 4, "result": "ok"}]
    before = {"master_remove_requests_total": 10}
    after = {"master_remove_requests_total": 14}
    result = reconcile_events(events, before, after, stable_epoch())
    assert result.classification == "exact_match"
    assert result.master_items == 4
    assert result.master_batch_requests is None
```

- [ ] **Step 2: Run the Python tests to confirm failure.**

Run: `python3 -m pytest mooncake-store/tools/mock_client/python/tests -q`

Expected: FAIL because the parser, epoch handling, and report functions do not yet exist.

- [ ] **Step 3: Implement parser, snapshots, epoch handling, and raw evidence persistence.** Parse only numeric sample values, preserve labels in a stable key form, detect decreases rather than wrapping them, and retain every raw endpoint response under the run output directory.

- [ ] **Step 4: Implement event-to-master reconciliation.** Use only operation events with `purpose=workload` for workload totals; keep `validation` and `session` traffic in separate sections. Require stable epochs and an explicit no-external-traffic flag for `exact_match`. Use unknown attempts as an inclusive upper bound and mark shared-cluster traffic indeterminate.

- [ ] **Step 5: Implement the process runner and examples.** Pass `--operations` through to the C++ process, assign unique output/event paths per client, stop all children on failure, and write an invocation manifest containing the new `madsys-*` host names. Examples must demonstrate only Put, only BatchPut, and Put/Remove mixed operation sets.

- [ ] **Step 6: Run Python tests and example validation.**

Run: `python3 -m pytest mooncake-store/tools/mock_client/python/tests -q`

Run: `python3 -m mooncake_mock.run --config mooncake-store/tools/mock_client/examples/put-only.json --dry-run --output-dir /tmp/mooncake-mock-dry-run`

Expected: all tests pass; dry-run prints the exact selected operation set and produces an invocation manifest without starting a client.

- [ ] **Step 7: Commit orchestration and reporting.**

```bash
git add mooncake-store/tools/mock_client
git diff --cached --check
git commit -m "[Store] Add mock client orchestration and reconciliation"
```

### Task 6: Add integration coverage, operational documentation, and release verification

**Files:**
- Create: `mooncake-store/tools/mock_client/tests/mock_master_integration_test.cpp`
- Create: `mooncake-store/tools/mock_client/tests/mock_client_smoke.sh`
- Modify: `mooncake-store/tools/mock_client/CMakeLists.txt` to register the integration test when its prerequisites are available
- Modify: `mooncake-store/tools/mock_client/README.md` with build, configuration, operation-selection, capability-probe, exit-code, and report examples
- Modify: `docs/source/getting_started/observability.md` only if the new report fields need a user-facing cross-reference

**Interfaces:**
- The integration test starts the repository's in-process master test harness or a local master fixture, runs a low-rate one-client Put/Upsert/Get/Remove lifecycle, and asserts final shadow reconciliation.
- `mock_client_smoke.sh` is a read-only/explicitly opt-in smoke wrapper. It requires a supplied master RPC and metrics endpoint, never assumes a production endpoint, and emits a clear prerequisite error when absent.
- README documents these concrete operation selectors: `put:single`, `put:batch`, `upsert:single`, `get:batch`, `remove:single`, and `put:single=3,remove:single=1`.

- [ ] **Step 1: Write the integration assertions and smoke-script checks.** Assert that selected operation sets produce only their mapped RPC families, metadata validation catches a wrong replica count/segment endpoint, unknown mutation results require reconciliation, and reports contain `data_plane_not_exercised=true` and the correct host names.

- [ ] **Step 2: Run the integration test before implementation to establish the expected failure.**

Run: `cmake --build build --target mock_master_integration_test -j2`

Expected: the target is skipped or fails with a missing fixture until the harness wiring is added; no test may silently contact a remote master.

- [ ] **Step 3: Implement the local fixture and smoke wrapper.** Reuse existing `InProcMaster`/master test setup patterns where possible. Make remote smoke execution an explicit command-line mode and keep it out of default CTest.

- [ ] **Step 4: Document configuration and operation selection.** Show JSON and `--operations` examples, explain logical-op versus RPC/item counts, explain exact versus bounded reconciliation, list master metric mappings, and state that control-plane PASS is not data-plane validation.

- [ ] **Step 5: Run the complete verification set.**

Run: `cmake --build build --target mock_client_core_test mock_master_integration_test mooncake_master_mock_client -j2`

Run: `ctest --test-dir build -R 'mock_client' --output-on-failure`

Run: `python3 -m pytest mooncake-store/tools/mock_client/python/tests -q`

Run: `pre-commit run --files CMakeLists.txt mooncake-store/CMakeLists.txt mooncake-store/tools/mock_client/CMakeLists.txt mooncake-store/tools/mock_client/include/mock_client/config.h mooncake-store/tools/mock_client/src/config.cpp mooncake-store/tools/mock_client/python/mooncake_mock/metrics.py mooncake-store/tools/mock_client/python/mooncake_mock/report.py`

Expected: focused C++/Python tests pass, integration tests use only their local fixture, pre-commit passes, and no generated archive or unrelated branch files appear in `git diff`.

- [ ] **Step 6: Perform a real-master low-speed smoke and record evidence.** Use a dedicated test window mapped to `madsys-01`/`madsys-03`/`madsys-02`, run the metadata-only capability probe, execute each example operation set separately, scrape all master endpoints before/during/after, reconcile the counters, and clean up all keys and synthetic segments.

- [ ] **Step 7: Commit documentation and integration coverage.**

```bash
git add mooncake-store/tools/mock_client docs/source/getting_started/observability.md
git diff --cached --check
git commit -m "[Store] Verify and document master mock client"
```

## Final Review Checklist

- [ ] Every spec section maps to at least one task: architecture, synthetic segments, session protocol, deterministic shadow model, JSON configuration, explicit operation selection, event logging, master reconciliation, faults, exit codes, and staged testing.
- [ ] A placeholder scan over the plan reports no unfinished markers, vague cross-task references, or test steps without concrete commands/assertions.
- [ ] `git diff --check` and focused pre-commit pass on all changed files.
- [ ] The final report can answer both “what logical operation mix did the test request?” and “what did master actually count?” without relying on client-side totals alone.
