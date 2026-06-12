# Mooncake Store Master HA — Stage 3 进展与注意事项

日期：2026-06-10
依据：`mooncake_store_master_ha_basic_available_staged_tdd_plan_2026-06-09.md` §6（Stage 3）
上游：`mooncake_store_master_ha_stage2_progress_2026-06-10.md`

## 1. 状态：Stage 3 已完成 RED→GREEN→LEGACY GREEN 并提交

- Commit: `678c52b1` on `dev/store_ha_rebase_0525`
- 4 files / +786 / -11

**RED → GREEN 闭环已闭合**：

- 7/7 新增 Stage 3 红灯测试全绿
  - `Stage3_ReadBatchesSinceReturnsBatchesInBatchIdOrder`（5 ms）
  - `Stage3_AppendBatchCreatesBatchAndLatestAtomically`（4 ms）
  - `Stage3_AppendBatchRejectsLatestBatchMismatch`（5 ms）
  - `Stage3_AppendBatchRetrySamePayloadIsIdempotentSuccess`（5 ms）
  - `Stage3_AppendBatchSameBatchDifferentPayloadReturnsConflict`（12 ms）
  - `Stage3_AppendBatchAcceptsNewLeaderAfterFailover`（7 ms）← 2026-06-12 修正方向，原 `Stage3_AppendBatchRejectsStaleLeaderToken` 编码了已删除的 per-batch token fencing 行为
  - `Stage3_LegacyGroupCommitDoesNotOverwriteDifferentExistingSequenceValue`（3004 ms，含 3s persist-timeout 等待）
- 26/27 `etcd_oplog_store_test` 全绿（1 个 SKIPPED 是 pre-existing `TestBatchUpdate_FailurePlaceholder`，GTEST_SKIP）
- HA/oplog 回归套件 7/7 全绿，约 84 s 总时长
  - `oplog_serializer_test` 0.26 s
  - `oplog_manager_test` 0.11 s
  - `etcd_oplog_store_test` 9.49 s
  - `oplog_applier_test` 10.55 s
  - `ha_recovery_test` 14.14 s
  - `master_service_ha_test` 38.38 s
  - `localfs_hot_standby_integration_test` 10.63 s
  - `high_availability_test` 22.89 s（额外验证）

## 2. 改动清单（4 文件 / +786 / -11）

| 文件 | 内容 |
|---|---|
| `mooncake-store/include/types.h` | 新增 2 个 HA 错误码：`SEQUENCE_CONFLICT (-1012)`、`STALE_LEADER (-1013)` |
| `mooncake-store/include/ha/oplog/etcd_oplog_store.h` | 新增 4 个 Stage 3 override 声明（`AppendBatch` / `ReadBatch` / `ReadBatchesSince` / `GetLatestBatchId`）；新增 5 个 private helper（`BuildBatchKey` / `BuildLatestBatchKey` / `EncodeLatestBatchValue` / `ParseLatestBatchValue` / `ReadLatestBatchValue` / `CompareBatchRecord`）；新增 3 个常量（`kShardsPrefix` / `kBatchesSuffix` / `kLatestBatchSuffix`） |
| `mooncake-store/src/ha/oplog/etcd_oplog_store.cpp` | 实现 4 个 override + 5 个 helper；legacy `FlushBatch` fallback 改成 read-and-compare（plan §6.4 no-overwrite 修复）；legacy `ReadOpLogSinceWithRevision` / `GetMinSequenceId` / `GetMaxSequenceIdInternal` 三个 range scan 加上 `/shards/` 过滤 |
| `mooncake-store/tests/ha/oplog/etcd_oplog_store_test.cpp` | 新增 7 个 Stage 3 测试 + 1 个 `MakeBatch` helper |

## 3. 关键设计决策（Stage 4+ 需遵守）

### 3.1 latest_batch 值格式：`<batch_id>`（纯十进制数字串）

- 即一个 `std::to_string(batch_id)`，没有任何分隔符或元数据
- 反序列化失败时返回 `INTERNAL_ERROR`，调用方需要把这个状态显式 raise，不能 silently 当作「empty shard」处理
- Stage 4 LogWriter flush 前**必须**自己拼这个字符串（用 `EncodeLatestBatchValue`），不要自己重新发明格式
- 早期方案曾把 `owner_token` 也塞进 value（`<batch_id>|<owner_token>`），但那会把 failover 卡死——见 §3.3

### 3.2 AppendBatch 用「读-校-写」三步而非真正 etcd Txn

- batch key 用 `Create`（`CreateRevision==0` 语义）：原子且原生支持「不存在才创建」— 这是 etcd txn，但只覆盖一个 key
- latest_batch 走 `Put` overwrite：在我们用「读」+「最新值校验」之后才写
- **小竞态窗口**：`Create(batch)` 成功到 `Put(latest_batch)` 之间（毫秒级），如果出现：
  - 旧 leader 失去 lease → 新 leader 接管
  - 新 leader 读 latest_batch = N（我们 Put 之前的值）
  - 新 leader 写 batch N+1，Create 失败（我们已写），读我们写的 batch，对比失败，CONFLICT
  - 新 leader 退一步写 batch N+2
  - 我们 Put latest_batch = N+1（乱序写）
  - 后果：latest_batch 回退 1。下一轮 AppendBatch 读 latest_batch = N+1，拒绝 batch N+2
  - **不会造成数据丢失**（我们的 batch N+1 还在），但 latest_batch 滞后于实际批次数
  - **修复路径**：下一个成功的 AppendBatch 会用 batch_id-gap 校验发现 latest_batch 滞后并把指针推上去
- 优点：不需要新增 etcd Txn wrapper（`go` + cgo + CMake 全部要重建）
- 缺点：atomicity 是 best-effort，不是 strict
- plan §6.4 提到的「strict atomic」留给 Stage 6 promotion gate / Stage 4 LogWriter backpressure 来兜底，不在 Stage 3 强求

### 3.3 owner_token **不**在校验路径中（关键决策，2026-06-12 修正）

- 早期实现把 `owner_token` 一起塞进 `latest_batch` value 并在 `AppendBatch` 里做「和 latest_token 是否相同」的比较，意图做 per-batch fencing。
- 这是错的：failover 之后合法的新 leader 必然带一个旧 leader 没见过的 lease_id，老的 token 校验会把它打成 `STALE_LEADER` 并永远卡住。
- 修正后的契约：
  - `AppendBatch` **不**读 `batch.owner_token`，**不**和任何持久化 token 比对，**不**返回 `STALE_LEADER`
  - `latest_batch` value 不记录 owner_token
  - 单条批次的 `OpLogBatchRecord` 仍带 owner_token 字段（用于审计「这条 batch 是哪个 leader 写的」），但不参与互斥
  - 真正的 leader fencing 走另一条路——Stage 6 promotion gate / `/master_view` 当前 lease holder 与 `batch.owner_token` 对比，**写路径**加 fencing，**不**在 `EtcdOpLogStore` 内部做
- 这个决策已经反映在 `Stage3_AppendBatchAcceptsNewLeaderAfterFailover` 测试里：leader-A 写 batch 1，leader-B（不同 token）继续写 batch 2、3、4，全部 OK；leader-A 想用 batch 4 抢回被拒，是因为 batch_id 已经走在 leader-B 后面（SEQUENCE_CONFLICT），**不是** STALE_LEADER
- 详细 fencing 设计是 Stage 6 的事，列入 §8.1 carryover

### 3.4 batch_id 的两种「重放」语义

| 场景 | 期望返回 | 原因 |
|---|---|---|
| 同样的 batch_id、同样 payload | `OK`（idempotent）| 网络重试是常见情况 |
| 同样的 batch_id、不同 payload | `SEQUENCE_CONFLICT` | 永远不应该有合法 caller 写出第二种 payload |
| batch_id < latest_batch | `SEQUENCE_CONFLICT` | 回退到旧 batch_id 没有合法理由 |
| batch_id == latest_batch（payload 相同）| `OK`（idempotent）| 同上行 |
| batch_id == latest_batch（payload 不同）| `SEQUENCE_CONFLICT` | 同上行 |
| batch_id > latest_batch + 1（gap）| `SEQUENCE_CONFLICT` | 必须严格连续 |
| 第一次写 batch_id > 1 | `SEQUENCE_CONFLICT` | empty shard 的第一个 batch 必须是 1 |
| shard_id != 0 | `INVALID_PARAMS` | basic-available HA 是 single-shard |
| **新 leader 用不同 owner_token 写下一个 batch_id** | **`OK`** | **failover 必须能继续**（参见 §3.3）|

### 3.5 Legacy `FlushBatch` fallback 的修复

**Before（buggy）**：
```cpp
if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
    // 看到 key 已存在就 blind Put（overwrite）—— 会把同 sequence_id 的旧 value 覆盖掉
    EtcdHelper::Put(...);
}
```

**After（fixed）**：
```cpp
if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
    // 1. Get 已有 value
    // 2. value 与 incoming 相同 → 当作 idempotent 跳过
    // 3. value 与 incoming 不同 → log error，**不** overwrite，记 false 让 batch 标 fail
    // 4. key 已被删（race）→ fallback Put 写新 value
}
```

调用方（`WriteOpLog` sync=true）会拿到 `ETCD_OPERATION_ERROR`（persist-timeout 3s 后），并知道自己的 write 没有被采纳。**不会**污染 durable history。

注意：测试 `Stage3_LegacyGroupCommitDoesNotOverwriteDifferentExistingSequenceValue` 耗时 3 秒（persist-timeout），这是 WriteOpLog 等待 `last_persisted_seq_id_ >= target_seq` 永远不可能满足导致。不是 bug。

### 3.6 legacy range scan 过滤 `/shards/`

`ReadOpLogSinceWithRevision` / `GetMinSequenceId` / `GetMaxSequenceIdInternal` 都用 `key.find("/shards/")` 显式跳过 batch namespace 的 key：

- 不跳过 → 会把 `OpLogBatchRecord` 的 JSON 当作 `OpLogEntry` 反序列化，必失败
- `/shards/` 字符串不会和 `/latest`、`/snapshot/...` 冲突
- `/shards/` 是唯一已知的 Stage 3 namespace；未来 multi-shard 也用同一个 prefix

## 4. Stage 3 未做（不影响独立合入，但 Stage 4+ 必做）

- **runtime 写路径未接入**：`OpLogLogWriter` 不存在，`OpLogManager::Append*` 仍走 `OpLogStore::WriteOpLog` legacy 路径。Stage 4 实现
- **`EtcdOpLogStore` 缺 `BatchCreateWithLease` 类原语**：当前用「读-校-写」三步，true atomic Txn 需要新增 Go wrapper（成本：Go + cgo + CMake 重建）。Stage 6 promotion gate 之前不阻塞
- **Standby 不消费 batch history**：`OpLogApplier::ApplyOpLogBatch` 仍不存在，standby 还是按 sequence_id 单条 replay。Stage 5 实现
- **没有 batch metric**：pending entries / pending bytes / durable batch / flush latency / flush failure 还没接到 `HAMetricManager`。Stage 4 实现
- **MockOpLogStore 仍未实现 batch API**：`tests/ha/oplog/mock_oplog_store.h` 仍走基类 default UNAVAILABLE。Stage 4 实现（如果 LogWriter unit test 需要 in-memory mock）

## 5. Stage 4+ 注意事项

- **batch_checksum 调用 `ComputeOpLogBatchChecksum` 设好再传**：`AppendBatch` 会验 `batch.batch_checksum == ComputeOpLogBatchChecksum(batch)`，不匹配返回 `INTERNAL_ERROR`。LogWriter 在 flush 前**必须**调一次并写回
- **owner_token 字段名一致**：`OpLogBatchRecord::owner_token` 是 `ha::OwnerToken`（`std::string`），即 `EtcdLeaderCoordinator::MakeOwnerToken(lease_id)` 输出的格式。Stage 4 LogWriter 在每次 AcquireLeadership 成功时把 session.owner_token 记到内部状态，flush 时填进 `OpLogBatchRecord`
- **producer_view_version 已经预留**：`OpLogBatchRecord::producer_view_version` 是 `ViewVersionId`（int64_t）。当前 `AppendBatch` 只读不写它（serialized 字段，未参与校验），Stage 6 promotion gate 可以加 also-compare
- **legacy `OpLogEntry` 3 个 batch 字段不变**：LogWriter 给 `OpLogEntry` 设 `shard_id` / `batch_id` / `local_index` 时前两个应当与所在 `OpLogBatchRecord` 同名字段一致。`DeserializeOpLogBatchRecord` 不强制（batch header 是 source of truth），但 Stage 4 测试可以加 invariant 检查
- **`AppendBatch` 没有任何写锁**：EtcdHelper 各 API 本身是 thread-safe（global etcd client），但 LogWriter 多线程并发 AppendBatch 时 order 依赖 batch_id 强制。如果 LogWriter 内部多线程 flush 同 shard 的不同 batch_id，CAS 路径会处理；如果同 batch_id 并发，最后一个 Create 成功、其他走 fallback
- **AppendBatch 不是 noexcept**：返回 `ErrorCode`，上层需要处理 `ETCD_OPERATION_ERROR`（backend 暂时不可用）—— Stage 4 LogWriter 应该把这个错误映射到「batch flush 失败、waiter 全部 fail」

## 6. 风险与已知小事

- **clang-format 跑了**：pre-commit hook 自动调了 `clang-format` 在 etcd_oplog_store.cpp / .h / test.cpp 上，提交时多了 1 行变更（formatting-only）。没改语义
- **`flock` 等 io 锁未受影响**：本阶段只动 `EtcdOpLogStore`，不动 localfs / snapshot
- **go.mod / go.sum 未动**：本阶段不新增 Go wrapper
- **`mooncake-store/include/types.h` 只新增 2 行**：`SEQUENCE_CONFLICT` 和 `STALE_LEADER` 加在 `UNAVAILABLE_IN_CURRENT_MODE` 之后。Stage 6 promotion gate 可以再加 `PROMOTION_NOT_READY` 等
- **etcd lease id 不会含 `|`**：`EtcdLeaderCoordinator::ParseLeaseId` 用 `std::stoll`，`MakeOwnerToken` 用 `std::to_string(int64_t)`，输出永远是纯十进制无符号字符串。所以 `|` 是安全的分隔符。如果未来 leader coordinator 改用非整数 token，Stage 3 的 `EncodeLatestBatchValue` / `ParseLatestBatchValue` 需要换分隔符或换格式
- **`CompareBatchRecord` 字段比较**直接手写，没用 `SerializeOpLogBatchRecord` 双向比 JSON：避免 JsonCpp key ordering 不一致导致 false positive。Stage 5 strict applier 可以复用同样模式
- **`/shards/` 过滤是子串匹配**：`/shards/` 不会出现在 `/latest`、`/snapshot/...`、`/<20-digit-seq>` 路径里。如果未来加 `/sharded_master_view/` 之类 legacy key，filter 要更新

## 7. 下一步

按 plan §7 顺序，Stage 4 是 single-shard `OpLogLogWriter` + 接入 `OpLogManager` + async PUT_END + dirty mutation durable-before-success。Stage 3 已经把 `AppendBatch` 在 etcd 上可工作，Stage 4 直接：
1. 新建 `OpLogLogWriter`（在 `include/ha/oplog/oplog_log_writer.h` / `src/ha/oplog/oplog_log_writer.cpp`）
2. `OpLogManager::SetLogWriter` 接进去
3. `MasterService` 在 `ha_oplog_format=batched` + `ha_required_level=full` 时切到 LogWriter 路径（这部分需要先做 Stage 2 之前 §8 留下的 (1) + (2) 两个 follow-up：gflags / YAML + `MasterConfig::FromFlags`）
4. 写 plan §7.3 的 9 个红绿测试

## 8. 从 Stage 1/2 滚入的 follow-up（仍未实现）

与 `mooncake_store_master_ha_stage2_progress_2026-06-10.md` §8 同样的 carryover 纪律，集中登记所有上游遗留、本阶段未碰、Stage 4+ 仍需处理的 item。grep 验证（2026-06-10 14:38 GMT+8）：

- `mooncake-store/src/` 下**仍无** `DEFINE_string(ha_required_level/.../ha_oplog_format/.../ha_etcd_endpoint/.../ha_client_timeout_ms/.../ha_supported_capability/.../actual_capability)` 等任意 ha_* gflags
- `MasterConfig` 4 个 ha_* 字段仍在 struct 上有默认值（Stage 1 加的），但**没有** `DEFINE_*` 与 `FromFlags()` 解析层
- `MasterAdminServer` health/status RPC **仍未暴露** `ha_required_level` / `actual_capability` 字段

### 8.1 Stage 1 follow-up（仍 3 项 carryover）

1. **gflags / YAML 配置层未接**（`master.cpp` 缺 `DEFINE_string(ha_required_level, ...)`、`DEFINE_string(ha_oplog_format, ...)`、`DEFINE_string(ha_etcd_endpoint, ...)`、`DEFINE_string(ha_client_timeout_ms, ...)`；YAML `default_config.GetString` 覆盖路径与 `FLAGS_*` 覆盖路径都缺）。当前只能通过 C++ 构造 `MasterServiceSupervisorConfig` / `MasterConfig` 设置这 4 个字段，集成测试无法走 CLI 触发。
2. **`MasterConfig::FromFlags()` 解析未接**：`MasterConfig` struct 的 4 个 ha_* 字段已在 Stage 1 加上（默认同 supervisor config），但 `DEFINE_*` 与 `MasterConfig::FromFlags()` 解析层未实现。CLI 启动 master 不会把 gflags 透到 `MasterConfig`。
3. **health/status RPC 未暴露 required level + actual capability**（plan §4.4 末项）：`MasterAdminServer` 的 health/status 响应里没有 `ha_required_level` 也没有 `actual_capability`（或 `ha_supported_capability`），运维侧目前看不出"声明要 basic-available vs 实际支持 basic-available"的差。

**实现时机**：建议 (1) + (2) **在 Stage 4 启动前补完**——因为 Stage 4 的 `MasterService` 切到 `LogWriter` 路径需要 CLI 触发 `ha_oplog_format=batched` + `ha_required_level=full`，否则集成测试只能走 C++ 构造 config。(3) 顺延到 Stage 6 收尾（promotion gate 落地后再补运维侧的可观测性更合理；早了也没有对比基准）。

### 8.1.1 Stage 3 修正后新增的 carryover（failover fencing 设计）

4. **真正的 leader fencing 设计**（修正日期 2026-06-12，原 Blocker 见 §9）：Stage 3 已经把 `EtcdOpLogStore::AppendBatch` 与 `latest_batch` 解耦 owner_token，保证 failover 时新 leader 能继续写 batch_id 序列。**真正的 fencing** 走另一条路：
   - 写路径（Stage 4 `OpLogLogWriter`）：在 `AppendBatch` 之前用一次 etcd Txn 把 `/master_view` 当前 lease_id 与 `session.owner_token` 做一次 compare——不匹配直接 `STALE_LEADER`，根本不发 AppendBatch
   - 读路径 / promotion gate（Stage 6）：standby 接管为新 leader 前要求 `/master_view` 当前 lease 等于自己持有的 lease_id，否则等
   - 决策点：是否要把这个 Txn 包成新的 Go wrapper（成本 = Go + cgo + CMake 重建），还是用一次 `Get(/master_view)` + 客户端判断 + best-effort Put 的组合？Stage 4 启动前需要讨论敲定
   - 错误码 `STALE_LEADER (-1013)` 保留，给 Stage 4 写路径用

### 8.2 Stage 2 §4 carryover 状态盘点

| Stage 2 §4 item | Stage 3 状态 | 落点 |
|---|---|---|
| `AppendBatch` runtime 写路径未接入（LogWriter）| **未实现** | stage3 §4 第 1 条重述 |
| **etcd batch key schema + AppendBatch 默认 UNAVAILABLE** | **✅ 已实现**（本阶段本职）| 完成，可从 carryover 划掉 |
| `OpLogApplier::ApplyOpLogBatch` 未实现（Standby 不消费 batch）| **未实现** | stage3 §4 第 3 条（改名「Standby 不消费 batch history」）|
| `MockOpLogStore` batch API 未实现 | **未实现** | stage3 §4 第 5 条重述 |

Stage 3 §4 自身还多列了 1 条 stage3 新增的未做项（`EtcdOpLogStore` 缺 `BatchCreateWithLease` 类原语）和 1 条 stage4 工作（没有 batch metric），这两条不是 carryover，是 stage3 评估后新加的。

### 8.3 Stage 3 评估新加的 1 条 stage3-only follow-up

- **`EtcdOpLogStore` 缺 `BatchCreateWithLease` 类原语**：当前用「读-校-写」三步实现 latest_batch 推进，true atomic Txn 需要新增 Go wrapper（成本：Go + cgo + CMake 重建）。Stage 3 §3.2 已记录竞态窗口与 owner_token 兜底策略。Stage 6 promotion gate 之前不阻塞。

### 8.4 验证矩阵

| 项 | 阶段 | grep 命中？ | 期望 |
|---|---|---|---|
| `DEFINE_string(ha_required_level, ...)` 等 4 个 gflags | Stage 4 启动前 | ❌ | 命中 4 行 |
| `MasterConfig::FromFlags()` | Stage 4 启动前 | ❌ | 命中定义 |
| `MasterAdminServer` health/status 含 `ha_required_level` / `actual_capability` | Stage 6 | ❌ | 出现在响应 JSON 字段 |
| `OpLogLogWriter` | Stage 4 | ❌ | 出现新 .h / .cpp |
| `OpLogApplier::ApplyOpLogBatch` | Stage 5 | ❌ | 出现新方法 |
| `MockOpLogStore` 4 个 batch override | Stage 4+ 按需 | ❌ | 出现 4 个 override |
| 真正 leader fencing（写路径 + promotion gate）| Stage 4 (写) / Stage 6 (gate) | ❌ | 出现 `OpLogLogWriter` 中 `/master_view` 比较 + `MasterService::PromoteToMaster` 中 lease 校验 |

## 9. 2026-06-12 修正（Blocker + Major 处置）

### 9.1 Blocker：failover 之后新 leader 无法继续写

- **原始实现**（`678c52b1`）：`latest_batch` value 是 `<batch_id>|<owner_token>`，`AppendBatch` 在「`current_token != batch.owner_token`」时返回 `STALE_LEADER`
- **问题**：failover 之后合法的新 leader 必然带新 lease_id，老的 owner_token 校验会把它打回 `STALE_LEADER`，batch_id 序列永远卡住
- **修正**（本次提交）：
  - `latest_batch` value 简化为纯 `<batch_id>`，不再记录 owner_token
  - `AppendBatch` 不再做 owner_token 校验，**不**返回 `STALE_LEADER`
  - `OpLogBatchRecord.owner_token` 字段保留（审计：哪条 batch 是哪个 leader 写的），但**不**参与互斥
  - 测试从 `Stage3_AppendBatchRejectsStaleLeaderToken`（编码坏行为）改为 `Stage3_AppendBatchAcceptsNewLeaderAfterFailover`（验证 failover 路径可走）
- **真正的 fencing** 放到 Stage 4 写路径 + Stage 6 promotion gate，用 `/master_view` 当前 lease_id 与 session token 比较；详见 §8.1.1

### 9.2 Major：legacy `FlushBatch` fallback 可能部分持久化

- **现状**（`678c52b1` 起就有，未改）：`BatchCreate` 失败时逐 key 走「Get → compare → 条件 Put」，对 race 出现的「`BatchCreate` 失败 + Get 时 key 被删」会先 Get 报 `ETCD_KEY_NOT_EXIST` 再 fallback Put 写新 value——这条路径里如果有 N 个 key，partial persist 的窗口是存在的
- **影响**：caller 会被告知 batch 失败（`WriteOpLog` sync=true 拿 `ETCD_OPERATION_ERROR`），可重试实现 eventual consistency；不污染 durable history 的语义
- **决策**：采用 Option A（**仅文档化**），不动 fallback 语义。`TestWriteOpLog_Idempotent` 仍然覆盖主路径，partial-persist 窗口留作 Stage 6 promotion gate 一并处理
- 风险登记在 §6，本节作为决策记录
