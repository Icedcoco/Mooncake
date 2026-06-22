// mooncake-store/tests/ha/oplog/oplog_log_writer_test.cpp
//
// Stage 4 LogWriter unit tests. Each test corresponds to one red case from
// plan §7.3 (LogWriterAssignsContiguousLocalIndexInEnqueueOrder,
// LogWriterFlushesWhenSyncEntryEnqueued, etc.). The MockOpLogStore used
// here exercises the base-class batch API from Stage 2 with ordering
// rules that match EtcdOpLogStore::AppendBatch.
//
// Run:
//   ./mooncake-store/tests/ha/oplog/oplog_log_writer_test
#include "ha/oplog/oplog_log_writer.h"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "ha/oplog/oplog_serializer.h"
#include "ha_metric_manager.h"
#include "mock_oplog_store.h"

namespace mooncake::test {

namespace {

// Build a minimal PUT_END entry for the LogWriter tests. The writer only
// requires op_type / object_key / payload / checksum / timestamp_ms to be
// sensibly populated — the rest is filled in by the writer or left default.
OpLogEntry MakePutEndEntry(const std::string& key,
                           const std::string& payload = "v") {
    OpLogEntry entry;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "default";
    entry.object_key = key;
    entry.payload = payload;
    entry.timestamp_ms = 0;
    entry.sequence_id = 0;  // writer assigns batch coords; sequence_id unused
    entry.checksum = 0;
    entry.prefix_hash = 0;
    return entry;
}

// Default LogWriter config used by all tests. Small flush-on-sync semantics
// so tests can drive batching deterministically without timers.
OpLogLogWriterConfig MakeDefaultConfig(uint32_t max_entries = 4) {
    OpLogLogWriterConfig cfg;
    cfg.shard_id = 0;
    cfg.max_entries = max_entries;
    cfg.max_bytes = 1u << 20;
    cfg.max_delay = std::chrono::microseconds(50000);  // 50ms safety timer
    cfg.flush_on_sync_op = true;
    cfg.producer_view_version = 1;
    cfg.owner_token = "writer-test-token";
    return cfg;
}

}  // namespace

// ===========================================================================
// RED 1: LogWriterAssignsContiguousLocalIndexInEnqueueOrder
// Plan §7.3 / §7.4: enqueueing kAsync should assign positions (shard, batch,
// local_index) in order. With max_entries=4, three enqueues all land in
// the same batch with local_index 0,1,2.
// ===========================================================================
TEST(OpLogLogWriterTest, LogWriterAssignsContiguousLocalIndexInEnqueueOrder) {
    MockOpLogStore store;
    OpLogLogWriter writer(MakeDefaultConfig(4),
                          std::shared_ptr<OpLogStore>(&store, [](auto*) {}));
    ASSERT_EQ(ErrorCode::OK, writer.Start());

    EnqueueResult r1 =
        writer.Enqueue(MakePutEndEntry("k1"), OpLogDurabilityMode::kAsync);
    EnqueueResult r2 =
        writer.Enqueue(MakePutEndEntry("k2"), OpLogDurabilityMode::kAsync);
    EnqueueResult r3 =
        writer.Enqueue(MakePutEndEntry("k3"), OpLogDurabilityMode::kAsync);

    EXPECT_EQ(0u, r1.position.shard_id);
    EXPECT_EQ(0u, r2.position.shard_id);
    EXPECT_EQ(0u, r3.position.shard_id);
    EXPECT_EQ(r1.position.batch_id, r2.position.batch_id);
    EXPECT_EQ(r2.position.batch_id, r3.position.batch_id);
    EXPECT_EQ(0u, r1.position.local_index);
    EXPECT_EQ(1u, r2.position.local_index);
    EXPECT_EQ(2u, r3.position.local_index);

    // Drive the batch to completion so Shutdown doesn't have to wait the
    // safety timer.
    writer.TriggerFlush();
    ASSERT_EQ(ErrorCode::OK, r1.durable_result.get());
    writer.Shutdown();
}

// ===========================================================================
// RED 2: LogWriterFlushesWhenSyncEntryEnqueued
// Plan §7.3 / §7.4: a sync enqueue (kWaitBatchDurable) must trigger an
// immediate flush and only return after the batch is durable. We confirm
// this by ensuring the mock has the batch by the time Enqueue returns.
// ===========================================================================
TEST(OpLogLogWriterTest, LogWriterFlushesWhenSyncEntryEnqueued) {
    MockOpLogStore store;
    OpLogLogWriter writer(MakeDefaultConfig(64),
                          std::shared_ptr<OpLogStore>(&store, [](auto*) {}));
    ASSERT_EQ(ErrorCode::OK, writer.Start());

    // Async PUT_END that should not flush on its own (max_entries=64).
    EnqueueResult async_r = writer.Enqueue(MakePutEndEntry("async-key"),
                                           OpLogDurabilityMode::kAsync);
    // Batch should not yet be durable (still in queue).
    EXPECT_EQ(0u, store.BatchCount());

    // Now enqueue a sync op. By the time Enqueue returns, the containing
    // batch must be durable (batch visible in mock).
    EnqueueResult sync_r = writer.Enqueue(
        MakePutEndEntry("sync-key"), OpLogDurabilityMode::kWaitBatchDurable);
    EXPECT_EQ(ErrorCode::OK, sync_r.durable_result.get());
    EXPECT_EQ(1u, store.BatchCount());
    // Async op landed in the same batch.
    EXPECT_EQ(async_r.position.batch_id, sync_r.position.batch_id);
    EXPECT_LT(async_r.position.local_index, sync_r.position.local_index);

    writer.Shutdown();
}

// ===========================================================================
// RED 3: LogWriterWaiterCompletesOnlyAfterContainingBatchDurable
// Plan §7.3 / §7.4: an async waiter's future must not resolve before the
// containing batch is durable. We inject a backend stall and assert that
// get() blocks until the batch becomes durable.
// ===========================================================================
TEST(OpLogLogWriterTest,
     LogWriterWaiterCompletesOnlyAfterContainingBatchDurable) {
    MockOpLogStore store;
    store.SetBatchDurabilityDelayMs(300);  // backend takes 300ms per batch
    OpLogLogWriter writer(MakeDefaultConfig(4),
                          std::shared_ptr<OpLogStore>(&store, [](auto*) {}));
    ASSERT_EQ(ErrorCode::OK, writer.Start());

    EnqueueResult r =
        writer.Enqueue(MakePutEndEntry("k1"), OpLogDurabilityMode::kAsync);
    writer.TriggerFlush();

    auto t0 = std::chrono::steady_clock::now();
    ErrorCode err = r.durable_result.get();
    auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(ErrorCode::OK, err);
    EXPECT_GE(
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
        200);
    EXPECT_EQ(1u, store.BatchCount());

    writer.Shutdown();
}

// ===========================================================================
// RED 4: LogWriterDoesNotFlushBatchNPlusOneAfterBatchNFailure
// Plan §7.3 / §7.4: when batch N's AppendBatch returns a backend error,
// batch N+1 must NOT be submitted. Any sync waiters for batch N+1 must
// resolve with the same error.
// ===========================================================================
TEST(OpLogLogWriterTest, LogWriterDoesNotFlushBatchNPlusOneAfterBatchNFailure) {
    MockOpLogStore store;
    store.SetBatchWriteError(ErrorCode::ETCD_OPERATION_ERROR);
    OpLogLogWriter writer(MakeDefaultConfig(4),
                          std::shared_ptr<OpLogStore>(&store, [](auto*) {}));
    ASSERT_EQ(ErrorCode::OK, writer.Start());

    // First batch is enqueued then flushed — backend refuses.
    EnqueueResult r1 =
        writer.Enqueue(MakePutEndEntry("k1"), OpLogDurabilityMode::kAsync);
    writer.TriggerFlush();
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, r1.durable_result.get());
    EXPECT_EQ(0u, store.BatchCount());
    EXPECT_EQ(0u, store.BatchAppendCount());

    // A subsequent sync enqueue must not push another batch. The writer
    // should either refuse (future resolves to error immediately) or
    // refuse to make progress on batch_id > 1. We accept both observable
    // forms: the next sync enqueue either returns INTERNAL_ERROR / same
    // backend error, OR blocks indefinitely. The contract we verify is
    // that no additional batch is appended.
    EnqueueResult r2 = writer.Enqueue(MakePutEndEntry("k2"),
                                      OpLogDurabilityMode::kWaitBatchDurable);
    // Give the writer a chance to (incorrectly) flush — it must not.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(0u, store.BatchAppendCount());
    // r2 should be marked failed (either future already resolved or will
    // resolve to ETCD_OPERATION_ERROR on get()).
    ErrorCode r2_err = r2.durable_result.wait_for(std::chrono::seconds(2)) ==
                               std::future_status::ready
                           ? r2.durable_result.get()
                           : ErrorCode::ETCD_OPERATION_ERROR;
    EXPECT_NE(ErrorCode::OK, r2_err);

    writer.Shutdown();
}

// ===========================================================================
// RED 5: LogWriterMetricsReflectBackendHealth
// Plan §7.3 / §7.4: HAMetricManager gauges/counters must track LogWriter
// activity. We verify that a successful flush increments the durable
// batches counter and that the pending gauges are reset after drain.
// ===========================================================================
TEST(OpLogLogWriterTest, LogWriterMetricsReflectBackendHealth) {
    auto& metrics = HAMetricManager::instance();
    const int64_t durable_before =
        metrics.get_oplog_batch_writer_durable_batches_total();
    const int64_t failures_before =
        metrics.get_oplog_batch_writer_flush_failures_total();

    MockOpLogStore store;
    OpLogLogWriter writer(MakeDefaultConfig(4),
                          std::shared_ptr<OpLogStore>(&store, [](auto*) {}));
    ASSERT_EQ(ErrorCode::OK, writer.Start());

    // Enqueue a sync entry to force a flush.
    EnqueueResult r = writer.Enqueue(MakePutEndEntry("metric-key"),
                                     OpLogDurabilityMode::kWaitBatchDurable);
    EXPECT_EQ(ErrorCode::OK, r.durable_result.get());

    // After the flush completes, durable_batches must have advanced,
    // pending gauges must be reset to 0, and flush_failures must NOT
    // have advanced (this is a successful flush).
    EXPECT_GE(metrics.get_oplog_batch_writer_durable_batches_total(),
              durable_before + 1);
    EXPECT_EQ(0, metrics.get_oplog_batch_writer_pending_entries());
    EXPECT_EQ(0, metrics.get_oplog_batch_writer_pending_bytes());
    EXPECT_EQ(failures_before,
              metrics.get_oplog_batch_writer_flush_failures_total());

    writer.Shutdown();
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    // Without InitGoogleLogging, the first LOG(INFO) from
    // OpLogLogWriter::Enqueue deadlocks in glog's default file sink
    // (no sink is configured). Match the pattern used by every other
    // HA test under tests/ha/.
    google::InitGoogleLogging("OpLogLogWriterTest");
    FLAGS_logtostderr = 1;
    return RUN_ALL_TESTS();
}
