#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "ha/ha_types.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store.h"

namespace mooncake {

// Stage 4: LogWriter tunables. Defaults match plan §7.2.
struct OpLogLogWriterConfig {
    uint32_t shard_id = 0;          // single-shard only today (basic available)
    uint32_t max_entries = 64;      // entries per batch
    uint64_t max_bytes = 1u << 20;  // bytes per batch (entries' payload+key)
    std::chrono::microseconds max_delay{1000};  // flush wait for non-full batch
    bool flush_on_sync_op = true;  // trigger immediate flush on sync entry
    ViewVersionId producer_view_version = 0;  // stamped on every batch
    ha::OwnerToken owner_token;  // stamped on every batch (audit field)
};

// Stage 4: single-shard batched write pipeline. Drives
// EtcdOpLogStore::AppendBatch (Stage 3) from the request path. See plan §7.
//
// Contract (each item is enforced by a unit test, plan §7.3):
//   - shard_id is fixed (basic available single-shard).
//   - batch_id is strictly increasing per shard.
//   - local_index is 0-based and contiguous within a batch.
//   - On flush, the writer calls store->AppendBatch with batch_checksum
//     populated via ComputeOpLogBatchChecksum.
//   - A sync enqueue (kWaitBatchDurable) blocks until the containing batch
//     is durable OR the backend refuses the batch. On refusal, the entry's
//     durable_result carries the error and the writer does NOT advance
//     past the failed batch_id (any waiters for batch_id+1 resolve to the
//     same error and the writer refuses subsequent flushes until
//     Start/Shutdown cycle recovers it).
//   - flush_on_sync_op triggers an immediate flush regardless of fill or
//     timer. The async waiters' futures resolve only after the batch is
//     durable (NOT on enqueue).
class OpLogLogWriter {
   public:
    OpLogLogWriter(OpLogLogWriterConfig config,
                   std::shared_ptr<OpLogStore> store);
    ~OpLogLogWriter();

    OpLogLogWriter(const OpLogLogWriter&) = delete;
    OpLogLogWriter& operator=(const OpLogLogWriter&) = delete;

    // Start the background flush thread. Must be called before Enqueue.
    ErrorCode Start();

    // Stop accepting new entries and join the flush thread.
    void Shutdown();

    // Enqueue an entry. Assigns position synchronously. The future in the
    // returned EnqueueResult resolves when the containing batch is durable
    // (or has failed). For kWaitBatchDurable the call blocks until the
    // future is satisfied. Returns an invalid result (future with
    // INTERNAL_ERROR) if the writer has been shutdown or has not been
    // started.
    EnqueueResult Enqueue(OpLogEntry entry, OpLogDurabilityMode mode);

    // Force a flush of the in-progress batch immediately. The flush runs
    // on the background thread; this method just wakes it. Returns
    // immediately.
    void TriggerFlush();

    // Latest batch_id this writer has observed as durable from the
    // backend. Returns OK with batch_id=0 when no batch has been written.
    ErrorCode GetLatestDurableBatchId(uint64_t& batch_id) const;

    // Producer identity. Set by MasterService when binding a new
    // leadership session. Stamped on every subsequent batch.
    void SetProducerViewVersion(ViewVersionId view_version);
    void SetOwnerToken(ha::OwnerToken owner_token);

   private:
    struct PendingEntry {
        OpLogEntry entry;
        std::shared_ptr<std::promise<ErrorCode>> promise;
        OpLogDurabilityMode mode;
    };

    void FlushThreadMain();
    void DoFlush();
    ErrorCode BuildAndSubmitBatch();

    OpLogLogWriterConfig config_;
    std::shared_ptr<OpLogStore> store_;

    mutable std::mutex mu_;
    std::condition_variable cv_flush_;
    std::condition_variable cv_batch_complete_;
    std::deque<PendingEntry> queue_;  // pending entries (current batch)
    std::atomic<uint64_t> next_batch_id_{1};
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> batch_in_flight_{false};
    std::atomic<uint64_t> latest_durable_batch_id_{0};
    std::thread flush_thread_;
};

}  // namespace mooncake
