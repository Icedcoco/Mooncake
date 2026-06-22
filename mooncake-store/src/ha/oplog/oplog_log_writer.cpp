// mooncake-store/src/ha/oplog/oplog_log_writer.cpp
//
// Stage 4: single-shard batched write pipeline. See
// mooncake-store/include/ha/oplog/oplog_log_writer.h for the contract.

#include "ha/oplog/oplog_log_writer.h"

#include <algorithm>
#include <chrono>
#include <numeric>
#include <utility>

#include <glog/logging.h>

#include "ha_metric_manager.h"
#include "ha/oplog/oplog_serializer.h"

namespace mooncake {

namespace {

// Approximate per-entry size for byte-bounded flush. We use key + payload
// length as a proxy — exact serialization size is backend-specific and
// re-measuring at flush time would require serializing twice.
inline uint64_t ApproxEntrySize(const OpLogEntry& e) {
    return e.object_key.size() + e.payload.size() + 128;  // header overhead
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

OpLogLogWriter::OpLogLogWriter(OpLogLogWriterConfig config,
                               std::shared_ptr<OpLogStore> store)
    : config_(std::move(config)), store_(std::move(store)) {}

OpLogLogWriter::~OpLogLogWriter() { Shutdown(); }

ErrorCode OpLogLogWriter::Start() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (running_.load()) {
            return ErrorCode::OK;
        }
        stop_requested_.store(false);
        running_.store(true);
    }
    flush_thread_ = std::thread(&OpLogLogWriter::FlushThreadMain, this);
    return ErrorCode::OK;
}

void OpLogLogWriter::Shutdown() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_.load()) return;
        stop_requested_.store(true);
    }
    cv_flush_.notify_all();
    cv_batch_complete_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    running_.store(false);
}

EnqueueResult OpLogLogWriter::Enqueue(OpLogEntry entry,
                                      OpLogDurabilityMode mode) {
    auto promise = std::make_shared<std::promise<ErrorCode>>();
    auto future = promise->get_future().share();

    PendingEntry pending;
    pending.entry = std::move(entry);
    pending.promise = promise;
    pending.mode = mode;

    EnqueueResult result;
    result.durable_result = future;

    if (!running_.load() || stop_requested_.load()) {
        promise->set_value(ErrorCode::INTERNAL_ERROR);
        result.position = {config_.shard_id, 0, 0};
        return result;
    }

    bool need_flush_now = false;
    bool was_idle = false;
    int64_t pending_bytes = 0;
    int64_t pending_entries = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        // Assign position based on the batch we'll be in.
        const uint64_t batch_id = next_batch_id_.load();
        const uint32_t local_index = static_cast<uint32_t>(queue_.size());
        result.position = {config_.shard_id, batch_id, local_index};

        was_idle = queue_.empty();
        queue_.push_back(std::move(pending));
        const bool fill_threshold = queue_.size() >= config_.max_entries;
        const uint64_t approx_bytes =
            std::accumulate(queue_.begin(), queue_.end(), uint64_t{0},
                            [](uint64_t s, const PendingEntry& p) {
                                return s + ApproxEntrySize(p.entry);
                            });
        const bool byte_threshold = approx_bytes >= config_.max_bytes;
        if (mode == OpLogDurabilityMode::kWaitBatchDurable &&
            config_.flush_on_sync_op) {
            need_flush_now = true;
        }
        if (fill_threshold || byte_threshold) {
            need_flush_now = true;
        }
        pending_bytes = static_cast<int64_t>(approx_bytes);
        pending_entries = static_cast<int64_t>(queue_.size());
    }
    HAMetricManager::instance().set_oplog_batch_writer_pending_entries(
        pending_entries);
    HAMetricManager::instance().set_oplog_batch_writer_pending_bytes(
        pending_bytes);

    if (need_flush_now) {
        cv_flush_.notify_all();
    }
    // Note: we intentionally do NOT notify on was_idle (queue went from
    // empty to non-empty). The flush thread is parked in wait_for with the
    // max_delay timer running; waking it on every first enqueue would
    // produce a one-entry batch per Enqueue call — exactly the batching we
    // are trying to achieve. Instead, the flush thread relies on the
    // max_delay timer to coalesce back-to-back async enqueues into a
    // single batch. Sync enqueues (kWaitBatchDurable) and fill/byte
    // thresholds short-circuit via need_flush_now above.

    if (mode == OpLogDurabilityMode::kWaitBatchDurable) {
        // Block until the future is satisfied. The flush thread (or the
        // synchronous flush path below) is responsible for setting the
        // promise value.
        future.wait();
    }

    return result;
}

void OpLogLogWriter::TriggerFlush() { cv_flush_.notify_all(); }

ErrorCode OpLogLogWriter::GetLatestDurableBatchId(uint64_t& batch_id) const {
    batch_id = latest_durable_batch_id_.load();
    return ErrorCode::OK;
}

void OpLogLogWriter::SetProducerViewVersion(ViewVersionId view_version) {
    std::lock_guard<std::mutex> lock(mu_);
    config_.producer_view_version = view_version;
}

void OpLogLogWriter::SetOwnerToken(ha::OwnerToken owner_token) {
    std::lock_guard<std::mutex> lock(mu_);
    config_.owner_token = std::move(owner_token);
}

// ---------------------------------------------------------------------------
// Internal: background flush thread
// ---------------------------------------------------------------------------

void OpLogLogWriter::FlushThreadMain() {
    while (!stop_requested_.load()) {
        std::unique_lock<std::mutex> lock(mu_);
        // No predicate: we always wait at least max_delay (or until
        // notified by need_flush_now / shutdown / TriggerFlush). A
        // predicate would short-circuit on first enqueue if the thread
        // starts after the queue has been populated, defeating batching.
        cv_flush_.wait_for(lock, config_.max_delay);
        if (stop_requested_.load()) {
            // Drain any pending entries by failing them so callers don't
            // hang on shutdown.
            for (auto& p : queue_) {
                p.promise->set_value(ErrorCode::INTERNAL_ERROR);
            }
            queue_.clear();
            return;
        }
        if (queue_.empty()) continue;
        // DoFlush re-acquires mu_ internally. Release the outer
        // unique_lock first to avoid self-deadlock (non-recursive mutex).
        lock.unlock();
        DoFlush();
    }
}

void OpLogLogWriter::DoFlush() {
    // Atomically drain the queue and claim a batch_id. The lock is held
    // for the duration of the build but released before the backend call
    // so other writers can keep enqueuing while we're appending.
    OpLogBatchRecord batch;
    std::vector<std::shared_ptr<std::promise<ErrorCode>>> promises;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (queue_.empty()) {
            return;
        }
        const uint64_t bid = next_batch_id_.load();
        const auto shard = config_.shard_id;
        batch.shard_id = shard;
        batch.batch_id = bid;
        batch.producer_view_version = config_.producer_view_version;
        batch.owner_token = config_.owner_token;
        batch.entries.reserve(queue_.size());
        promises.reserve(queue_.size());
        for (auto& p : queue_) {
            // Stamp batch coords on each entry so a future applier can
            // cross-reference entries to their batch.
            p.entry.shard_id = shard;
            p.entry.batch_id = bid;
            p.entry.local_index = static_cast<uint32_t>(batch.entries.size());
            batch.entries.push_back(std::move(p.entry));
            promises.push_back(std::move(p.promise));
        }
        queue_.clear();
        next_batch_id_.store(bid + 1);
    }

    batch.batch_checksum = ComputeOpLogBatchChecksum(batch);

    // After draining, the queue is empty — reset pending gauges.
    HAMetricManager::instance().set_oplog_batch_writer_pending_entries(0);
    HAMetricManager::instance().set_oplog_batch_writer_pending_bytes(0);

    // Measure end-to-end flush latency (drain + build + AppendBatch). This
    // is the metric operators should alert on for Stage 4 batch pipeline
    // health (plan §7.4).
    const auto flush_start = std::chrono::steady_clock::now();
    ErrorCode err = store_->AppendBatch(batch);
    const auto flush_end = std::chrono::steady_clock::now();
    const int64_t flush_latency_us =
        std::chrono::duration_cast<std::chrono::microseconds>(flush_end -
                                                              flush_start)
            .count();
    HAMetricManager::instance().observe_oplog_batch_writer_flush_latency_us(
        flush_latency_us);

    if (err != ErrorCode::OK) {
        LOG(WARNING) << "OpLogLogWriter: AppendBatch failed for batch_id="
                     << batch.batch_id << ", err=" << static_cast<int>(err);
        // Refuse to advance past a failed batch. Restore next_batch_id_.
        next_batch_id_.store(batch.batch_id);
        HAMetricManager::instance().inc_oplog_batch_writer_flush_failures();
        for (auto& p : promises) {
            p->set_value(err);
        }
        return;
    }

    latest_durable_batch_id_.store(batch.batch_id);
    HAMetricManager::instance().inc_oplog_batch_writer_durable_batches();
    for (auto& p : promises) {
        p->set_value(ErrorCode::OK);
    }
}

}  // namespace mooncake
