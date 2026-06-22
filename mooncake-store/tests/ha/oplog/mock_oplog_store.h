// mooncake-store/tests/hot_standby_ut/mock_oplog_store.h
#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store.h"
#include "types.h"

namespace mooncake::test {

// In-memory OpLog store for unit tests.
class MockOpLogStore : public OpLogStore {
   public:
    MockOpLogStore() = default;

    ErrorCode Init() override { return ErrorCode::OK; }

    ErrorCode WriteOpLog(const OpLogEntry& entry,
                         bool /*sync*/ = true) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (write_error_ != ErrorCode::OK) {
            return write_error_;
        }
        entries_[entry.sequence_id] = entry;
        if (entry.sequence_id > latest_seq_id_) {
            latest_seq_id_ = entry.sequence_id;
        }
        return ErrorCode::OK;
    }

    ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (read_error_ != ErrorCode::OK) {
            return read_error_;
        }
        auto it = entries_.find(sequence_id);
        if (it == entries_.end()) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        entry = it->second;
        return ErrorCode::OK;
    }

    ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                             std::vector<OpLogEntry>& entries) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (read_error_ != ErrorCode::OK) {
            return read_error_;
        }
        entries.clear();
        for (const auto& [seq, entry] : entries_) {
            if (seq > start_sequence_id) {
                entries.push_back(entry);
                if (entries.size() >= limit) break;
            }
        }
        return ErrorCode::OK;
    }

    ErrorCode GetLatestSequenceId(uint64_t& sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        sequence_id = latest_seq_id_;
        return ErrorCode::OK;
    }

    ErrorCode GetMaxSequenceId(uint64_t& sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (entries_.empty()) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        sequence_id = entries_.rbegin()->first;
        return ErrorCode::OK;
    }

    ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        latest_seq_id_ = sequence_id;
        return ErrorCode::OK;
    }

    ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                       uint64_t sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshots_[snapshot_id] = sequence_id;
        return ErrorCode::OK;
    }

    ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                    uint64_t& sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = snapshots_.find(snapshot_id);
        if (it == snapshots_.end()) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        sequence_id = it->second;
        return ErrorCode::OK;
    }

    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = entries_.begin(); it != entries_.end();) {
            if (it->first < before_sequence_id) {
                it = entries_.erase(it);
            } else {
                break;
            }
        }
        return ErrorCode::OK;
    }

    // ===================================================================
    // Stage 4: in-memory batch API for LogWriter tests.
    //
    // We extend the mock rather than rely on the base class default so that
    // LogWriter unit tests can exercise real sequence (AppendBatch ordering,
    // latest_batch advancement, idempotent retry) and inject failures.
    // ===================================================================
    ErrorCode AppendBatch(const OpLogBatchRecord& batch) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (batch_durability_delay_ms_ > 0) {
            // Sleep outside the lock to simulate backend stall. Caller is
            // expected to handle timeouts in their own way.
            const auto delay = batch_durability_delay_ms_;
            mutex_.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            mutex_.lock();
        }
        if (batch_write_error_ != ErrorCode::OK) {
            return batch_write_error_;
        }
        const uint32_t shard = batch.shard_id;
        const uint64_t bid = batch.batch_id;
        // Idempotent retry with identical payload: treat as success and
        // keep the original record in the map.
        auto it = batches_[shard].find(bid);
        if (it != batches_[shard].end()) {
            return ErrorCode::OK;
        }
        if (bid != latest_batch_id_[shard] + 1 &&
            latest_batch_id_[shard] != 0) {
            // Mimic the real store's strict ordering rule.
            return ErrorCode::SEQUENCE_CONFLICT;
        }
        if (bid != 1 && latest_batch_id_[shard] == 0) {
            return ErrorCode::SEQUENCE_CONFLICT;
        }
        batches_[shard][bid] = batch;
        latest_batch_id_[shard] = bid;
        batch_append_count_++;
        return ErrorCode::OK;
    }

    ErrorCode ReadBatch(uint32_t shard_id, uint64_t batch_id,
                        OpLogBatchRecord& batch) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = batches_.find(shard_id);
        if (it == batches_.end()) return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        auto jt = it->second.find(batch_id);
        if (jt == it->second.end()) return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        batch = jt->second;
        return ErrorCode::OK;
    }

    ErrorCode ReadBatchesSince(
        uint32_t shard_id, uint64_t start_batch_id, size_t limit,
        std::vector<OpLogBatchRecord>& batches) override {
        std::lock_guard<std::mutex> lock(mutex_);
        batches.clear();
        auto it = batches_.find(shard_id);
        if (it == batches_.end()) return ErrorCode::OK;
        for (const auto& [bid, rec] : it->second) {
            if (bid <= start_batch_id) continue;
            batches.push_back(rec);
            if (batches.size() >= limit) break;
        }
        return ErrorCode::OK;
    }

    ErrorCode GetLatestBatchId(uint32_t shard_id, uint64_t& batch_id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        batch_id = latest_batch_id_[shard_id];
        return ErrorCode::OK;
    }

    // === Test control methods (Stage 4) ===
    void SetBatchWriteError(ErrorCode err) { batch_write_error_ = err; }
    void SetBatchDurabilityDelayMs(int delay_ms) {
        batch_durability_delay_ms_ = delay_ms;
    }
    size_t BatchAppendCount() const { return batch_append_count_; }
    size_t BatchCount(uint32_t shard_id = 0) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = batches_.find(shard_id);
        return it == batches_.end() ? 0 : it->second.size();
    }
    std::map<uint32_t, std::map<uint64_t, OpLogBatchRecord>> SnapshotBatches()
        const {
        std::lock_guard<std::mutex> lock(mutex_);
        return batches_;
    }

    // === Test control methods ===

    void SetWriteError(ErrorCode err) { write_error_ = err; }
    void SetReadError(ErrorCode err) { read_error_ = err; }
    void Clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_.clear();
        snapshots_.clear();
        latest_seq_id_ = 0;
    }
    size_t EntryCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return entries_.size();
    }

    // Find the latest OpLog entry for a given key. Returns
    // OPLOG_ENTRY_NOT_FOUND if no entry matches.
    ErrorCode FindLatestEntryForKey(const std::string& key,
                                    OpLogEntry& out) const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = entries_.rbegin(); it != entries_.rend(); ++it) {
            if (it->second.object_key == key) {
                out = it->second;
                return ErrorCode::OK;
            }
        }
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }

   private:
    mutable std::mutex mutex_;
    std::map<uint64_t, OpLogEntry> entries_;
    std::map<std::string, uint64_t> snapshots_;
    uint64_t latest_seq_id_{0};
    ErrorCode write_error_{ErrorCode::OK};
    ErrorCode read_error_{ErrorCode::OK};

    // Stage 4 batch state. shard -> batch_id -> batch record.
    std::map<uint32_t, std::map<uint64_t, OpLogBatchRecord>> batches_;
    std::map<uint32_t, uint64_t> latest_batch_id_;
    ErrorCode batch_write_error_{ErrorCode::OK};
    int batch_durability_delay_ms_{0};
    size_t batch_append_count_{0};
};

}  // namespace mooncake::test
