#include "ha/oplog/oplog_manager.h"

#include <algorithm>
#include <chrono>
#include <xxhash.h>
#include <glog/logging.h>

#include "ha/oplog/oplog_log_writer.h"
#include "ha/oplog/oplog_store.h"

namespace mooncake {

OpLogManager::OpLogManager() = default;

void OpLogManager::SetOpLogStore(std::shared_ptr<OpLogStore> oplog_store) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    oplog_store_ = oplog_store;
}

void OpLogManager::SetLogWriter(std::shared_ptr<OpLogLogWriter> log_writer) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    log_writer_ = std::move(log_writer);
}

bool OpLogManager::IsBatchedMode() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return log_writer_ != nullptr;
}

EnqueueResult OpLogManager::AppendBatched(OpType type,
                                          const std::string& tenant_id,
                                          const std::string& key,
                                          const std::string& payload) {
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = tenant_id;
    entry.object_key = key;
    entry.payload = payload;
    entry.timestamp_ms = NowMs();
    entry.checksum = ComputeChecksum(entry.payload);
    entry.prefix_hash = ComputePrefixHash(entry.object_key);

    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        entry.sequence_id = ++last_seq_id_;
        if (buffer_.size() >= kMaxBufferEntries_) {
            buffer_.pop_front();
            ++first_seq_id_;
        }
        buffer_.emplace_back(entry);
    }

    std::shared_ptr<OpLogLogWriter> writer;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        writer = log_writer_;
    }
    if (!writer) {
        auto promise = std::make_shared<std::promise<ErrorCode>>();
        promise->set_value(ErrorCode::INTERNAL_ERROR);
        EnqueueResult bad;
        bad.position = {0, 0, 0};
        bad.durable_result = promise->get_future();
        return bad;
    }

    const OpLogDurabilityMode mode =
        (type == OpType::PUT_END) ? OpLogDurabilityMode::kAsync
                                  : OpLogDurabilityMode::kWaitBatchDurable;
    return writer->Enqueue(entry, mode);
}

uint64_t OpLogManager::Append(OpType type, const std::string& key,
                              const std::string& payload) {
    return Append(type, "default", key, payload);
}

uint64_t OpLogManager::Append(OpType type, const std::string& tenant_id,
                              const std::string& key,
                              const std::string& payload) {
    // Stage 4: batched mode routes through the LogWriter. PUT_END is
    // async (returns immediately), other ops block until containing batch
    // is durable.
    if (IsBatchedMode()) {
        EnqueueResult r = AppendBatched(type, tenant_id, key, payload);
        // We don't propagate durable_result here — callers that need
        // error propagation for dirty ops use AppendAndPersist instead.
        return r.position.batch_id;  // best-effort monotonic id from batch
    }

    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = tenant_id;
    entry.object_key = key;
    entry.payload = payload;
    entry.timestamp_ms = NowMs();
    entry.checksum = ComputeChecksum(entry.payload);
    entry.prefix_hash = ComputePrefixHash(entry.object_key);

    std::unique_lock<std::shared_mutex> lock(mutex_);
    entry.sequence_id = ++last_seq_id_;
    const uint64_t seq = entry.sequence_id;  // save before potential unlock

    if (buffer_.size() >= kMaxBufferEntries_) {
        buffer_.pop_front();
        ++first_seq_id_;
    }

    buffer_.emplace_back(entry);  // Copy entry to buffer

    // Write to etcd if EtcdOpLogStore is set.
    // Strategy: PUT_END is async (sync=false) — only pushes to batch queue
    // (microsecond-level), safe to hold mutex_.
    // REMOVE / PUT_REVOKE are sync (sync=true) — blocks until etcd confirms
    // persistence; must release mutex_ to avoid blocking other Append calls
    // during the wait.  The caller relies on sync semantics to know the
    // entry is durable before freeing/reusing associated memory.
    if (oplog_store_) {
        bool sync = (type != OpType::PUT_END);
        if (sync) {
            // Release lock before the blocking wait to avoid holding
            // mutex_ for the entire etcd round-trip.
            lock.unlock();
        }
        ErrorCode err = oplog_store_->WriteOpLog(entry, sync);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to write OpLog to store, sequence_id="
                         << seq << ", but entry is in memory buffer";
        }
    }

    return seq;
}

OpLogEntry OpLogManager::AllocateEntry(OpType type, const std::string& key,
                                       const std::string& payload) {
    return AllocateEntry(type, "default", key, payload);
}

OpLogEntry OpLogManager::AllocateEntry(OpType type,
                                       const std::string& tenant_id,
                                       const std::string& key,
                                       const std::string& payload) {
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = tenant_id;
    entry.object_key = key;
    entry.payload = payload;
    entry.timestamp_ms = NowMs();
    entry.checksum = ComputeChecksum(entry.payload);
    entry.prefix_hash = ComputePrefixHash(entry.object_key);

    std::unique_lock<std::shared_mutex> lock(mutex_);
    entry.sequence_id = ++last_seq_id_;

    if (buffer_.size() >= kMaxBufferEntries_) {
        buffer_.pop_front();
        ++first_seq_id_;
    }
    buffer_.emplace_back(entry);
    return entry;
}

ErrorCode OpLogManager::PersistEntry(const OpLogEntry& entry) const {
    std::shared_lock<std::shared_mutex> lock1(mutex_);
    auto writer = log_writer_;
    auto store = oplog_store_;
    lock1.unlock();
    // Stage 4: in batched mode the durable path goes through the writer.
    // PUT_END is async (returns immediately); other ops wait for the
    // containing batch to be durable (or to fail).
    if (writer) {
        const OpLogDurabilityMode mode =
            (entry.op_type == OpType::PUT_END)
                ? OpLogDurabilityMode::kAsync
                : OpLogDurabilityMode::kWaitBatchDurable;
        EnqueueResult r = writer->Enqueue(entry, mode);
        if (mode == OpLogDurabilityMode::kAsync) {
            return ErrorCode::OK;
        }
        return r.durable_result.get();
    }

    if (!store) {
        return ErrorCode::INTERNAL_ERROR;
    }
    // Strategy 2+: PUT_END is Async, REMOVE (and others) are Sync
    bool sync = (entry.op_type != OpType::PUT_END);
    return store->WriteOpLog(entry, sync);
}

tl::expected<uint64_t, ErrorCode> OpLogManager::AppendAndPersist(
    OpType type, const std::string& key, const std::string& payload) {
    return AppendAndPersist(type, "default", key, payload);
}

tl::expected<uint64_t, ErrorCode> OpLogManager::AppendAndPersist(
    OpType type, const std::string& tenant_id, const std::string& key,
    const std::string& payload) {
    // Stage 4: in batched mode route the dirty mutation through the
    // LogWriter and wait for the containing batch to be durable. This is
    // what guarantees dirty-mutation durable-before-success under
    // ha_oplog_format=batched.
    if (IsBatchedMode()) {
        EnqueueResult r = AppendBatched(type, tenant_id, key, payload);
        ErrorCode err = r.durable_result.get();
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
        return r.position.batch_id;
    }

    // Seq pre-allocation semantics: allocate first, then persist.
    OpLogEntry entry = AllocateEntry(type, tenant_id, key, payload);
    ErrorCode err = PersistEntry(entry);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return entry.sequence_id;
}

uint64_t OpLogManager::GetLastSequenceId() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return last_seq_id_;
}

void OpLogManager::SetInitialSequenceId(uint64_t sequence_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (last_seq_id_ == 0 && buffer_.empty()) {
        // Only allow setting initial sequence_id if OpLogManager is empty
        last_seq_id_ = sequence_id;
        first_seq_id_ = sequence_id +
                        1;  // first_seq_id_ should be > last_seq_id_ when empty
        LOG(INFO) << "OpLogManager initial sequence_id set to " << sequence_id;
    } else {
        LOG(WARNING)
            << "Cannot set initial sequence_id: OpLogManager is not empty "
            << "(last_seq_id_=" << last_seq_id_
            << ", buffer_size=" << buffer_.size() << ")";
    }
}

size_t OpLogManager::GetEntryCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return buffer_.size();
}

ErrorCode OpLogManager::CleanupOpLogBefore(uint64_t before_sequence_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!oplog_store_) {
        return ErrorCode::OK;
    }
    return oplog_store_->CleanupOpLogBefore(before_sequence_id);
}

uint64_t OpLogManager::NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
        .count();
}

uint32_t OpLogManager::ComputeChecksum(const std::string& data) {
    // Use xxHash XXH32 for a fast, deterministic 32-bit checksum.
    // Requires linking against xxHash (e.g., libxxhash) and including
    // <xxhash.h>.
    return static_cast<uint32_t>(XXH32(data.data(), data.size(), 0));
}

uint32_t OpLogManager::ComputePrefixHash(const std::string& key) {
    if (key.empty()) {
        return 0;
    }
    // Use XXH32 for consistency with ComputeChecksum and better performance.
    // XXH32 provides faster hashing and lower collision rate than std::hash.
    // Computing hash for the entire key ensures better distribution and fewer
    // collisions.
    return static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
}

bool OpLogManager::VerifyChecksum(const OpLogEntry& entry) {
    uint32_t computed = ComputeChecksum(entry.payload);
    return computed == entry.checksum;
}

bool OpLogManager::ValidateEntrySize(const OpLogEntry& entry,
                                     std::string* reason) {
    if (entry.object_key.size() > kMaxObjectKeySize) {
        if (reason) {
            *reason = "object_key too large: size=" +
                      std::to_string(entry.object_key.size());
        }
        return false;
    }
    if (entry.payload.size() > kMaxPayloadSize) {
        if (reason) {
            *reason = "payload too large: size=" +
                      std::to_string(entry.payload.size());
        }
        return false;
    }
    return true;
}

}  // namespace mooncake
