#include "ha/oplog/etcd_oplog_store.h"

#include <glog/logging.h>
#include <sstream>
#include <iomanip>

#include "ha_metric_manager.h"
#include "ha/oplog/oplog_serializer.h"
#include "ha/oplog/etcd_oplog_change_notifier.h"
#include "utils/base64.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif

#include "etcd_helper.h"

namespace mooncake {

EtcdOpLogStore::EtcdOpLogStore(const std::string& cluster_id,
                               bool enable_latest_seq_batch_update,
                               bool enable_batch_write)
    : cluster_id_(cluster_id),
      enable_latest_seq_batch_update_(enable_latest_seq_batch_update),
      enable_batch_write_(enable_batch_write),
      last_update_time_(std::chrono::steady_clock::now()) {
    if (!NormalizeAndValidateClusterId(cluster_id_)) {
        LOG(FATAL)
            << "Invalid cluster_id for EtcdOpLogStore: '" << cluster_id_
            << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128, no slashes.";
    }
}

ErrorCode EtcdOpLogStore::Init() {
    // Initialize /latest key to 0 if it doesn't exist (first startup).
    // This avoids "key not found" errors when querying the latest sequence ID.
    // Important: Only initialize if the key doesn't exist to avoid overwriting
    // existing data.
    // Skip for read-only instances to avoid unnecessary etcd writes.
    if (enable_batch_write_ && !cluster_id_.empty()) {
        std::string latest_key = BuildLatestKey();
        std::string existing_value;
        EtcdRevisionId revision_id;
        ErrorCode get_err = EtcdHelper::Get(
            latest_key.c_str(), latest_key.size(), existing_value, revision_id);
        if (get_err == ErrorCode::ETCD_KEY_NOT_EXIST) {
            // Key doesn't exist, safe to initialize to 0
            std::string initial_value = "0";
            ErrorCode create_err =
                EtcdHelper::Create(latest_key.c_str(), latest_key.size(),
                                   initial_value.c_str(), initial_value.size());
            if (create_err == ErrorCode::OK) {
                LOG(INFO) << "Initialized /latest key to 0 for cluster_id="
                          << cluster_id_;
            } else if (create_err == ErrorCode::ETCD_TRANSACTION_FAIL) {
                // Race condition: another instance created it between Get and
                // Create
                LOG(INFO) << "/latest key was created by another instance for "
                             "cluster_id="
                          << cluster_id_;
            } else {
                // Other errors (e.g., etcd not connected) are logged but don't
                // fail initialization. The key will be created when the first
                // OpLog entry is written
                LOG(WARNING)
                    << "Failed to initialize /latest key (error=" << create_err
                    << "), will be created on first OpLog write";
            }
        } else if (get_err == ErrorCode::OK) {
            // Key already exists, do nothing - preserve existing value
            LOG(INFO) << "/latest key already exists (value=" << existing_value
                      << ") for cluster_id=" << cluster_id_;
        } else {
            // Other errors (e.g., etcd not connected) are logged but don't fail
            // initialization
            LOG(WARNING) << "Failed to check /latest key existence (error="
                         << get_err
                         << "), will be created on first OpLog write";
        }
    }

    // Start batch update thread only for writers.
    if (enable_latest_seq_batch_update_) {
        // Prevent double start
        if (!batch_update_running_.exchange(true)) {
            batch_update_thread_ =
                std::thread(&EtcdOpLogStore::BatchUpdateThread, this);
        }
    }

    // Start OpLog batch write thread only for writers.
    if (enable_batch_write_) {
        // Prevent double start
        if (!batch_write_running_.exchange(true)) {
            batch_write_thread_ =
                std::thread(&EtcdOpLogStore::BatchWriteThread, this);
        }
    }

    return ErrorCode::OK;
}

EtcdOpLogStore::~EtcdOpLogStore() {
    // Stop OpLog batch write thread (only started for writers)
    if (enable_batch_write_) {
        batch_write_running_.store(false);
        cv_batch_updated_.notify_all();
        if (batch_write_thread_.joinable()) {
            batch_write_thread_.join();
        }

        // Attempt final flush (FlushBatch manages its own locking)
        FlushBatch();
    }

    if (!enable_latest_seq_batch_update_) {
        return;
    }

    // Stop batch update thread
    batch_update_running_.store(false);
    if (batch_update_thread_.joinable()) {
        batch_update_thread_.join();
    }

    // Perform final update if there are pending updates
    if (pending_count_.load() > 0) {
        DoBatchUpdate();
    }
}

ErrorCode EtcdOpLogStore::WriteOpLog(const OpLogEntry& entry, bool sync) {
    if (!enable_batch_write_) {
        LOG(ERROR) << "WriteOpLog called on a read-only EtcdOpLogStore "
                   << "(enable_batch_write=false), cluster_id=" << cluster_id_;
        return ErrorCode::INVALID_PARAMS;
    }
    std::string key = BuildOpLogKey(entry.sequence_id);
    std::string value = mooncake::SerializeOpLogEntry(entry);

    {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        pending_batch_.push_back(
            {std::move(key), std::move(value), entry.sequence_id, sync});

        bool should_notify = false;
        if (sync) {
            // Strategy 2+: Sync writes (DELETE) trigger immediate flush
            should_notify = true;
        } else {
            // Async writes (PUT_END): trigger if threshold reached
            if (pending_batch_.size() >= kOpLogBatchCountLimit) {
                should_notify = true;
            }
        }

        if (should_notify) {
            cv_batch_updated_.notify_one();
        }

        if (sync) {
            // Wait for persistence
            uint64_t target_seq = entry.sequence_id;
            bool success = cv_sync_completed_.wait_for(
                lock, std::chrono::milliseconds(kSyncWaitTimeoutMs),
                [&] { return last_persisted_seq_id_.load() >= target_seq; });
            if (!success) {
                LOG(ERROR) << "Timeout waiting for OpLog persistence, seq="
                           << target_seq;
                return ErrorCode::ETCD_OPERATION_ERROR;
            }
        }
    }

    // Update /latest pointer logic
    // We defer this to the batch flush or just queue it up here?
    // Original logic:
    if (!enable_latest_seq_batch_update_) {
        // Direct update (may be slow, but it's what config asked for)
        // Warning: This is now done AFTER op log write, which is correct order.
        return UpdateLatestSequenceId(entry.sequence_id);
    }

    // For batch update, we update the pending counter
    pending_latest_seq_id_.store(entry.sequence_id);
    size_t count = pending_count_.fetch_add(1) + 1;
    if (count >= kBatchSize) {
        DoBatchUpdate();
    }

    return ErrorCode::OK;
}

void EtcdOpLogStore::BatchWriteThread() {
    while (batch_write_running_.load()) {
        {
            std::unique_lock<std::mutex> lock(batch_mutex_);
            if (pending_batch_.empty()) {
                // Wait for signal or timeout (Group Commit time window)
                cv_batch_updated_.wait_for(
                    lock, std::chrono::milliseconds(kOpLogBatchTimeoutMs));
            }

            if (!batch_write_running_.load() && pending_batch_.empty()) {
                break;
            }
        }

        FlushBatch();
    }
}

void EtcdOpLogStore::FlushBatch() {
    // Step 1: Take pending batch under lock.
    std::deque<BatchEntry> batch_to_write;
    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        batch_to_write.swap(pending_batch_);
    }

    if (batch_to_write.empty()) {
        return;
    }

    // Step 2: Perform IO without holding the lock.
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(batch_to_write.size());
    values.reserve(batch_to_write.size());

    uint64_t max_seq = 0;
    bool has_sync_entry = false;
    for (const auto& entry : batch_to_write) {
        keys.push_back(entry.key);
        if (entry.is_sync) {
            has_sync_entry = true;
        }
        values.push_back(entry.value);
        if (entry.sequence_id > max_seq) {
            max_seq = entry.sequence_id;
        }
    }

    ErrorCode err = ErrorCode::OK;
    for (int i = 0; i <= kFlushRetryCount; ++i) {
        err = EtcdHelper::BatchCreate(keys, values);
        if (err == ErrorCode::OK) {
            break;
        }
        if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
            // Stage 3 fix: BatchCreate uses Txn(If all keys CreateRevision==0).
            // Transaction failure means some keys already exist. The previous
            // (legacy) fallback blindly overwrote with Put, which silently
            // destroyed any pre-existing value at the same sequence_id. The
            // basic-available contract (plan §6.4) requires read-and-compare:
            // if the existing value equals what we are about to write, it is
            // an idempotent retry; if it differs, we must NOT overwrite —
            // that would corrupt the durable history.
            LOG(WARNING)
                << "BatchCreate transaction failed (keys already exist), "
                << "falling back to read-and-compare for " << keys.size()
                << " entries";
            bool all_ok = true;
            for (size_t j = 0; j < keys.size(); ++j) {
                std::string existing_value;
                EtcdRevisionId existing_rev = 0;
                ErrorCode get_err =
                    EtcdHelper::Get(keys[j].c_str(), keys[j].size(),
                                    existing_value, existing_rev);
                if (get_err == ErrorCode::OK) {
                    if (existing_value == values[j]) {
                        // Idempotent retry — same sequence_id, same payload.
                        continue;
                    }
                    LOG(ERROR)
                        << "Refusing to overwrite pre-existing OpLog entry:"
                        << " key=" << keys[j]
                        << " (existing payload differs from incoming)";
                    all_ok = false;
                    continue;
                }
                if (get_err == ErrorCode::ETCD_KEY_NOT_EXIST) {
                    // Race: the key vanished between the BatchCreate attempt
                    // and our Get. Fall through to Put to materialize it.
                } else {
                    LOG(ERROR) << "Fallback Get failed for key=" << keys[j]
                               << " (err=" << get_err << ")";
                    all_ok = false;
                    continue;
                }
                ErrorCode put_err =
                    EtcdHelper::Put(keys[j].c_str(), keys[j].size(),
                                    values[j].c_str(), values[j].size());
                if (put_err != ErrorCode::OK) {
                    LOG(ERROR) << "Fallback Put failed for key=" << keys[j]
                               << " (err=" << put_err << ")";
                    all_ok = false;
                }
            }
            if (all_ok) {
                err = ErrorCode::OK;
            }
            break;  // Do not retry further; fallback already handled it.
        }
        if (i < kFlushRetryCount) {
            LOG(WARNING) << "Failed to flush OpLog batch (attempt " << i + 1
                         << "/" << kFlushRetryCount + 1 << "), retrying...";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kFlushRetryIntervalMs));
        }
    }

    // Step 3: Update state under lock.
    {
        std::lock_guard<std::mutex> lock(batch_mutex_);

        if (err == ErrorCode::OK) {
            if (max_seq > last_persisted_seq_id_.load()) {
                last_persisted_seq_id_.store(max_seq);
            }

            // Update HA metrics
            HAMetricManager::instance().inc_oplog_batch_commits();
            if (has_sync_entry) {
                HAMetricManager::instance().inc_oplog_sync_batch_commits();
            }

            if (batch_to_write.size() > 1) {
                LOG(INFO)
                    << "HA Strategy: Group Commit flush success. batch_size="
                    << batch_to_write.size() << ", max_seq=" << max_seq;
            } else {
                VLOG(3)
                    << "HA Strategy: Group Commit flush success. batch_size=1, "
                       "max_seq="
                    << max_seq;
                if (!has_sync_entry) {
                    LOG_EVERY_N(INFO, 1000)
                        << "Note: Frequent single-entry async "
                           "flushes detected (sample).";
                }
            }
        } else {
            LOG(ERROR) << "Failed to flush OpLog batch, count="
                       << batch_to_write.size();
        }
    }

    // Wake up all waiting threads (Strategy 2+: DELETE waiters)
    cv_sync_completed_.notify_all();
}

ErrorCode EtcdOpLogStore::ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) {
    std::string key = BuildOpLogKey(sequence_id);
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err != ErrorCode::OK) {
        // Translate etcd-specific "key not found" to the generic OpLogStore
        // error.
        if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        return err;
    }

    if (!mooncake::DeserializeOpLogEntry(value, entry)) {
        LOG(ERROR) << "Failed to deserialize OpLog entry, sequence_id="
                   << sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::ReadOpLogSince(uint64_t start_sequence_id,
                                         size_t limit,
                                         std::vector<OpLogEntry>& entries) {
    EtcdRevisionId rev = 0;
    return ReadOpLogSinceWithRevision(start_sequence_id, limit, entries, rev);
}

ErrorCode EtcdOpLogStore::ReadOpLogSinceWithRevision(
    uint64_t start_sequence_id, size_t limit, std::vector<OpLogEntry>& entries,
    EtcdRevisionId& revision_id) {
    entries.clear();
    entries.reserve(limit);

    // Range is limited to OpLog entry keys only.
    const std::string prefix = std::string(kOpLogPrefix) + cluster_id_ + "/";
    std::string current_start_key = BuildOpLogKey(start_sequence_id + 1);

    // Compute prefix range end (etcd prefix end).
    auto prefix_end = [](std::string p) -> std::string {
        for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i) {
            unsigned char c = static_cast<unsigned char>(p[i]);
            if (c < 0xFF) {
                p[i] = static_cast<char>(c + 1);
                p.resize(i + 1);
                return p;
            }
        }
        return std::string(1, '\0');
    };
    const std::string end_key = prefix_end(prefix);

    // Pagination:
    // - Use range-get with limit
    // - Start next page from lastKey + '\0' (lexicographically just after
    // lastKey) This avoids repeating the last key without adding new Go/C++
    // APIs.
    revision_id = 0;
    while (entries.size() < limit) {
        const size_t page_limit = limit - entries.size();
        std::string json;
        EtcdRevisionId page_rev = 0;
        ErrorCode err = EtcdHelper::GetRangeAsJson(
            current_start_key.c_str(), current_start_key.size(),
            end_key.c_str(), end_key.size(), page_limit, json, page_rev);
        if (err != ErrorCode::OK) {
            return err;
        }
        if (page_rev > revision_id) {
            revision_id = page_rev;
        }

        // Parse kv list: [{"key":"...","value":"..."}]
        Json::Value root;
        Json::CharReaderBuilder reader;
        std::string errs;
        std::istringstream s(json);
        if (!Json::parseFromStream(reader, s, &root, &errs)) {
            LOG(ERROR) << "Failed to parse range JSON: " << errs;
            return ErrorCode::INTERNAL_ERROR;
        }
        if (!root.isArray()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (root.empty()) {
            break;  // no more data
        }

        std::string last_key_in_page;
        for (const auto& kv : root) {
            const std::string key = kv.get("key", "").asString();
            last_key_in_page = key;
            // Stage 3: also skip the durable batch namespace so legacy
            // sequence readers never try to deserialize an OpLogBatchRecord
            // as an OpLogEntry.
            if (key.empty() || key.find("/latest") != std::string::npos ||
                key.find("/snapshot/") != std::string::npos ||
                key.find("/shards/") != std::string::npos) {
                continue;
            }

            // Parse seq from key suffix and filter (handles legacy keys too).
            size_t pos = key.rfind('/');
            if (pos == std::string::npos || pos + 1 >= key.size()) {
                continue;
            }
            uint64_t seq = 0;
            try {
                seq = static_cast<uint64_t>(std::stoull(key.substr(pos + 1)));
            } catch (...) {
                continue;
            }
            if (IsSequenceOlderOrEqual(seq, start_sequence_id)) {
                continue;
            }

            OpLogEntry entry;
            const std::string value = kv.get("value", "").asString();
            if (!mooncake::DeserializeOpLogEntry(value, entry)) {
                LOG(ERROR) << "Failed to deserialize OpLog entry from key="
                           << key;
                return ErrorCode::INTERNAL_ERROR;
            }
            entries.push_back(std::move(entry));
            if (entries.size() >= limit) {
                break;
            }
        }

        // Advance start key for next page.
        if (last_key_in_page.empty()) {
            break;
        }
        current_start_key = last_key_in_page;
        current_start_key.push_back('\0');
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::GetLatestSequenceId(uint64_t& sequence_id) {
    std::string key = BuildLatestKey();
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        return err;
    }

    try {
        sequence_id = std::stoull(value);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse latest sequence_id: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::GetMaxSequenceId(uint64_t& sequence_id) {
    auto max_seq_opt = GetMaxSequenceIdInternal();
    if (!max_seq_opt.has_value()) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    sequence_id = max_seq_opt.value();
    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::UpdateLatestSequenceId(uint64_t sequence_id) {
    std::string key = BuildLatestKey();
    std::string value = std::to_string(sequence_id);
    return EtcdHelper::Put(key.c_str(), key.size(), value.c_str(),
                           value.size());
}

ErrorCode EtcdOpLogStore::RecordSnapshotSequenceId(
    const std::string& snapshot_id, uint64_t sequence_id) {
    std::string key = BuildSnapshotKey(snapshot_id);
    std::string value = std::to_string(sequence_id);
    return EtcdHelper::Put(key.c_str(), key.size(), value.c_str(),
                           value.size());
}

ErrorCode EtcdOpLogStore::GetSnapshotSequenceId(const std::string& snapshot_id,
                                                uint64_t& sequence_id) {
    std::string key = BuildSnapshotKey(snapshot_id);
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
            return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
        }
        return err;
    }

    try {
        sequence_id = std::stoull(value);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse snapshot sequence_id: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::CleanupOpLogBefore(uint64_t before_sequence_id) {
    // Robust cleanup (Scheme 3):
    // - Determine current minimum sequence_id in etcd
    // - DeleteRange [min_key, before_key)
    //
    // IMPORTANT: This relies on lexicographical ordering of keys, so the
    // sequence_id portion MUST be fixed-width (zero-padded).
    auto min_seq_opt = GetMinSequenceId();
    if (!min_seq_opt.has_value()) {
        return ErrorCode::OK;  // nothing to cleanup
    }

    uint64_t min_seq = min_seq_opt.value();
    if (before_sequence_id <= min_seq) {
        return ErrorCode::OK;
    }

    std::string start_key = BuildOpLogKey(min_seq);
    std::string end_key =
        BuildOpLogKey(before_sequence_id);  // delete < before_sequence_id

    return EtcdHelper::DeleteRange(start_key.c_str(), start_key.size(),
                                   end_key.c_str(), end_key.size());
}

std::string EtcdOpLogStore::BuildOpLogKey(uint64_t sequence_id) const {
    std::ostringstream oss;
    // Fixed-width encoding for correct etcd lexicographical range operations.
    // 20 digits is enough for uint64_t max (18446744073709551615).
    oss << kOpLogPrefix << cluster_id_ << "/" << std::setw(20)
        << std::setfill('0') << sequence_id;
    return oss.str();
}

std::optional<uint64_t> EtcdOpLogStore::GetMinSequenceId() const {
    std::string prefix = std::string(kOpLogPrefix) + cluster_id_ + "/";
    std::string first_key;
    ErrorCode err = EtcdHelper::GetFirstKeyWithPrefix(prefix.c_str(),
                                                      prefix.size(), first_key);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Skip non-entry keys if any (e.g. "/latest", "/snapshot/...", or the
    // Stage 3 batch namespace under "/shards/").
    // Entries are expected to be ".../<20-digit-seq>".
    // If the first key isn't an entry key, fall back to nullopt (safe no-op).
    if (first_key.find("/latest") != std::string::npos ||
        first_key.find("/snapshot/") != std::string::npos ||
        first_key.find("/shards/") != std::string::npos) {
        return std::nullopt;
    }

    size_t pos = first_key.rfind('/');
    if (pos == std::string::npos || pos + 1 >= first_key.size()) {
        return std::nullopt;
    }
    std::string seq_str = first_key.substr(pos + 1);
    try {
        return static_cast<uint64_t>(std::stoull(seq_str));
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<uint64_t> EtcdOpLogStore::GetMaxSequenceIdInternal() const {
    // Entry keys are fixed-width 20-digit numbers, which (in practice) start
    // with '0'. Use "/0" to avoid picking up "/latest" which is
    // lexicographically after digits.
    std::string prefix = std::string(kOpLogPrefix) + cluster_id_ + "/0";
    std::string last_key;
    ErrorCode err = EtcdHelper::GetLastKeyWithPrefix(prefix.c_str(),
                                                     prefix.size(), last_key);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    size_t pos = last_key.rfind('/');
    if (pos == std::string::npos || pos + 1 >= last_key.size()) {
        return std::nullopt;
    }
    std::string seq_str = last_key.substr(pos + 1);
    try {
        return static_cast<uint64_t>(std::stoull(seq_str));
    } catch (...) {
        return std::nullopt;
    }
}

std::string EtcdOpLogStore::BuildLatestKey() const {
    std::ostringstream oss;
    oss << kOpLogPrefix << cluster_id_ << kLatestSuffix;
    return oss.str();
}

std::string EtcdOpLogStore::BuildSnapshotKey(
    const std::string& snapshot_id) const {
    std::ostringstream oss;
    oss << kOpLogPrefix << cluster_id_ << kSnapshotSuffix << snapshot_id
        << "/sequence_id";
    return oss.str();
}

// ---------- Stage 3: durable batch namespace ----------

std::string EtcdOpLogStore::BuildBatchKey(uint32_t shard_id,
                                          uint64_t batch_id) const {
    // 20-digit zero-padded batch_id so lexicographic order == numeric order,
    // which lets ReadBatchesSince use a simple range scan.
    std::ostringstream oss;
    oss << kOpLogPrefix << cluster_id_ << kShardsPrefix << shard_id
        << kBatchesSuffix << std::setw(20) << std::setfill('0') << batch_id;
    return oss.str();
}

std::string EtcdOpLogStore::BuildLatestBatchKey(uint32_t shard_id) const {
    std::ostringstream oss;
    oss << kOpLogPrefix << cluster_id_ << kShardsPrefix << shard_id
        << kLatestBatchSuffix;
    return oss.str();
}

std::string EtcdOpLogStore::EncodeLatestBatchValue(uint64_t batch_id) {
    return std::to_string(batch_id);
}

bool EtcdOpLogStore::ParseLatestBatchValue(const std::string& value,
                                           uint64_t& batch_id) {
    try {
        batch_id = std::stoull(value);
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

ErrorCode EtcdOpLogStore::ReadLatestBatchValue(uint32_t shard_id,
                                               uint64_t& batch_id) {
    batch_id = 0;
    std::string key = BuildLatestBatchKey(shard_id);
    std::string value;
    EtcdRevisionId revision_id = 0;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        // No batches yet for this shard — treat as "latest_batch=0".
        // This is the documented empty-shard contract.
        return ErrorCode::OK;
    }
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!ParseLatestBatchValue(value, batch_id)) {
        LOG(ERROR) << "Corrupt latest_batch value under " << key
                   << " (value=" << value << ")";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::CompareBatchRecord(uint32_t shard_id,
                                             uint64_t batch_id,
                                             const OpLogBatchRecord& expected) {
    std::string key = BuildBatchKey(shard_id, batch_id);
    std::string value;
    EtcdRevisionId revision_id = 0;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    if (err != ErrorCode::OK) {
        return err;
    }

    OpLogBatchRecord existing;
    if (!mooncake::DeserializeOpLogBatchRecord(value, existing)) {
        LOG(ERROR) << "Failed to deserialize batch record at " << key
                   << " during CompareBatchRecord";
        return ErrorCode::INTERNAL_ERROR;
    }

    // Re-serialize both sides with a normalized checksum and compare JSON
    // byte-for-byte. This catches every field difference (including
    // producer_view_version and entry-level local_index gaps) while being
    // robust to JsonCpp key ordering.
    //
    // NOTE: owner_token is intentionally NOT compared here. The
    // /latest_batch pointer does not record owner_token, and a legitimate
    // new leader must be allowed to write a different owner_token than the
    // previous leader. Comparing it here would re-introduce the
    // STALE_LEADER-after-failover bug.
    if (existing.shard_id != expected.shard_id ||
        existing.batch_id != expected.batch_id ||
        existing.producer_view_version != expected.producer_view_version ||
        existing.entries.size() != expected.entries.size() ||
        existing.batch_checksum != expected.batch_checksum) {
        return ErrorCode::SEQUENCE_CONFLICT;
    }
    for (size_t i = 0; i < existing.entries.size(); ++i) {
        const auto& a = existing.entries[i];
        const auto& b = expected.entries[i];
        if (a.sequence_id != b.sequence_id ||
            a.timestamp_ms != b.timestamp_ms || a.op_type != b.op_type ||
            a.tenant_id != b.tenant_id || a.object_key != b.object_key ||
            a.payload != b.payload || a.checksum != b.checksum ||
            a.prefix_hash != b.prefix_hash || a.shard_id != b.shard_id ||
            a.batch_id != b.batch_id || a.local_index != b.local_index) {
            return ErrorCode::SEQUENCE_CONFLICT;
        }
    }
    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::AppendBatch(const OpLogBatchRecord& batch) {
    // 1. Caller must have populated batch_checksum via
    //    ComputeOpLogBatchChecksum. Anything else is a programming error
    //    and we surface it as INTERNAL_ERROR rather than silently writing
    //    a bad record.
    if (batch.entries.empty()) {
        LOG(ERROR) << "AppendBatch rejected: batch has no entries"
                   << " (shard=" << batch.shard_id
                   << ", batch_id=" << batch.batch_id << ")";
        return ErrorCode::INVALID_PARAMS;
    }
    if (batch.batch_checksum != mooncake::ComputeOpLogBatchChecksum(batch)) {
        LOG(ERROR) << "AppendBatch rejected: batch_checksum mismatch"
                   << " (shard=" << batch.shard_id
                   << ", batch_id=" << batch.batch_id << ")";
        return ErrorCode::INTERNAL_ERROR;
    }
    // Basic-available HA is single-shard. Refuse other shard_ids explicitly
    // so multi-shard misuse is loud rather than silently accepted.
    if (batch.shard_id != 0) {
        LOG(ERROR) << "AppendBatch rejected: non-zero shard_id is not"
                   << " supported in basic-available HA (shard="
                   << batch.shard_id << ")";
        return ErrorCode::INVALID_PARAMS;
    }

    // 2. Read latest_batch and enforce the batch_id continuity invariant.
    //
    // IMPORTANT: we do NOT compare owner_token here. A legitimate new leader
    // (with a fresh lease_id after failover) must be able to continue the
    // batch_id sequence regardless of the token the previous leader used.
    // Real fencing is enforced at a higher layer (Stage 6 promotion gate /
    // LogWriter session injection) — not by /latest_batch content.
    uint64_t current_batch_id = 0;
    ErrorCode latest_err =
        ReadLatestBatchValue(batch.shard_id, current_batch_id);
    if (latest_err != ErrorCode::OK) {
        return latest_err;
    }
    const bool latest_present = (current_batch_id != 0);

    if (!latest_present) {
        // No durable history yet for this shard. The first batch MUST be
        // batch_id=1.
        if (batch.batch_id != 1) {
            LOG(ERROR) << "AppendBatch rejected: no latest_batch yet but"
                       << " incoming batch_id=" << batch.batch_id
                       << " (expected 1)";
            return ErrorCode::SEQUENCE_CONFLICT;
        }
    } else {
        if (batch.batch_id == current_batch_id) {
            // Same batch_id as the durable one. If everything matches, this
            // is an idempotent retry; otherwise it's a conflict. CompareBatch
            // Record returns OK on equal, SEQUENCE_CONFLICT on differ.
            ErrorCode cmp =
                CompareBatchRecord(batch.shard_id, batch.batch_id, batch);
            if (cmp == ErrorCode::OK) {
                return ErrorCode::OK;
            }
            if (cmp == ErrorCode::SEQUENCE_CONFLICT) {
                return ErrorCode::SEQUENCE_CONFLICT;
            }
            return cmp;
        }
        if (batch.batch_id <= current_batch_id) {
            // Strictly behind the durable pointer. Reject as a stale write —
            // there is no durable reason to rewind the batch id space.
            LOG(ERROR) << "AppendBatch rejected: incoming batch_id="
                       << batch.batch_id
                       << " is behind latest_batch=" << current_batch_id;
            return ErrorCode::SEQUENCE_CONFLICT;
        }
        if (batch.batch_id != current_batch_id + 1) {
            LOG(ERROR) << "AppendBatch rejected: gap between latest_batch="
                       << current_batch_id
                       << " and incoming batch_id=" << batch.batch_id;
            return ErrorCode::SEQUENCE_CONFLICT;
        }
    }

    // 3. Pre-check the batch key. If a record already exists at this
    // (shard, batch_id), it must be byte-identical to what we are about to
    // write. This is the idempotent-retry / conflict path BEFORE the
    // Create so we don't need to roll back on the next step.
    {
        ErrorCode pre_cmp =
            CompareBatchRecord(batch.shard_id, batch.batch_id, batch);
        if (pre_cmp == ErrorCode::OK) {
            // Same payload already durable — idempotent retry.
            return ErrorCode::OK;
        }
        if (pre_cmp != ErrorCode::OPLOG_ENTRY_NOT_FOUND) {
            return pre_cmp;
        }
    }

    // 4. Atomically create the batch record. CreateRevision==0 is etcd's
    // "key must not exist" gate — if two leaders race, only one wins.
    const std::string batch_key = BuildBatchKey(batch.shard_id, batch.batch_id);
    const std::string batch_value = mooncake::SerializeOpLogBatchRecord(batch);
    ErrorCode create_err =
        EtcdHelper::Create(batch_key.c_str(), batch_key.size(),
                           batch_value.c_str(), batch_value.size());
    if (create_err == ErrorCode::ETCD_TRANSACTION_FAIL) {
        // Lost the race. Re-read and compare: same payload → idempotent OK,
        // different payload → SEQUENCE_CONFLICT.
        ErrorCode post_cmp =
            CompareBatchRecord(batch.shard_id, batch.batch_id, batch);
        if (post_cmp == ErrorCode::OK) {
            return ErrorCode::OK;
        }
        return (post_cmp == ErrorCode::OPLOG_ENTRY_NOT_FOUND)
                   ? ErrorCode::ETCD_TRANSACTION_FAIL
                   : post_cmp;
    }
    if (create_err != ErrorCode::OK) {
        return create_err;
    }

    // 5. Advance the latest_batch pointer. We do this as a plain Put rather
    // than a true multi-key Txn: a concurrent leader that races this step
    // will be caught by the batch_id-gap check on the next AppendBatch call
    // (their write will see our batch_id and either advance further or be
    // rejected as a gap). The atomicity here is "best effort"; the durable
    // invariant is that any visible (batch_key, batch_id) record implies
    // latest_batch >= batch_id once the dust settles.
    const std::string latest_key = BuildLatestBatchKey(batch.shard_id);
    const std::string latest_value = EncodeLatestBatchValue(batch.batch_id);
    ErrorCode put_err =
        EtcdHelper::Put(latest_key.c_str(), latest_key.size(),
                        latest_value.c_str(), latest_value.size());
    if (put_err != ErrorCode::OK) {
        LOG(ERROR) << "AppendBatch: batch key written but latest_batch"
                   << " update failed (key=" << batch_key << ", err=" << put_err
                   << "). A later read will see a"
                   << " batch record without a matching latest_batch pointer"
                   << " until the next successful AppendBatch call.";
        // Best-effort: do not undo the batch record. The caller / next
        // AppendBatch will repair the pointer on success.
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::ReadBatch(uint32_t shard_id, uint64_t batch_id,
                                    OpLogBatchRecord& batch) {
    std::string key = BuildBatchKey(shard_id, batch_id);
    std::string value;
    EtcdRevisionId revision_id = 0;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!mooncake::DeserializeOpLogBatchRecord(value, batch)) {
        LOG(ERROR) << "Failed to deserialize batch record at " << key;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::ReadBatchesSince(
    uint32_t shard_id, uint64_t start_batch_id, size_t limit,
    std::vector<OpLogBatchRecord>& batches) {
    batches.clear();
    if (limit == 0) {
        return ErrorCode::OK;
    }

    // Range scan over the fixed-width 20-digit batch key, starting just past
    // start_batch_id. We use the same GetRangeAsJson pipeline as the legacy
    // sequence scan because it gives us page-level revision metadata and
    // avoids the C++/Go memory juggling that a per-key Get would require.
    std::ostringstream prefix_oss;
    prefix_oss << kOpLogPrefix << cluster_id_ << kShardsPrefix << shard_id
               << kBatchesSuffix;
    const std::string prefix = prefix_oss.str();

    auto prefix_end = [](std::string p) -> std::string {
        for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i) {
            unsigned char c = static_cast<unsigned char>(p[i]);
            if (c < 0xFF) {
                p[i] = static_cast<char>(c + 1);
                p.resize(i + 1);
                return p;
            }
        }
        return std::string(1, '\0');
    };
    const std::string end_key = prefix_end(prefix);

    // Build start key as the prefix + 20-digit (start_batch_id + 1), so the
    // scan is naturally exclusive of start_batch_id and inclusive of the
    // next batch.
    std::ostringstream start_oss;
    start_oss << prefix << std::setw(20) << std::setfill('0')
              << (start_batch_id + 1);
    std::string current_start_key = start_oss.str();

    while (batches.size() < limit) {
        const size_t page_limit = limit - batches.size();
        std::string json;
        EtcdRevisionId page_rev = 0;
        ErrorCode err = EtcdHelper::GetRangeAsJson(
            current_start_key.c_str(), current_start_key.size(),
            end_key.c_str(), end_key.size(), page_limit, json, page_rev);
        if (err != ErrorCode::OK) {
            return err;
        }
        if (json.empty()) {
            break;
        }

        Json::Value root;
        Json::CharReaderBuilder reader;
        std::string errs;
        std::istringstream s(json);
        if (!Json::parseFromStream(reader, s, &root, &errs)) {
            LOG(ERROR) << "Failed to parse range JSON in ReadBatchesSince: "
                       << errs;
            return ErrorCode::INTERNAL_ERROR;
        }
        if (!root.isArray() || root.empty()) {
            break;
        }

        std::string last_key_in_page;
        for (const auto& kv : root) {
            const std::string key = kv.get("key", "").asString();
            last_key_in_page = key;
            const std::string value = kv.get("value", "").asString();

            // Defensive: skip anything that isn't under the batch prefix.
            if (key.rfind(prefix, 0) != 0) {
                continue;
            }

            OpLogBatchRecord batch;
            if (!mooncake::DeserializeOpLogBatchRecord(value, batch)) {
                LOG(ERROR) << "Failed to deserialize batch record from key="
                           << key;
                return ErrorCode::INTERNAL_ERROR;
            }
            batches.push_back(std::move(batch));
            if (batches.size() >= limit) {
                break;
            }
        }

        if (last_key_in_page.empty()) {
            break;
        }
        // Advance start for next page: one byte past last_key_in_page.
        current_start_key = last_key_in_page;
        current_start_key.push_back('\0');
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::GetLatestBatchId(uint32_t shard_id,
                                           uint64_t& batch_id) {
    return ReadLatestBatchValue(shard_id, batch_id);
}

std::unique_ptr<OpLogChangeNotifier> EtcdOpLogStore::CreateChangeNotifier(
    const std::string& cluster_id) {
    return std::make_unique<EtcdOpLogChangeNotifier>(cluster_id, this);
}

void EtcdOpLogStore::BatchUpdateThread() {
    if (!enable_latest_seq_batch_update_) {
        return;
    }
    while (batch_update_running_.load()) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kBatchIntervalMs));

        // Check if we need to update based on time interval
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - last_update_time_)
                           .count();

        if (pending_count_.load() > 0 && elapsed >= kBatchIntervalMs) {
            DoBatchUpdate();
        }
    }
}

void EtcdOpLogStore::TriggerBatchUpdateIfNeeded() {
    // This method is kept for potential future use (e.g., manual trigger)
    // Currently, DoBatchUpdate() is called directly from WriteOpLog
    // when batch size threshold is reached
    if (pending_count_.load() >= kBatchSize) {
        DoBatchUpdate();
    }
}

void EtcdOpLogStore::DoBatchUpdate() {
    if (!enable_latest_seq_batch_update_) {
        return;
    }
    std::lock_guard<std::mutex> lock(batch_update_mutex_);

    // Get the pending sequence_id and reset counters
    uint64_t seq_id_to_update = pending_latest_seq_id_.load();
    size_t count = pending_count_.exchange(0);

    if (count == 0) {
        return;  // Nothing to update
    }

    // Update latest_sequence_id in etcd
    ErrorCode err = UpdateLatestSequenceId(seq_id_to_update);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to batch update latest_sequence_id="
                     << seq_id_to_update << ", error=" << err
                     << ". Will retry in next batch.";
        // Restore the count so it will be retried
        pending_count_.fetch_add(count);
    } else {
        last_update_time_ = std::chrono::steady_clock::now();
        VLOG(2) << "Batch updated latest_sequence_id=" << seq_id_to_update
                << " (count=" << count << " entries)";
    }
}

}  // namespace mooncake
