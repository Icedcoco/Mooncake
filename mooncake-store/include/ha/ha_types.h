#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake {
namespace ha {

using ClusterNamespace = std::string;
using OwnerToken = std::string;
using OpLogSequenceId = uint64_t;
using SnapshotId = std::string;

enum class HABackendType {
    UNKNOWN = 0,
    ETCD = 1,
    REDIS = 2,
    K8S = 3,
};

inline std::string HABackendTypeToString(HABackendType type) {
    switch (type) {
        case HABackendType::UNKNOWN:
            return "unknown";
        case HABackendType::ETCD:
            return "etcd";
        case HABackendType::REDIS:
            return "redis";
        case HABackendType::K8S:
            return "k8s";
    }
    return "unknown";
}

inline std::optional<HABackendType> ParseHABackendType(std::string_view type) {
    if (type == "etcd") {
        return HABackendType::ETCD;
    }
    if (type == "redis") {
        return HABackendType::REDIS;
    }
    if (type == "k8s") {
        return HABackendType::K8S;
    }
    return std::nullopt;
}

inline ErrorCode ValidateHABackendAvailability(HABackendType type) {
    switch (type) {
        case HABackendType::UNKNOWN:
            return ErrorCode::INVALID_PARAMS;
        case HABackendType::ETCD:
#ifdef STORE_USE_ETCD
            return ErrorCode::OK;
#else
            return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
        case HABackendType::REDIS:
#ifdef STORE_USE_REDIS
            return ErrorCode::OK;
#else
            return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
        case HABackendType::K8S:
            return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    return ErrorCode::INVALID_PARAMS;
}

struct HABackendSpec {
    HABackendType type = HABackendType::UNKNOWN;
    std::string connstring;
    ClusterNamespace cluster_namespace;
};

struct MasterView {
    std::string leader_address;
    ViewVersionId view_version = 0;
};

struct LeadershipSession {
    MasterView view;
    // Backend-issued opaque ownership token. Only the backend that created
    // this session is responsible for minting and interpreting it.
    OwnerToken owner_token;
    std::chrono::milliseconds lease_ttl{0};
};

enum class AcquireLeadershipStatus {
    ACQUIRED,
    CONTENDED,
};

struct AcquireLeadershipResult {
    AcquireLeadershipStatus status = AcquireLeadershipStatus::CONTENDED;
    std::optional<LeadershipSession> session;
    std::optional<MasterView> observed_view;
};

struct ViewChangeResult {
    bool changed = false;
    bool timed_out = false;
    std::optional<MasterView> current_view;
};

enum class MasterRuntimeState {
    kStarting,
    kStandby,
    kCandidate,
    kRecovering,
    kCatchingUp,
    kLeaderWarmup,
    kServing,
};

inline const char* MasterRuntimeStateToString(MasterRuntimeState state) {
    switch (state) {
        case MasterRuntimeState::kStarting:
            return "starting";
        case MasterRuntimeState::kStandby:
            return "standby";
        case MasterRuntimeState::kCandidate:
            return "candidate";
        case MasterRuntimeState::kRecovering:
            return "recovering";
        case MasterRuntimeState::kCatchingUp:
            return "catching_up";
        case MasterRuntimeState::kLeaderWarmup:
            return "leader_warmup";
        case MasterRuntimeState::kServing:
            return "serving";
    }
    return "unknown";
}

inline const char* MasterRuntimeRoleToString(MasterRuntimeState state) {
    switch (state) {
        case MasterRuntimeState::kLeaderWarmup:
        case MasterRuntimeState::kServing:
            return "leader";
        case MasterRuntimeState::kStarting:
        case MasterRuntimeState::kStandby:
        case MasterRuntimeState::kCandidate:
        case MasterRuntimeState::kRecovering:
        case MasterRuntimeState::kCatchingUp:
            return "standby";
    }
    return "unknown";
}

enum class LeadershipLossReason {
    kRenewError,
    kLostLeadership,
};

inline const char* LeadershipLossReasonToString(LeadershipLossReason reason) {
    switch (reason) {
        case LeadershipLossReason::kRenewError:
            return "renew_error";
        case LeadershipLossReason::kLostLeadership:
            return "lost_leadership";
    }
    return "unknown";
}

using LeadershipLostCallback = std::function<void(LeadershipLossReason reason)>;

struct OpLogRecord {
    OpLogSequenceId seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string payload;
};

struct OpLogAppendRequest {
    OpLogSequenceId expected_next_seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string payload;
};

struct OpLogPollResult {
    std::vector<OpLogRecord> records;
    OpLogSequenceId next_seq = 0;
    bool timed_out = false;
};

struct SnapshotDescriptor {
    SnapshotId snapshot_id;
    OpLogSequenceId last_included_seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string manifest_key;
    std::string object_prefix;
    int64_t created_at_ms = 0;
};

// ----------------------------------------------------------------------------
// Stage 1: HA capability gate types.
// These describe the *logical* capability matrix that determines whether a
// given configuration is allowed to claim "full HA". They are intentionally
// independent of build-time availability (e.g. STORE_USE_ETCD). The runtime
// availability check stays in BuildHABackendSpec; this gate is a separate
// pre-flight that only enforces the basic-available contract.
// ----------------------------------------------------------------------------

enum class HARequiredLevel {
    kDisabled,
    kDegraded,
    kFull,
};

inline std::optional<HARequiredLevel> ParseHARequiredLevel(
    std::string_view level) {
    if (level == "disabled") return HARequiredLevel::kDisabled;
    if (level == "degraded") return HARequiredLevel::kDegraded;
    if (level == "full") return HARequiredLevel::kFull;
    return std::nullopt;
}

inline std::string HARequiredLevelToString(HARequiredLevel level) {
    switch (level) {
        case HARequiredLevel::kDisabled:
            return "disabled";
        case HARequiredLevel::kDegraded:
            return "degraded";
        case HARequiredLevel::kFull:
            return "full";
    }
    return "unknown";
}

enum class HAOpLogFormat {
    kLegacySequence,
    kBatched,
};

inline std::optional<HAOpLogFormat> ParseHAOpLogFormat(
    std::string_view format) {
    if (format == "legacy_sequence") return HAOpLogFormat::kLegacySequence;
    if (format == "batched") return HAOpLogFormat::kBatched;
    return std::nullopt;
}

inline std::string HAOpLogFormatToString(HAOpLogFormat format) {
    switch (format) {
        case HAOpLogFormat::kLegacySequence:
            return "legacy_sequence";
        case HAOpLogFormat::kBatched:
            return "batched";
    }
    return "unknown";
}

enum class HAPutEndCommitMode {
    kAsync,
    kWaitBatchDurable,
    kStrict,
};

inline std::optional<HAPutEndCommitMode> ParseHAPutEndCommitMode(
    std::string_view mode) {
    if (mode == "async") return HAPutEndCommitMode::kAsync;
    if (mode == "wait_batch_durable")
        return HAPutEndCommitMode::kWaitBatchDurable;
    if (mode == "strict") return HAPutEndCommitMode::kStrict;
    return std::nullopt;
}

inline std::string HAPutEndCommitModeToString(HAPutEndCommitMode mode) {
    switch (mode) {
        case HAPutEndCommitMode::kAsync:
            return "async";
        case HAPutEndCommitMode::kWaitBatchDurable:
            return "wait_batch_durable";
        case HAPutEndCommitMode::kStrict:
            return "strict";
    }
    return "unknown";
}

struct HABackendCapabilities {
    bool leader_election = false;
    bool fencing = false;
    bool batched_ordered_oplog = false;
    bool oplog_following = false;
    bool snapshot_catalog = false;
    bool full_ha = false;
};

// Logical capability matrix per backend. This is a *static* property of the
// backend type, not of the current build. Redis and K8s cannot serve full HA
// even when their respective wrappers are compiled in.
inline HABackendCapabilities GetHABackendCapabilities(HABackendType type) {
    switch (type) {
        case HABackendType::ETCD:
            return HABackendCapabilities{
                .leader_election = true,
                .fencing = true,
                .batched_ordered_oplog = true,
                .oplog_following = true,
                .snapshot_catalog = true,
                .full_ha = true,
            };
        case HABackendType::REDIS:
        case HABackendType::K8S:
        case HABackendType::UNKNOWN:
        default:
            return HABackendCapabilities{};
    }
}

struct HACapabilityConfig {
    std::string ha_required_level = "degraded";
    std::string ha_backend_type = "etcd";
    std::string ha_oplog_format = "legacy_sequence";
    uint32_t ha_oplog_shard_count = 1;
    std::string ha_put_end_commit_mode = "async";
};

// Validates the basic-available HA capability gate. This is a *logical*
// pre-flight: it does not check build-time availability (handled separately
// in BuildHABackendSpec). It enforces:
//   - ha_required_level parses to one of {disabled, degraded, full};
//   - "full" requires a backend whose capabilities report full_ha = true;
//   - "full" requires batched OpLog format;
//   - "full" requires a single shard;
//   - "full" requires async PUT_END commit mode.
//
// On success returns ErrorCode::OK. On failure returns a descriptive error
// identifying the violating field.
inline ErrorCode ValidateHACapabilityConfig(const HACapabilityConfig& config) {
    auto level = ParseHARequiredLevel(config.ha_required_level);
    if (!level.has_value()) {
        return ErrorCode::INVALID_PARAMS;
    }

    // Disabled and degraded HA are not subject to the full-HA matrix.
    // Degraded HA still runs the legacy single-entry oplog path; disabled
    // HA bypasses HA entirely. The capability gate must not interfere with
    // either.
    if (level.value() != HARequiredLevel::kFull) {
        return ErrorCode::OK;
    }

    auto format = ParseHAOpLogFormat(config.ha_oplog_format);
    if (!format.has_value()) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (format.value() != HAOpLogFormat::kBatched) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    if (config.ha_oplog_shard_count != 1) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto commit_mode = ParseHAPutEndCommitMode(config.ha_put_end_commit_mode);
    if (!commit_mode.has_value()) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (commit_mode.value() != HAPutEndCommitMode::kAsync) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    auto backend_type = ParseHABackendType(config.ha_backend_type);
    if (!backend_type.has_value()) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto capabilities = GetHABackendCapabilities(backend_type.value());
    if (!capabilities.full_ha) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }

    return ErrorCode::OK;
}

}  // namespace ha
}  // namespace mooncake
