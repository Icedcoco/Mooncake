#include "ha/ha_types.h"

#include <gtest/gtest.h>

#include "ha/leadership/master_service_supervisor.h"
#include "master_config.h"

namespace mooncake {
namespace ha {
namespace {

// Capability gate: full HA is only allowed when every prerequisite is
// satisfied (etcd backend, batched oplog, single shard, async PUT_END).
// This test file pins down the matrix defined in
// mydocs/mooncake_store_master_ha_basic_available_staged_tdd_plan_2026-06-09.md
// §4.3.

HACapabilityConfig MakeFullHAEtcdBatchedConfig() {
    HACapabilityConfig config;
    config.ha_required_level = "full";
    config.ha_backend_type = "etcd";
    config.ha_oplog_format = "batched";
    config.ha_oplog_shard_count = 1;
    config.ha_put_end_commit_mode = "async";
    return config;
}

TEST(HACapabilityConfigTest, FullHAWithRedisBackendIsRejected) {
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_backend_type = "redis";
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHACapabilityConfig(config));
}

TEST(HACapabilityConfigTest, FullHAWithK8sBackendIsRejected) {
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_backend_type = "k8s";
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHACapabilityConfig(config));
}

TEST(HACapabilityConfigTest, FullHAWithLegacyOpLogFormatIsRejected) {
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_oplog_format = "legacy_sequence";
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHACapabilityConfig(config));
}

TEST(HACapabilityConfigTest, MultiShardOpLogCountIsRejected) {
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_oplog_shard_count = 2;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, ValidateHACapabilityConfig(config));
}

TEST(HACapabilityConfigTest, StrictPutEndCommitModeIsRejected) {
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_put_end_commit_mode = "strict";
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHACapabilityConfig(config));
}

TEST(HACapabilityConfigTest, DegradedHAWithEtcdBatchedIsAccepted) {
    // Sanity check: the gate is not a blanket rejector. A non-full
    // configuration that the legacy path already supports must continue
    // to be accepted.
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_required_level = "degraded";
    EXPECT_EQ(ErrorCode::OK, ValidateHACapabilityConfig(config));
}

TEST(HACapabilityConfigTest, DisabledHAAlwaysAccepted) {
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_required_level = "disabled";
    // Even with a backend that would otherwise be rejected, "disabled"
    // means HA is not used at all and the gate must not interfere.
    config.ha_backend_type = "redis";
    config.ha_oplog_format = "legacy_sequence";
    config.ha_oplog_shard_count = 2;
    config.ha_put_end_commit_mode = "strict";
    EXPECT_EQ(ErrorCode::OK, ValidateHACapabilityConfig(config));
}

TEST(HACapabilityConfigTest, UnknownRequiredLevelStringIsRejected) {
    auto config = MakeFullHAEtcdBatchedConfig();
    config.ha_required_level = "ultimate";
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, ValidateHACapabilityConfig(config));
}

// ----------------------------------------------------------------------------
// Stage 1b: wiring test. Validates that the capability gate is actually
// reachable from the supervisor-level configuration object. This is the
// pre-flight that the supervisor's Start() must run before bringing up the
// leader/standby machinery.
// ----------------------------------------------------------------------------

TEST(HACapabilityConfigTest, SupervisorConfigWithFullHAAndRedisIsRejected) {
    MasterServiceSupervisorConfig config;
    config.ha_required_level = "full";
    config.ha_oplog_format = "batched";
    config.ha_oplog_shard_count = 1;
    config.ha_put_end_commit_mode = "async";
    config.ha_backend_type = "redis";
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateMasterServiceSupervisorHAConfig(config));
}

TEST(HACapabilityConfigTest,
     SupervisorConfigWithFullHAAndMultiShardIsRejected) {
    MasterServiceSupervisorConfig config;
    config.ha_required_level = "full";
    config.ha_oplog_format = "batched";
    config.ha_oplog_shard_count = 4;
    config.ha_put_end_commit_mode = "async";
    config.ha_backend_type = "etcd";
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              ValidateMasterServiceSupervisorHAConfig(config));
}

TEST(HACapabilityConfigTest,
     SupervisorConfigWithDefaultDegradedLegacyIsAccepted) {
    // Backward-compatibility sanity: a configuration that exercises only
    // the legacy single-entry oplog path must continue to be accepted.
    MasterServiceSupervisorConfig config;
    // ha_required_level defaults to "degraded" and the rest follow the
    // legacy defaults; this must validate OK.
    EXPECT_EQ(ErrorCode::OK, ValidateMasterServiceSupervisorHAConfig(config));
}

}  // namespace
}  // namespace ha
}  // namespace mooncake
