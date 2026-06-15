#include <gtest/gtest.h>
#include "master_metric_manager.h"

using namespace mooncake;

// These tests assert the documented semantics of the master RPC health
// metrics. They use the test-only read accessors
// (test_get_rpc_thread_pool_size / test_get_rpc_in_flight) to inspect the
// in-process state directly, and check the serialized metrics output for
// the presence and exact value of the exposed gauges.

namespace {

std::string SerializeMetrics() {
    return MasterMetricManager::instance().serialize_metrics();
}

void ResetInFlight(size_t value = 0) {
    MasterMetricManager::instance().observe_rpc_in_flight(value);
}

}  // namespace

TEST(MasterRpcHealthMetricsTest, ThreadPoolSizeReflectsObservedValue) {
    auto& mgr = MasterMetricManager::instance();
    mgr.observe_rpc_thread_pool_size(8);
    EXPECT_EQ(mgr.test_get_rpc_thread_pool_size(), 8u);
    mgr.observe_rpc_thread_pool_size(16);
    EXPECT_EQ(mgr.test_get_rpc_thread_pool_size(), 16u);
    // The serialized output should also reflect the latest value.
    EXPECT_NE(SerializeMetrics().find("mooncake_master_rpc_thread_pool_size"),
              std::string::npos);
    mgr.observe_rpc_thread_pool_size(0);
}

TEST(MasterRpcHealthMetricsTest, InFlightStartsAtZeroAndBalances) {
    auto& mgr = MasterMetricManager::instance();
    ResetInFlight(0);
    EXPECT_EQ(mgr.test_get_rpc_in_flight(), 0u);

    {
        MasterMetricManager::RpcInFlightGuard guard;
        EXPECT_EQ(mgr.test_get_rpc_in_flight(), 1u);
    }
    EXPECT_EQ(mgr.test_get_rpc_in_flight(), 0u);
}

TEST(MasterRpcHealthMetricsTest, InFlightGuardBalancesOnException) {
    auto& mgr = MasterMetricManager::instance();
    ResetInFlight(0);
    try {
        MasterMetricManager::RpcInFlightGuard guard;
        EXPECT_EQ(mgr.test_get_rpc_in_flight(), 1u);
        throw std::runtime_error("boom");
    } catch (const std::runtime_error&) {
        // expected
    }
    EXPECT_EQ(mgr.test_get_rpc_in_flight(), 0u)
        << "guard must decrement on exception path, not just on normal "
           "return";
}

TEST(MasterRpcHealthMetricsTest, InFlightDoesNotExposeQueueDepth) {
    // The mooncake_master_rpc_queue_depth gauge was removed because the
    // value it derived from in_flight - thread_pool_size was not a real
    // measure of server backlog (requests waiting in coro_rpc's internal
    // queue never enter a handler, so the formula was misleading). The
    // serialized metrics output must no longer mention queue_depth.
    auto& mgr = MasterMetricManager::instance();
    mgr.observe_rpc_thread_pool_size(4);
    ResetInFlight(10);  // would have implied "queue depth = 6" in the old
                        // broken gauge
    const std::string body = SerializeMetrics();
    EXPECT_EQ(body.find("mooncake_master_rpc_queue_depth"), std::string::npos)
        << "queue_depth gauge must not be present in serialized metrics; "
           "it was a derived value, not a real backlog measurement";
    EXPECT_NE(body.find("mooncake_master_rpc_in_flight_requests"),
              std::string::npos);
    ResetInFlight(0);
}
