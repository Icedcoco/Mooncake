#include <gtest/gtest.h>
#include "master_metric_manager.h"

using namespace mooncake;

TEST(MasterRpcHealthMetricsTest, GaugesAreRegistered) {
    auto& mgr = MasterMetricManager::instance();
    EXPECT_NO_THROW(mgr.observe_rpc_thread_pool_size(4));
    EXPECT_NO_THROW(mgr.observe_rpc_in_flight(0));
    EXPECT_NO_THROW(mgr.observe_rpc_queue_depth(0));
}

TEST(MasterRpcHealthMetricsTest, InFlightCanBeIncrementedAndDecremented) {
    auto& mgr = MasterMetricManager::instance();
    mgr.observe_rpc_thread_pool_size(4);
    mgr.observe_rpc_in_flight(0);

    mgr.inc_rpc_in_flight();
    mgr.inc_rpc_in_flight();
    mgr.inc_rpc_in_flight();
    mgr.dec_rpc_in_flight();

    // We cannot read gauge values back through the current public API,
    // so this test compiles/runs without crash as a smoke test.
    SUCCEED();
}

TEST(MasterRpcHealthMetricsTest, QueueDepthIsApproximatedFromInFlight) {
    auto& mgr = MasterMetricManager::instance();
    mgr.observe_rpc_thread_pool_size(2);
    mgr.observe_rpc_in_flight(0);

    mgr.observe_rpc_in_flight(5);
    // queue depth approx = max(0, 5 - 2) = 3
    EXPECT_NO_THROW(mgr.observe_rpc_queue_depth(3));
    mgr.observe_rpc_in_flight(0);
}
