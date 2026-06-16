#include <gtest/gtest.h>

#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <regex>
#include <set>
#include <string>
#include <vector>

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

// ---------------------------------------------------------------------------
// Static source-level coverage check: every handler that is registered with
// the master coro_rpc_server must take the in-flight guard (either
// explicitly, or via the execute_rpc helper that contains it transitively).
//
// This is the test that would have caught the ServiceReady /
// MountLocalDiskSegment / OffloadObjectHeartbeat / ReportSsdCapacity /
// NotifyOffloadSuccess / PromotionObjectHeartbeat / PromotionAllocStart /
// NotifyPromotionSuccess / NotifyPromotionFailure / CreateDrainJob /
// QueryDrainJob / CancelDrainJob / QuerySegmentStatus / QuerySegmentStatusById
// / RemoveAll leaks before they shipped.
//
// The test reads rpc_service.cpp from the source tree (located relative to
// the test binary's CWD or via MOONCAKE_SOURCE_ROOT). It extracts:
//   1. Every method name registered in RegisterRpcService().
//   2. The body of every WrappedMasterService::Foo() definition in the
//      same file.
// It then asserts (2) covers (1) — i.e. every registered handler has a
// corresponding definition that contains either `RpcInFlightGuard` or
// `execute_rpc`.
// ---------------------------------------------------------------------------

std::string ReadFile(const std::string& path) {
    std::ifstream in(path);
    if (!in) return {};
    return std::string((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>{});
}

std::string LocateSourceRoot() {
    // 1. Explicit override.
    if (const char* env = std::getenv("MOONCAKE_SOURCE_ROOT")) {
        return env;
    }
    // 2. Walk up from CWD looking for a directory that contains
    //    mooncake-store/src/rpc_service.cpp.
    char buf[4096] = {0};
    if (!getcwd(buf, sizeof(buf))) return {};
    std::string dir = buf;
    for (int i = 0; i < 8; ++i) {
        std::string candidate = dir + "/mooncake-store/src/rpc_service.cpp";
        std::ifstream test(candidate);
        if (test) return dir;
        auto pos = dir.find_last_of('/');
        if (pos == std::string::npos) break;
        dir = dir.substr(0, pos);
    }
    return {};
}

struct HandlerBody {
    std::string name;
    std::string body;
};

// Find the body of `WrappedMasterService::Name(` and return the text
// from the opening `{` to its matching `}`. Returns empty body if the
// definition cannot be found.
std::vector<HandlerBody> ExtractHandlerBodies(const std::string& source) {
    std::vector<HandlerBody> out;
    // Match the start of a definition: optional return type, then
    // WrappedMasterService::Name(.
    const std::regex def_re(
        R"((?:\n|^)(?:[A-Za-z_:<> ,&*]+\s+)?WrappedMasterService::([A-Za-z]+)\s*\()",
        std::regex::ECMAScript);
    auto begin = std::sregex_iterator(source.begin(), source.end(), def_re);
    auto end = std::sregex_iterator();
    for (auto it = begin; it != end; ++it) {
        std::string name = (*it)[1].str();
        size_t match_pos = it->position() + it->length();
        // Walk forward to the next `{` (the function body opener).
        size_t brace = source.find('{', match_pos);
        if (brace == std::string::npos) continue;
        // Find the matching `}`. The first one inside a function body
        // could be a brace-init list; we do a simple depth-count from
        // the opening brace.
        int depth = 0;
        size_t end_pos = brace;
        for (size_t i = brace; i < source.size(); ++i) {
            if (source[i] == '{') ++depth;
            else if (source[i] == '}') {
                --depth;
                if (depth == 0) {
                    end_pos = i;
                    break;
                }
            }
        }
        if (depth != 0) continue;
        HandlerBody hb;
        hb.name = name;
        hb.body = source.substr(brace, end_pos - brace + 1);
        out.push_back(std::move(hb));
    }
    return out;
}

std::set<std::string> ExtractRegisteredHandlerNames(const std::string& source) {
    // Find the body of RegisterRpcService, then within it find every
    // `WrappedMasterService::Name` reference.
    auto pos = source.find("void RegisterRpcService(");
    if (pos == std::string::npos) return {};
    size_t brace = source.find('{', pos);
    if (brace == std::string::npos) return {};
    int depth = 0;
    size_t end_pos = brace;
    for (size_t i = brace; i < source.size(); ++i) {
        if (source[i] == '{') ++depth;
        else if (source[i] == '}') {
            --depth;
            if (depth == 0) {
                end_pos = i;
                break;
            }
        }
    }
    if (depth != 0) return {};
    const std::string body = source.substr(brace, end_pos - brace + 1);
    const std::regex name_re(R"(WrappedMasterService::([A-Za-z]+))");
    std::set<std::string> names;
    auto b = std::sregex_iterator(body.begin(), body.end(), name_re);
    auto e = std::sregex_iterator();
    for (auto it = b; it != e; ++it) {
        names.insert((*it)[1].str());
    }
    return names;
}

bool HandlerBodyHasGuard(const HandlerBody& hb) {
    return hb.body.find("RpcInFlightGuard") != std::string::npos ||
           hb.body.find("execute_rpc") != std::string::npos;
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

// The wrapper-boundary coverage test: every handler that is registered
// with the master coro_rpc_server must take an in-flight guard at the
// wrapper boundary. This is the regression guard for the issue
// observed on the dev/fix_2039_store_main branch where ~15 handlers
// (ServiceReady, MountLocalDiskSegment, OffloadObjectHeartbeat,
// ReportSsdCapacity, NotifyOffloadSuccess, PromotionObjectHeartbeat,
// PromotionAllocStart, NotifyPromotionSuccess, NotifyPromotionFailure,
// CreateDrainJob, QueryDrainJob, CancelDrainJob, QuerySegmentStatus,
// QuerySegmentStatusById, RemoveAll) had been registered without a
// guard, so mooncake_master_rpc_in_flight_requests was undercounting
// real master load.
//
// We accept the guard either as a direct `RpcInFlightGuard` declaration
// at the top of the handler body, or transitively via `execute_rpc`
// (which contains the guard in its template body).
TEST(MasterRpcHealthMetricsTest,
     EveryRegisteredHandlerTakesInFlightGuard) {
    const std::string source_root = LocateSourceRoot();
    if (source_root.empty()) {
        GTEST_SKIP() << "could not locate mooncake source root; set "
                        "MOONCAKE_SOURCE_ROOT to run this test";
    }
    const std::string path =
        source_root + "/mooncake-store/src/rpc_service.cpp";
    const std::string source = ReadFile(path);
    if (source.empty()) {
        GTEST_SKIP() << "could not read " << path
                     << "; check MOONCAKE_SOURCE_ROOT";
    }

    const std::set<std::string> registered =
        ExtractRegisteredHandlerNames(source);
    ASSERT_FALSE(registered.empty())
        << "could not extract registered handler names from " << path;

    const std::vector<HandlerBody> bodies = ExtractHandlerBodies(source);
    ASSERT_FALSE(bodies.empty())
        << "could not extract handler bodies from " << path;

    // Build a name -> body map for fast lookup.
    std::set<std::string> guarded;
    std::vector<std::string> missing;
    for (const auto& name : registered) {
        bool found = false;
        for (const auto& hb : bodies) {
            if (hb.name == name) {
                if (HandlerBodyHasGuard(hb)) {
                    guarded.insert(name);
                } else {
                    missing.push_back(name);
                }
                found = true;
                break;
            }
        }
        if (!found) {
            missing.push_back(name + " (no definition found)");
        }
    }

    EXPECT_TRUE(missing.empty())
        << "the following registered handlers are missing an "
           "RpcInFlightGuard (or execute_rpc) in their definition: "
        << [&] {
               std::string out;
               for (const auto& m : missing) out += m + ", ";
               return out;
           }()
        << "\nEvery handler registered in RegisterRpcService() must "
           "either declare RpcInFlightGuard at the top of its body or "
           "call execute_rpc() (which contains the guard). Otherwise "
           "mooncake_master_rpc_in_flight_requests will undercount real "
           "master load.";

    // Sanity check: at least 50 registered handlers (current count is
    // 59). If this drops sharply, the registration block has been
    // refactored and this test should be re-evaluated.
    EXPECT_GE(registered.size(), 50u)
        << "registered handler count dropped below 50; the registration "
           "block may have been refactored and this static check may "
           "no longer cover the intended surface";
}
