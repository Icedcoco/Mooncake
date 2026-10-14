#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <json/json.h>
#include <ylt/util/tl/expected.hpp>

#include "mock_client/operation_selection.h"

namespace mock_client {

struct MockSegmentConfig {
    std::string id;
    std::string name;
    uint64_t capacity_bytes = 0;
    std::string protocol;
    std::string transport_endpoint;
    uint64_t base_address = 0;
    std::string host_id;
};

struct MockClientDefinition {
    std::string uuid;
    std::string host_id;
    uint64_t seed_offset = 0;
    std::vector<MockSegmentConfig> segments;
};

struct MockClientConfig {
    uint32_t schema_version = 1;

    std::string run_id;
    uint64_t seed = 1;
    std::string namespace_name = "mock-client";
    uint64_t duration_seconds = 60;

    std::vector<std::string> master_endpoints;
    std::vector<std::string> metrics_endpoints;
    uint32_t request_timeout_ms = 5000;
    uint32_t scrape_interval_ms = 1000;

    std::vector<MockClientDefinition> clients;

    std::string workload_profile;
    uint64_t target_logical_ops_per_second = 0;
    uint32_t concurrency = 1;
    BatchSizeRange batch_size{1, 1};
    std::vector<OperationSelection> operations;

    Json::Value faults = Json::Value(Json::arrayValue);
    std::string jsonl_path;
    std::string raw_metrics_path;
    std::string summary_path;

    // Preserve the source objects for fields introduced by later runtime tasks.
    Json::Value workload_settings = Json::Value(Json::objectValue);
    Json::Value report_settings = Json::Value(Json::objectValue);

    static tl::expected<MockClientConfig, ConfigError> LoadFile(
        const std::string& path,
        std::optional<std::string_view> operation_override = std::nullopt);
};

}  // namespace mock_client
