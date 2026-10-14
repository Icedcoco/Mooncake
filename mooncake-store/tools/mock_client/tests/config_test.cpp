#include "mock_client/config.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

#include <gtest/gtest.h>

namespace mock_client {
namespace {

Json::Value ValidConfig() {
    Json::Value root(Json::objectValue);
    root["schema_version"] = 1;
    root["run"]["run_id"] = "config-test";
    root["run"]["seed"] = Json::UInt64(17);
    root["run"]["namespace"] = "config-test";
    root["run"]["duration_seconds"] = 1;

    root["master"]["endpoints"] = Json::Value(Json::arrayValue);
    root["master"]["endpoints"].append("127.0.0.1:50051");
    root["master"]["metrics_endpoints"] = Json::Value(Json::arrayValue);
    root["master"]["metrics_endpoints"].append("http://127.0.0.1:50052/metrics");
    root["master"]["request_timeout_ms"] = 1000;

    Json::Value client(Json::objectValue);
    client["uuid"] = "00000000-0000-4000-8000-000000000001";
    client["host_id"] = "madsys-01";
    client["segments"] = Json::Value(Json::arrayValue);
    Json::Value segment(Json::objectValue);
    segment["id"] = "00000000-0000-4000-8000-000000000101";
    segment["name"] = "segment-a";
    segment["capacity_bytes"] = Json::UInt64(1048576);
    segment["protocol"] = "tcp";
    segment["transport_endpoint"] = "127.0.0.1:18000";
    segment["base_address"] = Json::UInt64(4096);
    client["segments"].append(segment);
    root["clients"] = Json::Value(Json::arrayValue);
    root["clients"].append(client);

    root["workload"]["profile"] = "low";
    root["workload"]["target_logical_ops_per_second"] = Json::UInt64(10);
    root["workload"]["concurrency"] = 1;
    root["workload"]["batch_size"]["min"] = 1;
    root["workload"]["batch_size"]["max"] = 8;
    root["workload"]["operations"] = Json::Value(Json::arrayValue);
    Json::Value operation(Json::objectValue);
    operation["name"] = "put";
    operation["form"] = "single";
    operation["weight"] = 1;
    root["workload"]["operations"].append(operation);
    root["faults"] = Json::Value(Json::arrayValue);

    root["report"]["jsonl_path"] = "/tmp/mock-client-events.jsonl";
    root["report"]["raw_metrics_path"] = "/tmp/mock-client-metrics.txt";
    root["report"]["summary_path"] = "/tmp/mock-client-summary.json";
    return root;
}

class TemporaryConfigFile {
   public:
    explicit TemporaryConfigFile(const Json::Value& value) {
        static std::atomic<uint64_t> sequence{0};
        const auto unique = std::chrono::steady_clock::now()
                                .time_since_epoch()
                                .count() +
                            sequence.fetch_add(1);
        path_ = std::filesystem::temp_directory_path() /
                ("mock-client-config-" + std::to_string(unique) + ".json");
        Json::StreamWriterBuilder builder;
        builder["commentStyle"] = "None";
        builder["indentation"] = "";
        std::ofstream output(path_);
        output << Json::writeString(builder, value);
    }

    ~TemporaryConfigFile() { std::filesystem::remove(path_); }

    const std::filesystem::path& path() const { return path_; }

   private:
    std::filesystem::path path_;
};

tl::expected<MockClientConfig, ConfigError> Load(const Json::Value& value) {
    TemporaryConfigFile file(value);
    return MockClientConfig::LoadFile(file.path().string());
}

}  // namespace

TEST(MockClientConfigTest, LoadsVersionOnePutOnlyConfig) {
    auto result = Load(ValidConfig());

    ASSERT_TRUE(result.has_value()) << result.error().message;
    EXPECT_EQ(result->schema_version, 1);
    ASSERT_EQ(result->master_endpoints.size(), 1);
    EXPECT_EQ(result->master_endpoints.front(), "127.0.0.1:50051");
    ASSERT_EQ(result->clients.size(), 1);
    EXPECT_EQ(result->clients.front().host_id, "madsys-01");
    ASSERT_EQ(result->clients.front().segments.size(), 1);
    EXPECT_EQ(result->clients.front().segments.front().name, "segment-a");
    EXPECT_EQ(result->workload_profile, "low");
    ASSERT_EQ(result->operations.size(), 1);
    EXPECT_EQ(result->operations.front().operation, LogicalOperation::Put);
}

TEST(MockClientConfigTest, RejectsUnsupportedSchemaVersion) {
    Json::Value config = ValidConfig();
    config["schema_version"] = 2;

    EXPECT_FALSE(Load(config).has_value());
}

TEST(MockClientConfigTest, RequiresMasterEndpoint) {
    Json::Value config = ValidConfig();
    config["master"].removeMember("endpoints");
    config["master"].removeMember("endpoint");

    EXPECT_FALSE(Load(config).has_value());
}

TEST(MockClientConfigTest, RejectsDuplicateClientUuid) {
    Json::Value config = ValidConfig();
    config["clients"].append(config["clients"][0]);

    EXPECT_FALSE(Load(config).has_value());
}

TEST(MockClientConfigTest, RejectsDuplicateSegmentName) {
    Json::Value config = ValidConfig();
    Json::Value second_segment = config["clients"][0]["segments"][0];
    second_segment["id"] = "00000000-0000-4000-8000-000000000102";
    config["clients"][0]["segments"].append(second_segment);

    EXPECT_FALSE(Load(config).has_value());
}

TEST(MockClientConfigTest, CommandLineOverrideReplacesJsonSelection) {
    TemporaryConfigFile file(ValidConfig());
    auto result = MockClientConfig::LoadFile(
        file.path().string(), std::string_view("remove:single=3"));

    ASSERT_TRUE(result.has_value()) << result.error().message;
    ASSERT_EQ(result->operations.size(), 1);
    EXPECT_EQ(result->operations.front().operation, LogicalOperation::Remove);
    EXPECT_EQ(result->operations.front().weight, 3);
}

TEST(MockClientConfigTest, RejectsEmptyJsonOperationSelection) {
    Json::Value config = ValidConfig();
    config["workload"]["operations"] = Json::Value(Json::arrayValue);

    EXPECT_FALSE(Load(config).has_value());
}

}  // namespace mock_client
