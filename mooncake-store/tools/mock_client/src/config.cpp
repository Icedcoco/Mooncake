#include "mock_client/config.h"

#include <fstream>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace mock_client {
namespace {

[[noreturn]] void Fail(const std::string& message) {
    throw std::invalid_argument(message);
}

Json::Value RequireObject(const Json::Value& parent, const std::string& key,
                          const std::string& context) {
    const Json::Value& value = parent[key];
    if (!value.isObject()) Fail(context + " must be an object");
    return value;
}

Json::Value RequireMember(const Json::Value& parent, const std::string& key,
                          const std::string& context) {
    if (!parent.isMember(key)) Fail(context + " is required");
    return parent[key];
}

std::string ReadString(const Json::Value& parent, const std::string& key,
                       const std::string& context,
                       std::string default_value = {}) {
    if (!parent.isMember(key)) return default_value;
    const Json::Value& value = parent[key];
    if (!value.isString()) Fail(context + " must be a string");
    std::string result = value.asString();
    if (result.empty()) Fail(context + " cannot be empty");
    return result;
}

uint64_t ReadUnsigned(const Json::Value& parent, const std::string& key,
                      const std::string& context, uint64_t default_value,
                      bool require_positive = false) {
    if (!parent.isMember(key)) return default_value;
    const Json::Value& value = parent[key];
    Json::UInt64 unsigned_value = 0;
    if (value.type() == Json::intValue) {
        const Json::Int64 signed_value = value.asInt64();
        if (signed_value < 0 || (require_positive && signed_value == 0)) {
            Fail(context + (require_positive ? " must be positive"
                                             : " cannot be negative"));
        }
        unsigned_value = static_cast<Json::UInt64>(signed_value);
    } else if (value.type() == Json::uintValue) {
        unsigned_value = value.asUInt64();
        if (require_positive && unsigned_value == 0) {
            Fail(context + " must be positive");
        }
    } else {
        Fail(context + " must be an integer");
    }
    return static_cast<uint64_t>(unsigned_value);
}

uint32_t ReadUInt32(const Json::Value& parent, const std::string& key,
                    const std::string& context, uint32_t default_value,
                    bool require_positive = false) {
    const uint64_t value = ReadUnsigned(parent, key, context, default_value,
                                        require_positive);
    if (value > std::numeric_limits<uint32_t>::max()) {
        Fail(context + " exceeds UINT32_MAX");
    }
    return static_cast<uint32_t>(value);
}

std::vector<std::string> ReadStringList(const Json::Value& parent,
                                        const std::string& key,
                                        const std::string& context,
                                        bool required) {
    if (!parent.isMember(key)) {
        if (required) Fail(context + " is required");
        return {};
    }
    const Json::Value& value = parent[key];
    std::vector<std::string> strings;
    if (value.isString()) {
        const std::string item = value.asString();
        if (!item.empty()) strings.push_back(item);
    } else if (value.isArray()) {
        for (Json::ArrayIndex index = 0; index < value.size(); ++index) {
            if (!value[index].isString() || value[index].asString().empty()) {
                Fail(context + " entries must be non-empty strings");
            }
            strings.push_back(value[index].asString());
        }
    } else {
        Fail(context + " must be a string or an array of strings");
    }
    if (strings.empty() && required) Fail(context + " cannot be empty");
    return strings;
}

BatchSizeRange ReadBatchRange(const Json::Value& value,
                              const std::string& context,
                              BatchSizeRange default_value) {
    if (!value.isMember("batch_size")) return default_value;
    const Json::Value& range = value["batch_size"];
    if (!range.isObject() || !range.isMember("min") ||
        !range.isMember("max")) {
        Fail(context + ".batch_size requires min and max");
    }
    const uint32_t min = ReadUInt32(range, "min", context + ".batch_size.min",
                                    0, true);
    const uint32_t max = ReadUInt32(range, "max", context + ".batch_size.max",
                                    0, true);
    if (min > max) Fail(context + ".batch_size.min cannot exceed max");
    return {min, max};
}

void ParseClients(const Json::Value& root, MockClientConfig* config) {
    const Json::Value clients = RequireMember(root, "clients", "clients");
    if (!clients.isArray() || clients.empty()) {
        Fail("clients must be a non-empty array");
    }

    std::set<std::string> client_uuids;
    std::set<std::string> segment_names;
    std::set<std::string> segment_ids;
    for (Json::ArrayIndex client_index = 0; client_index < clients.size();
         ++client_index) {
        const Json::Value& source = clients[client_index];
        const std::string context = "clients[" + std::to_string(client_index) + "]";
        if (!source.isObject()) Fail(context + " must be an object");

        MockClientDefinition client;
        client.uuid = ReadString(source, "uuid", context + ".uuid");
        client.host_id = ReadString(source, "host_id", context + ".host_id");
        client.seed_offset = ReadUnsigned(source, "seed_offset",
                                          context + ".seed_offset", 0);
        if (!client_uuids.insert(client.uuid).second) {
            Fail("duplicate client UUID: " + client.uuid);
        }

        const Json::Value segments =
            RequireMember(source, "segments", context + ".segments");
        if (!segments.isArray() || segments.empty()) {
            Fail(context + ".segments must be a non-empty array");
        }
        for (Json::ArrayIndex segment_index = 0; segment_index < segments.size();
             ++segment_index) {
            const Json::Value& segment_source = segments[segment_index];
            const std::string segment_context =
                context + ".segments[" + std::to_string(segment_index) + "]";
            if (!segment_source.isObject()) {
                Fail(segment_context + " must be an object");
            }
            MockSegmentConfig segment;
            segment.id = ReadString(segment_source, "id", segment_context + ".id");
            segment.name =
                ReadString(segment_source, "name", segment_context + ".name");
            segment.capacity_bytes = ReadUnsigned(
                segment_source, "capacity_bytes", segment_context + ".capacity_bytes",
                0, true);
            segment.protocol = ReadString(segment_source, "protocol",
                                          segment_context + ".protocol");
            segment.transport_endpoint = ReadString(
                segment_source, "transport_endpoint",
                segment_context + ".transport_endpoint");
            segment.base_address = ReadUnsigned(
                segment_source, "base_address", segment_context + ".base_address",
                0, true);
            segment.host_id = ReadString(segment_source, "host_id",
                                         segment_context + ".host_id", client.host_id);
            if (!segment_names.insert(segment.name).second) {
                Fail("duplicate segment name: " + segment.name);
            }
            if (!segment_ids.insert(segment.id).second) {
                Fail("duplicate segment ID: " + segment.id);
            }
            client.segments.push_back(std::move(segment));
        }
        config->clients.push_back(std::move(client));
    }
}

}  // namespace

tl::expected<MockClientConfig, ConfigError> MockClientConfig::LoadFile(
    const std::string& path,
    std::optional<std::string_view> operation_override) {
    std::ifstream input(path);
    if (!input) {
        return tl::make_unexpected(ConfigError{"cannot open config file: " + path});
    }

    Json::Value root;
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    std::string parse_errors;
    if (!Json::parseFromStream(builder, input, &root, &parse_errors)) {
        return tl::make_unexpected(
            ConfigError{"cannot parse config JSON: " + parse_errors});
    }
    if (!root.isObject()) {
        return tl::make_unexpected(ConfigError{"config root must be an object"});
    }

    try {
        MockClientConfig config;
        const uint32_t schema_version =
            ReadUInt32(root, "schema_version", "schema_version", 0, true);
        if (schema_version != 1) {
            Fail("unsupported schema_version: " + std::to_string(schema_version));
        }
        config.schema_version = schema_version;

        const Json::Value run = RequireObject(root, "run", "run");
        config.run_id = ReadString(run, "run_id", "run.run_id");
        config.seed = ReadUnsigned(run, "seed", "run.seed", config.seed);
        config.namespace_name = ReadString(run, "namespace", "run.namespace",
                                           config.namespace_name);
        config.duration_seconds = ReadUnsigned(
            run, "duration_seconds", "run.duration_seconds",
            config.duration_seconds, true);

        const Json::Value master = RequireObject(root, "master", "master");
        config.master_endpoints =
            ReadStringList(master, "endpoints", "master.endpoints", true);
        config.metrics_endpoints =
            ReadStringList(master, "metrics_endpoints", "master.metrics_endpoints",
                           false);
        config.request_timeout_ms = ReadUInt32(
            master, "request_timeout_ms", "master.request_timeout_ms",
            config.request_timeout_ms, true);
        config.scrape_interval_ms = ReadUInt32(
            master, "scrape_interval_ms", "master.scrape_interval_ms",
            config.scrape_interval_ms, true);

        ParseClients(root, &config);

        const Json::Value workload =
            RequireObject(root, "workload", "workload");
        config.workload_settings = workload;
        config.workload_profile =
            ReadString(workload, "profile", "workload.profile");
        if (config.workload_profile != "low" &&
            config.workload_profile != "medium" &&
            config.workload_profile != "full" &&
            config.workload_profile != "adaptive") {
            Fail("workload.profile must be low, medium, full, or adaptive");
        }
        config.target_logical_ops_per_second = ReadUnsigned(
            workload, "target_logical_ops_per_second",
            "workload.target_logical_ops_per_second", 0);
        config.concurrency = ReadUInt32(workload, "concurrency",
                                        "workload.concurrency", 1, true);
        config.batch_size = ReadBatchRange(workload, "workload", {1, 1});
        auto operations = ParseOperationSelection(
            RequireMember(workload, "operations", "workload.operations"));
        if (!operations) return tl::make_unexpected(operations.error());
        config.operations = std::move(*operations);
        if (operation_override.has_value()) {
            auto override_operations = ParseOperationOverride(*operation_override);
            if (!override_operations) {
                return tl::make_unexpected(override_operations.error());
            }
            for (auto& operation : *override_operations) {
                if (operation.form == OperationForm::Batch) {
                    operation.batch_size = config.batch_size;
                }
            }
            config.operations = std::move(*override_operations);
        }

        if (root.isMember("faults")) {
            if (!root["faults"].isArray()) {
                Fail("faults must be an array");
            }
            config.faults = root["faults"];
        }

        const Json::Value report = RequireObject(root, "report", "report");
        config.report_settings = report;
        config.jsonl_path = ReadString(report, "jsonl_path", "report.jsonl_path");
        config.raw_metrics_path =
            ReadString(report, "raw_metrics_path", "report.raw_metrics_path");
        config.summary_path =
            ReadString(report, "summary_path", "report.summary_path", {});
        return config;
    } catch (const std::exception& error) {
        return tl::make_unexpected(ConfigError{error.what()});
    }
}

}  // namespace mock_client
