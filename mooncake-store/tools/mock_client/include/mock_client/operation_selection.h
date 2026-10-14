#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <json/json.h>
#include <ylt/util/tl/expected.hpp>

namespace mock_client {

struct ConfigError {
    std::string message;
};

enum class LogicalOperation { Put, Upsert, Get, Remove };
enum class OperationForm { Single, Batch };

struct BatchSizeRange {
    uint32_t min;
    uint32_t max;
};

struct OperationSelection {
    LogicalOperation operation;
    OperationForm form;
    uint32_t weight;
    BatchSizeRange batch_size;
};

std::string ToString(LogicalOperation operation);
std::string ToString(OperationForm form);

tl::expected<std::vector<OperationSelection>, ConfigError>
ParseOperationSelection(const Json::Value& value);

tl::expected<std::vector<OperationSelection>, ConfigError>
ParseOperationOverride(std::string_view value);

}  // namespace mock_client
