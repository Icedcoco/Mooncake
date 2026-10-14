#include "mock_client/operation_selection.h"

#include <charconv>
#include <limits>
#include <string>
#include <system_error>
#include <utility>

namespace mock_client {
namespace {

ConfigError Error(std::string message) { return ConfigError{std::move(message)}; }

std::string_view Trim(std::string_view value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos) return {};
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

bool ReadPositiveUInt32(const Json::Value& value, uint32_t* output) {
    Json::UInt64 unsigned_value = 0;
    if (value.type() == Json::intValue) {
        const Json::Int64 signed_value = value.asInt64();
        if (signed_value <= 0) return false;
        unsigned_value = static_cast<Json::UInt64>(signed_value);
    } else if (value.type() == Json::uintValue) {
        unsigned_value = value.asUInt64();
    } else {
        return false;
    }
    if (unsigned_value == 0 ||
        unsigned_value > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *output = static_cast<uint32_t>(unsigned_value);
    return true;
}

bool ParseWeight(std::string_view text, uint32_t* output) {
    if (text.empty()) return false;
    uint64_t parsed = 0;
    const auto result =
        std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() ||
        parsed == 0 || parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *output = static_cast<uint32_t>(parsed);
    return true;
}

tl::expected<LogicalOperation, ConfigError> ParseOperationName(
    std::string_view name) {
    if (name == "put") return LogicalOperation::Put;
    if (name == "upsert") return LogicalOperation::Upsert;
    if (name == "get") return LogicalOperation::Get;
    if (name == "remove") return LogicalOperation::Remove;
    return tl::make_unexpected(Error("unknown operation name: " +
                                     std::string(name)));
}

tl::expected<OperationForm, ConfigError> ParseForm(std::string_view form) {
    if (form == "single") return OperationForm::Single;
    if (form == "batch") return OperationForm::Batch;
    return tl::make_unexpected(
        Error("unknown operation form: " + std::string(form)));
}

tl::expected<OperationSelection, ConfigError> ParseOverrideEntry(
    std::string_view entry) {
    entry = Trim(entry);
    const auto colon = entry.find(':');
    const auto equals = entry.find('=');
    if (colon == std::string_view::npos || equals == std::string_view::npos ||
        colon == 0 || equals <= colon + 1 || equals + 1 >= entry.size() ||
        entry.find(':', colon + 1) != std::string_view::npos ||
        entry.find('=', equals + 1) != std::string_view::npos) {
        return tl::make_unexpected(Error(
            "operation override entry must use operation:form=weight"));
    }

    auto operation = ParseOperationName(Trim(entry.substr(0, colon)));
    if (!operation) return tl::make_unexpected(operation.error());
    auto form = ParseForm(Trim(entry.substr(colon + 1, equals - colon - 1)));
    if (!form) return tl::make_unexpected(form.error());
    uint32_t weight = 0;
    if (!ParseWeight(Trim(entry.substr(equals + 1)), &weight)) {
        return tl::make_unexpected(Error("operation weight must be a positive uint32"));
    }
    return OperationSelection{*operation, *form, weight, {1, 1}};
}

}  // namespace

std::string ToString(LogicalOperation operation) {
    switch (operation) {
        case LogicalOperation::Put:
            return "put";
        case LogicalOperation::Upsert:
            return "upsert";
        case LogicalOperation::Get:
            return "get";
        case LogicalOperation::Remove:
            return "remove";
    }
    return "unknown";
}

std::string ToString(OperationForm form) {
    switch (form) {
        case OperationForm::Single:
            return "single";
        case OperationForm::Batch:
            return "batch";
    }
    return "unknown";
}

tl::expected<std::vector<OperationSelection>, ConfigError>
ParseOperationSelection(const Json::Value& value) {
    if (!value.isArray() || value.empty()) {
        return tl::make_unexpected(
            Error("workload operations must be a non-empty array"));
    }

    std::vector<OperationSelection> selections;
    selections.reserve(value.size());
    for (Json::ArrayIndex index = 0; index < value.size(); ++index) {
        const Json::Value& entry = value[index];
        if (!entry.isObject()) {
            return tl::make_unexpected(
                Error("workload operation entry must be an object"));
        }
        const Json::Value& name_value = entry["name"];
        const Json::Value& form_value = entry["form"];
        if (!name_value.isString() || !form_value.isString()) {
            return tl::make_unexpected(Error(
                "workload operation entry requires string name and form"));
        }
        auto operation = ParseOperationName(name_value.asString());
        if (!operation) return tl::make_unexpected(operation.error());
        auto form = ParseForm(form_value.asString());
        if (!form) return tl::make_unexpected(form.error());

        uint32_t weight = 1;
        if (entry.isMember("weight") &&
            !ReadPositiveUInt32(entry["weight"], &weight)) {
            return tl::make_unexpected(
                Error("operation weight must be a positive uint32"));
        }

        BatchSizeRange batch_size{1, 1};
        if (*form == OperationForm::Single) {
            if (entry.isMember("batch_size")) {
                return tl::make_unexpected(
                    Error("single operation cannot specify batch_size"));
            }
        } else {
            const Json::Value& batch = entry["batch_size"];
            if (!batch.isObject() || !batch.isMember("min") ||
                !batch.isMember("max")) {
                return tl::make_unexpected(
                    Error("batch operation requires batch_size.min and batch_size.max"));
            }
            if (!ReadPositiveUInt32(batch["min"], &batch_size.min) ||
                !ReadPositiveUInt32(batch["max"], &batch_size.max) ||
                batch_size.min > batch_size.max) {
                return tl::make_unexpected(
                    Error("batch_size must satisfy 0 < min <= max <= UINT32_MAX"));
            }
        }
        selections.push_back(
            OperationSelection{*operation, *form, weight, batch_size});
    }
    return selections;
}

tl::expected<std::vector<OperationSelection>, ConfigError>
ParseOperationOverride(std::string_view value) {
    value = Trim(value);
    if (value.empty()) {
        return tl::make_unexpected(Error("operation selection cannot be empty"));
    }

    std::vector<OperationSelection> selections;
    size_t start = 0;
    while (start <= value.size()) {
        const auto comma = value.find(',', start);
        const auto end = comma == std::string_view::npos ? value.size() : comma;
        const std::string_view entry = value.substr(start, end - start);
        if (Trim(entry).empty()) {
            return tl::make_unexpected(
                Error("operation selection contains an empty entry"));
        }
        auto parsed = ParseOverrideEntry(entry);
        if (!parsed) return tl::make_unexpected(parsed.error());
        selections.push_back(*parsed);
        if (comma == std::string_view::npos) break;
        start = comma + 1;
    }
    return selections;
}

}  // namespace mock_client
