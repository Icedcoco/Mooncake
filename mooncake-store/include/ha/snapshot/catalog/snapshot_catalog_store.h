#pragma once

#include <charconv>
#include <cctype>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

namespace snapshot_catalog_store_detail {

constexpr std::string_view kSnapshotRootBase = "mooncake_master_snapshot";
constexpr std::string_view kSnapshotLatest = "latest.txt";
constexpr std::string_view kSnapshotManifest = "manifest.txt";
constexpr std::string_view kSnapshotDescriptor = "descriptor.txt";

/// Build the per-cluster snapshot root path.
/// When cluster_id is non-empty: "mooncake_master_snapshot/{cluster_id}/"
/// When cluster_id is empty:     "mooncake_master_snapshot/"
inline std::string BuildSnapshotRoot(const std::string& cluster_id) {
    std::string root(kSnapshotRootBase);
    root += '/';
    if (!cluster_id.empty()) {
        root += cluster_id;
        root += '/';
    }
    return root;
}

inline bool IsAsciiDigit(char ch) {
    return std::isdigit(static_cast<unsigned char>(ch)) != 0;
}

inline bool IsValidSnapshotId(std::string_view snapshot_id) {
    if (snapshot_id.size() != 19) {
        return false;
    }

    for (size_t i = 0; i < snapshot_id.size(); ++i) {
        if (i == 8 || i == 15) {
            if (snapshot_id[i] != '_') {
                return false;
            }
            continue;
        }

        if (!IsAsciiDigit(snapshot_id[i])) {
            return false;
        }
    }

    return true;
}

inline std::string TrimAsciiWhitespace(std::string value) {
    constexpr std::string_view kAsciiWhitespace = " \t\n\r\f\v";
    const auto first = value.find_first_not_of(kAsciiWhitespace);
    if (first == std::string::npos) {
        return "";
    }

    const auto last = value.find_last_not_of(kAsciiWhitespace);
    return value.substr(first, last - first + 1);
}

inline std::string BuildSnapshotPrefix(const std::string& snapshot_root,
                                       const SnapshotId& snapshot_id) {
    return snapshot_root + snapshot_id + "/";
}

inline std::string BuildManifestKey(const std::string& snapshot_root,
                                    const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_root, snapshot_id) +
           std::string(kSnapshotManifest);
}

inline std::string BuildDescriptorKey(const std::string& snapshot_root,
                                      const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_root, snapshot_id) +
           std::string(kSnapshotDescriptor);
}

inline std::string BuildLatestKey(const std::string& snapshot_root) {
    return snapshot_root + std::string(kSnapshotLatest);
}

inline SnapshotDescriptor MakeSnapshotDescriptor(
    const std::string& snapshot_root, const SnapshotId& snapshot_id) {
    SnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.manifest_key = BuildManifestKey(snapshot_root, snapshot_id);
    descriptor.object_prefix = BuildSnapshotPrefix(snapshot_root, snapshot_id);
    return descriptor;
}

template <typename Integer>
inline bool ParseDecimal(std::string_view text, Integer& value) {
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc() && result.ptr == end;
}

inline bool ValidateSnapshotDescriptor(const SnapshotDescriptor& descriptor) {
    if (descriptor.schema_version == 0) {
        return descriptor.last_included_batch_id == 0 &&
               descriptor.payload_checksum.empty() &&
               descriptor.previous_snapshot_id.empty();
    }
    return descriptor.schema_version == kSnapshotDescriptorSchemaVersion &&
           (descriptor.last_included_seq == 0) ==
               (descriptor.last_included_batch_id == 0) &&
           !descriptor.payload_checksum.empty() &&
           descriptor.payload_checksum.find('|') == std::string::npos &&
           (descriptor.previous_snapshot_id.empty() ||
            IsValidSnapshotId(descriptor.previous_snapshot_id));
}

inline std::string SerializeSnapshotDescriptor(
    const SnapshotDescriptor& descriptor) {
    std::string payload = std::to_string(descriptor.last_included_seq) + "|" +
                          std::to_string(descriptor.producer_view_version) +
                          "|" + std::to_string(descriptor.created_at_ms);
    if (descriptor.schema_version == 0) {
        return payload;
    }
    return payload + "|" + std::to_string(descriptor.schema_version) + "|" +
           std::to_string(descriptor.last_included_batch_id) + "|" +
           descriptor.payload_checksum + "|" + descriptor.previous_snapshot_id;
}

inline tl::expected<SnapshotDescriptor, ErrorCode>
DeserializeSnapshotDescriptor(const std::string& snapshot_root,
                              const SnapshotId& snapshot_id,
                              std::string_view payload) {
    std::vector<std::string_view> fields;
    for (size_t begin = 0;;) {
        const size_t end = payload.find('|', begin);
        fields.emplace_back(payload.substr(begin, end - begin));
        if (end == std::string_view::npos) {
            break;
        }
        begin = end + 1;
    }
    if (fields.size() != 3 && fields.size() != 7) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }

    SnapshotDescriptor descriptor =
        MakeSnapshotDescriptor(snapshot_root, snapshot_id);
    if (!ParseDecimal(fields[0], descriptor.last_included_seq) ||
        !ParseDecimal(fields[1], descriptor.producer_view_version) ||
        !ParseDecimal(fields[2], descriptor.created_at_ms)) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    if (fields.size() == 7) {
        if (!ParseDecimal(fields[3], descriptor.schema_version) ||
            !ParseDecimal(fields[4], descriptor.last_included_batch_id)) {
            return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
        }
        descriptor.payload_checksum = fields[5];
        descriptor.previous_snapshot_id = fields[6];
    }
    if (!ValidateSnapshotDescriptor(descriptor)) {
        return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
    }
    return descriptor;
}

}  // namespace snapshot_catalog_store_detail

class SnapshotCatalogStore {
   public:
    virtual ~SnapshotCatalogStore() = default;

    virtual ErrorCode Publish(const SnapshotDescriptor& snapshot) = 0;

    virtual tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
    GetLatest() = 0;

    virtual tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) = 0;

    virtual ErrorCode Delete(const SnapshotId& snapshot_id) = 0;

    /// Returns the per-cluster snapshot root path used by this catalog store.
    virtual const std::string& GetSnapshotRoot() const = 0;
};

}  // namespace ha
}  // namespace mooncake
