#include "mock_client/master_session.h"
#include <limits>
#include <charconv>
#include <set>
namespace mock_client {
bool ParseMockUuid(std::string_view text, mooncake::UUID& id) {
    auto integer = [](std::string_view t, uint64_t& value, int base) {
        const auto parsed =
            std::from_chars(t.data(), t.data() + t.size(), value, base);
        return !t.empty() && parsed.ec == std::errc{} &&
               parsed.ptr == t.data() + t.size();
    };
    mooncake::UUID parsed;
    if (text.size() == 36 && text[8] == '-' && text[13] == '-' &&
        text[18] == '-' && text[23] == '-') {
        std::string digits;
        for (size_t i = 0; i < text.size(); ++i)
            if (i != 8 && i != 13 && i != 18 && i != 23)
                digits.push_back(text[i]);
        if (!integer(std::string_view(digits).substr(0, 16), parsed.first,
                     16) ||
            !integer(std::string_view(digits).substr(16), parsed.second, 16))
            return false;
    } else {
        auto dash = text.find('-');
        if (dash == std::string_view::npos ||
            !integer(text.substr(0, dash), parsed.first, 10) ||
            !integer(text.substr(dash + 1), parsed.second, 10))
            return false;
    }
    if (parsed == mooncake::UUID{0, 0}) return false;
    id = parsed;
    return true;
}
std::optional<std::vector<mooncake::Segment>> SyntheticSegmentConfig::Build(
    const MockClientDefinition& d) {
    if (d.segments.empty() || d.uuid.empty() || d.host_id.empty())
        return std::nullopt;
    std::vector<mooncake::Segment> out;
    for (const auto& s : d.segments) {
        mooncake::Segment x;
        if (!ParseMockUuid(s.id, x.id) || x.id == mooncake::UUID{0, 0} ||
            s.name.empty() || s.protocol.empty() ||
            s.transport_endpoint.empty() || !s.base_address ||
            s.base_address % 4096 || !s.capacity_bytes ||
            s.capacity_bytes % 4096 ||
            s.capacity_bytes >
                std::numeric_limits<uintptr_t>::max() - s.base_address)
            return std::nullopt;
        x.name = s.name;
        x.base = s.base_address;
        x.size = s.capacity_bytes;
        x.protocol = s.protocol;
        x.te_endpoint = s.transport_endpoint;
        x.host_id = s.host_id.empty() ? d.host_id : s.host_id;
        for (const auto& y : out)
            if (y.name == x.name || y.id == x.id ||
                (x.base < y.base + y.size && y.base < x.base + x.size))
                return std::nullopt;
        out.push_back(std::move(x));
    }
    return out;
}
MasterSession::MasterSession(MasterApi& api, std::string endpoint,
                             std::vector<mooncake::Segment> s)
    : MasterSession(api, std::vector<std::string>{std::move(endpoint)},
                    std::move(s)) {}
MasterSession::MasterSession(MasterApi& api, std::vector<std::string> e,
                             std::vector<mooncake::Segment> s)
    : api_(api), endpoints_(std::move(e)), segments_(std::move(s)) {}
bool MasterSession::ConnectHealthy() {
    if (endpoints_.empty() || segments_.empty()) {
        last_error_ = mooncake::ErrorCode::INVALID_PARAMS;
        return false;
    }
    api_.SetRpcContext({"session-connect", "session", "none", "single"});
    for (size_t i = 0; i < endpoints_.size(); ++i) {
        auto index = (next_endpoint_ + i) % endpoints_.size();
        auto result = api_.InvokeRpc("ServiceReady", 1, [&]() -> VoidResult {
            auto error = api_.Connect(endpoints_[index]);
            if (error != mooncake::ErrorCode::OK)
                return tl::make_unexpected(error);
            return {};
        });
        if (result) {
            next_endpoint_ = (index + 1) % endpoints_.size();
            connected_ = true;
            last_error_ = mooncake::ErrorCode::OK;
            return true;
        }
        last_error_ = result.error();
        api_.Disconnect();
    }
    connected_ = false;
    return false;
}
bool MasterSession::ConnectAndMount() {
    // Outstanding acknowledged or ambiguous mounts must be explicitly cleaned
    // up or reconciled before a new mount sequence can replace bookkeeping.
    if (!mounted_.empty()) return false;
    if (!ConnectHealthy()) return false;
    for (const auto& s : segments_) {
        auto result = api_.InvokeRpc("MountSegment", 1,
                                     [&] { return api_.MountSegment(s); });
        if (!result) {
            const auto error = result.error();
            if (IsTransportError(error)) mounted_.push_back(s);
            if (Unmount()) {
                Disconnect();
                last_error_ = error;
            }
            // Failed rollback keeps its error, connection and descriptors so
            // the caller can retry Unmount rather than lose accepted mounts.
            return false;
        }
        mounted_.push_back(s);
    }
    return true;
}
bool MasterSession::HandlePingStatus(mooncake::ClientStatus status) {
    if (status != mooncake::ClientStatus::NEED_REMOUNT) return true;
    auto result = api_.InvokeRpc("ReMountSegment", segments_.size(), [&] {
        return api_.ReMountSegment(segments_);
    });
    if (!result) {
        last_error_ = result.error();
        return false;
    }
    mounted_ = segments_;
    return true;
}
PingResult MasterSession::PingOnce() {
    if (!connected_) return tl::make_unexpected(mooncake::ErrorCode::RPC_FAIL);
    api_.SetRpcContext({"session-ping", "session", "none", "single"});
    auto result = api_.InvokeRpc("Ping", 1, [&] { return api_.Ping(); });
    if (!result)
        last_error_ = result.error();
    else if (!HandlePingStatus(result->client_status))
        return tl::make_unexpected(last_error_);
    return result;
}
bool MasterSession::ReconnectAndRemount() {
    Disconnect();
    if (!ConnectHealthy()) return false;
    api_.SetRpcContext({"session-remount", "session", "none", "single"});
    auto result = api_.InvokeRpc("ReMountSegment", segments_.size(), [&] {
        return api_.ReMountSegment(segments_);
    });
    if (!result) {
        last_error_ = result.error();
        return false;
    }
    mounted_ = segments_;
    return true;
}
bool MasterSession::Unmount() {
    if (!connected_ && !mounted_.empty()) {
        last_error_ = mooncake::ErrorCode::RPC_FAIL;
        return false;
    }
    api_.SetRpcContext({"session-unmount", "session", "none", "single"});
    std::vector<mooncake::Segment> failed;
    for (const auto& s : mounted_) {
        auto result = api_.InvokeRpc("UnmountSegment", 1,
                                     [&] { return api_.UnmountSegment(s.id); });
        if (!result) {
            last_error_ = result.error();
            failed.push_back(s);
        }
    }
    mounted_ = std::move(failed);
    if (mounted_.empty()) last_error_ = mooncake::ErrorCode::OK;
    return mounted_.empty();
}
void MasterSession::Disconnect() {
    api_.Disconnect();
    connected_ = false;
}
}  // namespace mock_client
