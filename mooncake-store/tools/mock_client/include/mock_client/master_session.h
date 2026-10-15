#pragma once
#include "mock_client/master_api.h"
#include "mock_client/config.h"
namespace mock_client {
// Accept native decimal pair IDs and canonical UUID strings without truncation.
bool ParseMockUuid(std::string_view text, mooncake::UUID& id);
struct SyntheticSegmentConfig {
    static std::optional<std::vector<mooncake::Segment>> Build(
        const MockClientDefinition& definition);
};
class MasterSession {
   public:
    MasterSession(MasterApi& api, std::string endpoint,
                  std::vector<mooncake::Segment> segments);
    MasterSession(MasterApi& api, std::vector<std::string> endpoints,
                  std::vector<mooncake::Segment> segments);
    bool ConnectAndMount();
    PingResult PingOnce();
    bool HandlePingStatus(mooncake::ClientStatus status);
    bool ReconnectAndRemount();
    bool Unmount();
    void Disconnect();
    mooncake::ErrorCode last_error() const { return last_error_; }
    bool connected() const { return connected_; }
    const std::vector<mooncake::Segment>& segments() const { return segments_; }

   private:
    bool ConnectHealthy();
    MasterApi& api_;
    std::vector<std::string> endpoints_;
    std::vector<mooncake::Segment> segments_, mounted_;
    size_t next_endpoint_ = 0;
    bool connected_ = false;
    mooncake::ErrorCode last_error_ = mooncake::ErrorCode::OK;
};
}  // namespace mock_client
