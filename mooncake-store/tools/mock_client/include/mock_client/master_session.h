#pragma once
#include "mock_client/master_api.h"
#include "mock_client/config.h"
namespace mock_client {
struct SyntheticSegmentConfig {
 static std::optional<std::vector<mooncake::Segment>> Build(const MockClientDefinition& d) {
  std::vector<mooncake::Segment> out; for (const auto& s:d.segments) { mooncake::UUID id; if(!mooncake::StringToUuid(s.id,id)||!s.base_address||!s.capacity_bytes) return std::nullopt; mooncake::Segment x; x.id=id;x.name=s.name;x.base=s.base_address;x.size=s.capacity_bytes;x.protocol=s.protocol;x.te_endpoint=s.transport_endpoint;x.host_id=d.host_id; for(const auto& y:out) if(y.name==x.name || y.id==x.id || (x.base<y.base+y.size && y.base<x.base+x.size)) return std::nullopt; out.push_back(x);} return out;
 }
};
class MasterSession { MasterApi& api_; std::vector<mooncake::Segment> segments_; std::string endpoint_;
 public: MasterSession(MasterApi& a,std::string endpoint,std::vector<mooncake::Segment> s):api_(a),segments_(std::move(s)),endpoint_(std::move(endpoint)){} MasterSession(MasterApi&a,std::vector<std::string> e,std::vector<mooncake::Segment>s):MasterSession(a,e.empty()?std::string{}:e.front(),std::move(s)){}
 bool ConnectAndMount(){return api_.Connect(endpoint_)==mooncake::ErrorCode::OK && api_.MountSegment(segments_.front()).has_value();}
 bool HandlePingStatus(mooncake::ClientStatus s){return s!=mooncake::ClientStatus::NEED_REMOUNT || api_.ReMountSegment(segments_).has_value();}
 PingResult PingOnce(){return api_.Ping();} bool ReconnectAndRemount(){return ConnectAndMount()&&api_.ReMountSegment(segments_).has_value();} bool Unmount(){bool ok=true;for(auto&s:segments_)ok=api_.UnmountSegment(s.id).has_value()&&ok;return ok;} void Disconnect(){}
}; }
