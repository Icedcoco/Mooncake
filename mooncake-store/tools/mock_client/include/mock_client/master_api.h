#pragma once
#include <functional>
#include <memory>
#include "master_client.h"
namespace mock_client {
using VoidResult = tl::expected<void, mooncake::ErrorCode>;
using VoidResults = std::vector<VoidResult>;
using StartResult = tl::expected<std::vector<mooncake::Replica::Descriptor>, mooncake::ErrorCode>;
using StartResults = std::vector<StartResult>;
using GetResult = tl::expected<mooncake::GetReplicaListResponse, mooncake::ErrorCode>;
using GetResults = std::vector<GetResult>;
using PingResult = tl::expected<mooncake::PingResponse, mooncake::ErrorCode>;
struct RpcEvent { std::string method; bool before; };
using RpcHook = std::function<void(const RpcEvent&)>;
class MasterApi { public: virtual ~MasterApi() = default;
 virtual mooncake::ErrorCode Connect(const std::string&) = 0;
 virtual VoidResult MountSegment(const mooncake::Segment&) = 0;
 virtual VoidResult ReMountSegment(const std::vector<mooncake::Segment>&) = 0;
 virtual VoidResult UnmountSegment(const mooncake::UUID&) = 0;
 virtual PingResult Ping() = 0;
 virtual StartResult PutStart(const std::string&,const std::vector<size_t>&,const mooncake::ReplicateConfig&)=0;
 virtual StartResults BatchPutStart(const std::vector<std::string>&,const std::vector<std::vector<uint64_t>>&,const mooncake::ReplicateConfig&)=0;
 virtual VoidResult PutEnd(const mooncake::ObjectMeta&,mooncake::ReplicaType)=0;
 virtual VoidResults BatchPutEnd(const std::vector<mooncake::ObjectMeta>&,mooncake::ReplicaType)=0;
 virtual VoidResult PutRevoke(const std::string&,mooncake::ReplicaType)=0;
 virtual VoidResults BatchPutRevoke(const std::vector<std::string>&,mooncake::ReplicaType)=0;
 virtual StartResult UpsertStart(const std::string&,const std::vector<size_t>&,const mooncake::ReplicateConfig&)=0;
 virtual StartResults BatchUpsertStart(const std::vector<std::string>&,const std::vector<std::vector<uint64_t>>&,const mooncake::ReplicateConfig&)=0;
 virtual VoidResult UpsertEnd(const mooncake::ObjectMeta&,mooncake::ReplicaType)=0;
 virtual VoidResults BatchUpsertEnd(const std::vector<mooncake::ObjectMeta>&)=0;
 virtual VoidResult UpsertRevoke(const std::string&,mooncake::ReplicaType)=0;
 virtual VoidResults BatchUpsertRevoke(const std::vector<std::string>&)=0;
 virtual GetResult GetReplicaList(const std::string&)=0; virtual GetResults BatchGetReplicaList(const std::vector<std::string>&)=0;
 virtual VoidResult Remove(const std::string&,bool)=0; virtual VoidResults BatchRemove(const std::vector<std::string>&,bool)=0;
};
class MasterClientApi final : public MasterApi {
 public: MasterClientApi(const mooncake::UUID&,std::string="default",RpcHook={}); ~MasterClientApi() override;
#define MC_DECL(ret,name,args) ret name args override;
 MC_DECL(mooncake::ErrorCode,Connect,(const std::string&)); MC_DECL(VoidResult,MountSegment,(const mooncake::Segment&)); MC_DECL(VoidResult,ReMountSegment,(const std::vector<mooncake::Segment>&)); MC_DECL(VoidResult,UnmountSegment,(const mooncake::UUID&)); MC_DECL(PingResult,Ping,());
 MC_DECL(StartResult,PutStart,(const std::string&,const std::vector<size_t>&,const mooncake::ReplicateConfig&)); MC_DECL(StartResults,BatchPutStart,(const std::vector<std::string>&,const std::vector<std::vector<uint64_t>>&,const mooncake::ReplicateConfig&)); MC_DECL(VoidResult,PutEnd,(const mooncake::ObjectMeta&,mooncake::ReplicaType)); MC_DECL(VoidResults,BatchPutEnd,(const std::vector<mooncake::ObjectMeta>&,mooncake::ReplicaType)); MC_DECL(VoidResult,PutRevoke,(const std::string&,mooncake::ReplicaType)); MC_DECL(VoidResults,BatchPutRevoke,(const std::vector<std::string>&,mooncake::ReplicaType));
 MC_DECL(StartResult,UpsertStart,(const std::string&,const std::vector<size_t>&,const mooncake::ReplicateConfig&)); MC_DECL(StartResults,BatchUpsertStart,(const std::vector<std::string>&,const std::vector<std::vector<uint64_t>>&,const mooncake::ReplicateConfig&)); MC_DECL(VoidResult,UpsertEnd,(const mooncake::ObjectMeta&,mooncake::ReplicaType)); MC_DECL(VoidResults,BatchUpsertEnd,(const std::vector<mooncake::ObjectMeta>&)); MC_DECL(VoidResult,UpsertRevoke,(const std::string&,mooncake::ReplicaType)); MC_DECL(VoidResults,BatchUpsertRevoke,(const std::vector<std::string>&)); MC_DECL(GetResult,GetReplicaList,(const std::string&)); MC_DECL(GetResults,BatchGetReplicaList,(const std::vector<std::string>&)); MC_DECL(VoidResult,Remove,(const std::string&,bool)); MC_DECL(VoidResults,BatchRemove,(const std::vector<std::string>&,bool));
#undef MC_DECL
 private: std::unique_ptr<mooncake::MasterClient> client_; RpcHook hook_; template<class F> auto call(const char*,F&&)->decltype(f());
}; }
