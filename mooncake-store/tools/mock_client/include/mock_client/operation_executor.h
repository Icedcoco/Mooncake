#pragma once
#include "mock_client/master_api.h"
#include "mock_client/config.h"
#include "mock_client/shadow_model.h"
namespace mock_client {
struct OperationRequest { OperationSelection selection; std::vector<ObjectIdentity> objects; std::string purpose; };
struct ItemResult { std::string key; mooncake::ErrorCode error{mooncake::ErrorCode::OK}; };
struct OperationResult { std::string request_id; uint64_t logical_ops=0,batch_items=0; std::vector<ItemResult> items; bool transport_unknown=false; };
class OperationExecutor { MasterApi& api_; MockClientConfig config_; ShadowModel& shadow_;
 public: OperationExecutor(MasterApi&a,MockClientConfig c,ShadowModel&s):api_(a),config_(std::move(c)),shadow_(s){} ShadowModel& shadow_model(){return shadow_;}
 OperationResult Execute(const OperationRequest&r){OperationResult out;out.logical_ops=r.objects.size(); if(r.objects.empty())return out; mooncake::ReplicateConfig cfg; cfg.replica_num=1; std::vector<size_t> sizes;std::vector<std::string> keys;for(auto&o:r.objects){keys.push_back(o.key);sizes.push_back(o.logical_size);} if(r.selection.operation==LogicalOperation::Put||r.selection.operation==LogicalOperation::Upsert){auto x=r.selection.operation==LogicalOperation::Put?api_.PutStart(keys[0],sizes,cfg):api_.UpsertStart(keys[0],sizes,cfg);if(!x){out.transport_unknown=x.error()==mooncake::ErrorCode::RPC_TIMEOUT||x.error()==mooncake::ErrorCode::RPC_FAIL; if(out.transport_unknown)shadow_.MarkUnknown(keys[0]);return out;} mooncake::ObjectMeta m{keys[0],r.objects[0].checksum}; auto e=r.selection.operation==LogicalOperation::Put?api_.PutEnd(m,mooncake::ReplicaType::MEMORY):api_.UpsertEnd(m,mooncake::ReplicaType::MEMORY);if(!e){out.transport_unknown=true;shadow_.MarkUnknown(keys[0]);return out;} } return out; }
}; }
