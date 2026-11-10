#include "mock_client/master_api.h"
#include "read_plan.h"
#include "window.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <json/json.h>
using namespace mooncake;
using namespace mock_client;
using Clock = std::chrono::steady_clock;
static std::atomic<bool> stopped{false};
static std::atomic<bool> interrupted{false};
static void Stop(int) {
    interrupted = true;
    stopped = true;
}
static double Wall() {
    return std::chrono::duration<double>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
struct Counters {
    std::atomic<uint64_t> attempted{0}, success{0}, start_errors{0},
        end_errors{0}, verified{0}, missing{0}, mismatch{0}, unknown{0},
        batches{0}, start_us{0}, end_us{0}, get_us{0}, backoff_ms{0},
        remove_attempted{0}, remove_success{0}, remove_errors{0}, remove_us{0},
        live_keys{0}, get_attempted{0}, get_success{0}, get_errors{0},
        get_batches{0}, get_after_failed_put{0};
};
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "需要配置文件路径\n";
        return 2;
    }
    Json::Value c;
    std::ifstream in(argv[1]);
    in >> c;
    const auto endpoint = c["endpoint"].asString(),
               prefix = c["run_id"].asString();
    const unsigned workers = c["workers"].asUInt(),
                   batch = c.get("batch_size", 128).asUInt(),
                   seconds = c["duration_seconds"].asUInt(),
                   every = c.get("verify_every_batches", 16).asUInt();
    const uint64_t capacity = c["capacity_bytes"].asUInt64(),
                   size = c.get("object_bytes", 1024).asUInt64();
    if (!workers || workers > 64 || !batch || batch > 128 || !seconds ||
        !every || capacity % 4096 || !size)
        return 2;
    const auto workload = c.get("workload", "put_evict").asString();
    if (workload != "put_evict" && workload != "put_remove" &&
        workload != "put_evict_get")
        return 2;
    const bool mixed = workload == "put_remove";
    const unsigned get_batches_per_put =
        c.get("get_batches_per_put", workload == "put_evict_get" ? 2 : 0)
            .asUInt();
    if ((workload == "put_evict_get" &&
         (!get_batches_per_put || get_batches_per_put > 64)) ||
        (workload != "put_evict_get" && get_batches_per_put))
        return 2;
    const uint64_t live_target = c.get("live_target_keys", 65536).asUInt64();
    if (mixed && (live_target < workers * batch ||
                  (live_target + workers * batch) * size > capacity / 2))
        return 2;
    const UUID id{c["seed"].asUInt64(), 1};
    Segment segment;
    segment.id = {id.first, 2};
    segment.name = prefix;
    segment.base = 0x10000000000ULL;
    segment.size = capacity;
    segment.protocol = "tcp";
    segment.te_endpoint = "10.1.101.3:59999";
    segment.host_id = "madsys-03";
    MasterClientApi owner(id);
    if (owner.Connect(endpoint) != ErrorCode::OK ||
        !owner.MountSegment(segment))
        return 3;
    auto first_ping = owner.Ping();
    if (!first_ping ||
        (first_ping->client_status == ClientStatus::NEED_REMOUNT &&
         !owner.ReMountSegment({segment})))
        return 4;
    std::signal(SIGTERM, Stop);
    std::signal(SIGINT, Stop);
    Counters n;
    std::mutex errors_mutex;
    Json::Value errors(Json::objectValue);
    auto error = [&](const char* phase, ErrorCode e) {
        std::lock_guard lock(errors_mutex);
        auto key =
            std::string(phase) + ":" + std::to_string(static_cast<int>(e));
        errors[key] =
            Json::UInt64(errors.get(key, Json::UInt64(0)).asUInt64() + 1);
        if (IsTransportError(e)) ++n.unknown;
    };
    std::atomic<unsigned> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    auto valid = [&](const Replica::Descriptor& d, bool complete) {
        if (!d.is_memory_replica() ||
            (complete && d.status != ReplicaStatus::COMPLETE))
            return false;
        const auto& b = d.get_memory_descriptor().buffer_descriptor;
        return b.size_ == size && b.buffer_address_ >= segment.base &&
               b.buffer_address_ - segment.base <= capacity &&
               size <= capacity - (b.buffer_address_ - segment.base) &&
               b.protocol_ == segment.protocol &&
               b.transport_endpoint_ == segment.te_endpoint;
    };
    for (unsigned w = 0; w < workers; ++w)
        threads.emplace_back([&, w] {
            MasterClientApi api(id);
            if (api.Connect(endpoint) != ErrorCode::OK) {
                stopped = true;
                ++ready;
                return;
            }
            ++ready;
            while (!go && !stopped)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            ReplicateConfig replication;
            replication.replica_num = 1;
            uint64_t seq = 0, nb = 0;
            LiveWindow live(live_target / workers);
            while (!stopped) {
                if (mixed) {
                    auto remove = live.Due(batch);
                    if (!remove.empty()) {
                        n.remove_attempted += remove.size();
                        auto tick = Clock::now();
                        auto results = api.BatchRemove(remove, true);
                        n.remove_us +=
                            std::chrono::duration_cast<
                                std::chrono::microseconds>(Clock::now() - tick)
                                .count();
                        if (results.size() != remove.size()) {
                            ++n.unknown;
                            stopped = true;
                            break;
                        }
                        std::vector<bool> ok;
                        bool retry = false;
                        for (const auto& result : results) {
                            ok.push_back(bool(result));
                            if (result) {
                                ++n.remove_success;
                                --n.live_keys;
                            } else {
                                ++n.remove_errors;
                                error("remove", result.error());
                                retry = true;
                                if (IsTransportError(result.error()) ||
                                    result.error() ==
                                        ErrorCode::OBJECT_NOT_FOUND)
                                    stopped = true;
                            }
                        }
                        live.Complete(ok);
                        if (retry) {
                            n.backoff_ms += 10;
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(10));
                            continue;
                        }
                    }
                }
                std::vector<std::string> keys;
                std::vector<std::vector<uint64_t>> sizes(batch, {size});
                for (unsigned i = 0; i < batch; ++i)
                    keys.push_back(prefix + "-" + std::to_string(w) + "-" +
                                   std::to_string(seq++));
                n.attempted += batch;
                ++n.batches;
                auto tick = Clock::now();
                auto starts = api.BatchPutStart(keys, sizes, replication);
                n.start_us +=
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        Clock::now() - tick)
                        .count();
                if (starts.size() != batch) {
                    ++n.unknown;
                    stopped = true;
                    break;
                }
                std::vector<ObjectMeta> metas;
                std::vector<size_t> indices;
                std::vector<bool> put_success(batch, false);
                bool backpressure = false;
                for (size_t i = 0; i < starts.size(); ++i) {
                    if (!starts[i]) {
                        ++n.start_errors;
                        error("start", starts[i].error());
                        if (IsTransportError(starts[i].error())) stopped = true;
                        backpressure = true;
                        continue;
                    }
                    if (starts[i]->size() != 1 ||
                        !valid(starts[i]->front(), false)) {
                        ++n.mismatch;
                        stopped = true;
                        break;
                    }
                    metas.push_back({keys[i], uint64_t(0x12340000 + w)});
                    indices.push_back(i);
                }
                if (n.unknown || n.mismatch) break;
                if (!metas.empty()) {
                    tick = Clock::now();
                    auto ends = api.BatchPutEnd(metas);
                    n.end_us +=
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            Clock::now() - tick)
                            .count();
                    if (ends.size() != metas.size()) {
                        ++n.unknown;
                        stopped = true;
                        break;
                    }
                    for (size_t j = 0; j < ends.size(); ++j)
                        if (ends[j]) {
                            ++n.success;
                            put_success[indices[j]] = true;
                            if (mixed) {
                                live.Add(metas[j].key);
                                ++n.live_keys;
                            }
                        } else {
                            ++n.end_errors;
                            error("end", ends[j].error());
                            if (IsTransportError(ends[j].error()))
                                stopped = true;
                            backpressure = true;
                        }
                }
                if (n.unknown || n.mismatch) break;
                const auto plan =
                    PlanReads(get_batches_per_put, nb, every, put_success);
                std::vector<std::string> check;
                for (auto i : plan.indices) check.push_back(keys[i]);
                if (!check.empty()) {
                    for (unsigned round = 0; round < plan.rounds; ++round) {
                        n.get_attempted += check.size();
                        ++n.get_batches;
                        for (auto i : plan.indices)
                            if (!put_success[i]) ++n.get_after_failed_put;
                        tick = Clock::now();
                        auto gets = api.BatchGetReplicaList(check);
                        n.get_us +=
                            std::chrono::duration_cast<
                                std::chrono::microseconds>(Clock::now() - tick)
                                .count();
                        if (gets.size() != check.size()) {
                            ++n.unknown;
                            stopped = true;
                            break;
                        }
                        for (size_t j = 0; j < gets.size(); ++j) {
                            if (!gets[j]) {
                                if (gets[j].error() ==
                                    ErrorCode::OBJECT_NOT_FOUND)
                                    ++n.missing;
                                else {
                                    ++n.get_errors;
                                    error("get", gets[j].error());
                                    if (IsTransportError(gets[j].error()) ||
                                        put_success[plan.indices[j]])
                                        stopped = true;
                                }
                                continue;
                            }
                            ++n.get_success;
                            auto& g = *gets[j];
                            const auto& original = starts[plan.indices[j]];
                            if (!original || original->size() != 1) {
                                ++n.mismatch;
                                stopped = true;
                                continue;
                            }
                            auto& old = original->front();
                            bool match =
                                g.object_checksum ==
                                    std::optional<uint64_t>(0x12340000 + w) &&
                                g.replicas.size() == 1;
                            if (match) {
                                auto& d = g.replicas.front();
                                match =
                                    valid(d, true) && d.id == old.id &&
                                    d.get_memory_descriptor()
                                            .buffer_descriptor
                                            .buffer_address_ ==
                                        old.get_memory_descriptor()
                                            .buffer_descriptor.buffer_address_;
                            }
                            if (match)
                                ++n.verified;
                            else {
                                ++n.mismatch;
                                stopped = true;
                            }
                        }
                        if (n.unknown || n.mismatch) break;
                    }
                }
                ++nb;
                if (backpressure) {
                    n.backoff_ms += 10;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        });
    while (ready < workers && !stopped)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const auto begin = Clock::now();
    const auto deadline = begin + std::chrono::seconds(seconds);
    go = true;
    std::ofstream out(c["output"].asString());
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "";
    auto report = [&](const char* phase) {
        Json::Value v;
        v["phase"] = phase;
        v["time"] = Wall();
        v["elapsed"] =
            std::chrono::duration<double>(Clock::now() - begin).count();
#define COUNT(x) v[#x] = Json::UInt64(n.x.load())
        COUNT(attempted);
        COUNT(success);
        COUNT(start_errors);
        COUNT(end_errors);
        COUNT(verified);
        COUNT(missing);
        COUNT(mismatch);
        COUNT(unknown);
        COUNT(batches);
        COUNT(start_us);
        COUNT(end_us);
        COUNT(get_us);
        COUNT(get_attempted);
        COUNT(get_success);
        COUNT(get_errors);
        COUNT(get_batches);
        COUNT(get_after_failed_put);
        COUNT(backoff_ms);
        COUNT(remove_attempted);
        COUNT(remove_success);
        COUNT(remove_errors);
        COUNT(remove_us);
        COUNT(live_keys);
#undef COUNT
        {
            std::lock_guard lock(errors_mutex);
            v["errors"] = errors;
        }
        out << Json::writeString(writer, v) << '\n';
        out.flush();
    };
    report("started");
    while (!stopped && Clock::now() < deadline) {
        auto ping = owner.Ping();
        if (!ping || ping->client_status == ClientStatus::NEED_REMOUNT) {
            stopped = true;
            ++n.unknown;
        }
        report("workload");
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    bool early = Clock::now() < deadline;
    stopped = true;
    for (auto& t : threads) t.join();
    report("done");
    for (unsigned i = 0;
         i < c.get("hold_seconds", 120).asUInt() && !interrupted; ++i) {
        owner.Ping();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    // 保留 segment，采集器在保存静止计数后关闭本轮专用 master。
    return early || n.mismatch || n.unknown ? 5 : 0;
}
