#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <ylt/easylog.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "client_wrapper.h"
#include "e2e_utils.h"
#include "types.h"

USE_engine_flags;
DEFINE_string(master_server_entry, "etcd://0.0.0.0:2379",
              "Master server entry");
DEFINE_string(mode, "", "provider, seed, verify, or pressure");
DEFINE_int32(port, 19001, "Client transfer-engine port");
DEFINE_string(key_prefix, "ha-e2e", "Key prefix");
DEFINE_uint64(start_index, 0, "First object index");
DEFINE_uint64(count, 1000, "Number of objects for seed");
DEFINE_uint64(payload_size, 4096, "Payload bytes per object");
DEFINE_string(payload_sizes, "", "Comma-separated payload sizes");
DEFINE_string(
    payload_version, "",
    "Version salt for key-specific payloads; empty preserves legacy data");
DEFINE_uint64(operation_interval_ms, 0,
              "Minimum delay before each data operation");
DEFINE_uint64(bytes_per_second, 0,
              "Payload pacing limit; zero disables byte pacing");
DEFINE_string(manifest, "", "Acknowledged-write manifest path");
DEFINE_uint64(duration_sec, 45, "Pressure duration");
DEFINE_uint64(sleep_ms, 25, "Delay between pressure operations");
DEFINE_uint64(connect_timeout_sec, 30, "Client creation timeout");
DEFINE_uint64(segment_size, 134217728, "Provider memory-segment bytes");

namespace mooncake::testing {
namespace {

std::vector<uint64_t> payload_sizes;

std::string Key(uint64_t index) {
    return FLAGS_key_prefix + "-" + std::to_string(index);
}

std::string Payload(uint64_t index) {
    const uint64_t size = payload_sizes.empty()
                              ? FLAGS_payload_size
                              : payload_sizes[index % payload_sizes.size()];
    std::string value(size, '\0');
    if (!FLAGS_payload_version.empty()) {
        uint64_t state = 14695981039346656037ULL;
        const std::string identity = Key(index) + '\0' + FLAGS_payload_version;
        for (unsigned char byte : identity) {
            state = (state ^ byte) * 1099511628211ULL;
        }
        // SplitMix64, explicitly serialized little-endian for reproducibility.
        for (uint64_t offset = 0; offset < size;) {
            state += 0x9e3779b97f4a7c15ULL;
            uint64_t word = state;
            word = (word ^ (word >> 30)) * 0xbf58476d1ce4e5b9ULL;
            word = (word ^ (word >> 27)) * 0x94d049bb133111ebULL;
            word ^= word >> 31;
            for (int byte = 0; byte < 8 && offset < size; ++byte, ++offset) {
                value[offset] = static_cast<char>(word & 0xff);
                word >>= 8;
            }
        }
        return value;
    }
    for (uint64_t offset = 0; offset < size; ++offset) {
        value[offset] = static_cast<char>(
            (index * 1315423911ULL + offset * 2654435761ULL) & 0xff);
    }
    return value;
}

void Pace(uint64_t bytes) {
    const double seconds =
        std::max(FLAGS_operation_interval_ms / 1000.0,
                 FLAGS_bytes_per_second
                     ? static_cast<double>(bytes) / FLAGS_bytes_per_second
                     : 0.0);
    std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
}

bool ParsePayloadSizes() {
    if (FLAGS_payload_sizes.empty()) return true;
    size_t begin = 0;
    while (begin <= FLAGS_payload_sizes.size()) {
        const size_t end = FLAGS_payload_sizes.find(',', begin);
        const std::string item = FLAGS_payload_sizes.substr(begin, end - begin);
        try {
            size_t parsed = 0;
            if (item.empty() ||
                item.find_first_not_of("0123456789") != std::string::npos)
                return false;
            const uint64_t size = std::stoull(item, &parsed);
            if (parsed != item.size() || size == 0) return false;
            payload_sizes.push_back(size);
        } catch (const std::exception&) {
            return false;
        }
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return !payload_sizes.empty();
}

std::shared_ptr<ClientTestWrapper> CreateClient() {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_connect_timeout_sec);
    do {
        auto client = ClientTestWrapper::CreateClientWrapper(
            "localhost:" + std::to_string(FLAGS_port), FLAGS_engine_meta_url,
            FLAGS_protocol, FLAGS_device_name, FLAGS_master_server_entry);
        if (client) return *client;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    } while (std::chrono::steady_clock::now() < deadline);
    return nullptr;
}

class AckManifest {
   public:
    explicit AckManifest(const std::string& path)
        : fd_(open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)) {}

    ~AckManifest() {
        if (fd_ >= 0) close(fd_);
    }

    bool valid() const { return fd_ >= 0; }

    bool Append(uint64_t index) {
        const std::string line = std::to_string(index) + "\n";
        size_t written = 0;
        while (written < line.size()) {
            const ssize_t n =
                write(fd_, line.data() + written, line.size() - written);
            if (n <= 0) return false;
            written += static_cast<size_t>(n);
        }
        return fsync(fd_) == 0;
    }

   private:
    int fd_;
};

bool ReadManifest(std::vector<uint64_t>& indexes) {
    std::ifstream input(FLAGS_manifest);
    if (!input) return false;
    std::set<uint64_t> seen;
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty() ||
            line.find_first_not_of("0123456789") != std::string::npos) {
            return false;
        }
        try {
            size_t parsed = 0;
            uint64_t index = std::stoull(line, &parsed);
            if (parsed != line.size() || !seen.insert(index).second) {
                return false;
            }
            indexes.push_back(index);
        } catch (const std::exception&) {
            return false;
        }
    }
    return !indexes.empty();
}

bool VerifyOne(ClientTestWrapper& client, uint64_t index) {
    const std::string expected = Payload(index);
    Pace(expected.size());
    std::string actual;
    const ErrorCode error = client.Get(Key(index), actual);
    if (error != ErrorCode::OK) {
        std::cerr << "get_failed index=" << index
                  << " error=" << toString(error) << '\n';
        return false;
    }
    if (actual != expected) {
        std::cerr << "value_mismatch index=" << index
                  << " actual_size=" << actual.size() << '\n';
        return false;
    }
    return true;
}

int Seed(ClientTestWrapper& client) {
    AckManifest manifest(FLAGS_manifest);
    if (!manifest.valid()) return 2;
    uint64_t put_ok = 0;
    for (uint64_t offset = 0; offset < FLAGS_count; ++offset) {
        const uint64_t index = FLAGS_start_index + offset;
        const std::string payload = Payload(index);
        Pace(payload.size());
        const ErrorCode error = client.Put(Key(index), payload);
        if (error != ErrorCode::OK || !manifest.Append(index)) {
            std::cerr << "seed_failed index=" << index
                      << " error=" << toString(error) << '\n';
            return 20;
        }
        ++put_ok;
    }
    uint64_t get_ok = 0;
    for (uint64_t offset = 0; offset < FLAGS_count; ++offset) {
        if (!VerifyOne(client, FLAGS_start_index + offset)) return 21;
        ++get_ok;
    }
    std::cout << "summary mode=seed put_ok=" << put_ok
              << " put_fail=0 get_ok=" << get_ok << " get_fail=0 mismatch=0\n";
    return 0;
}

int Verify(ClientTestWrapper& client) {
    std::vector<uint64_t> indexes;
    if (!ReadManifest(indexes)) return 2;
    uint64_t get_ok = 0;
    for (uint64_t index : indexes) {
        if (!VerifyOne(client, index)) return 21;
        ++get_ok;
    }
    std::cout << "summary mode=verify put_ok=0 put_fail=0 get_ok=" << get_ok
              << " get_fail=0 mismatch=0\n";
    return 0;
}

int Delete(ClientTestWrapper& client) {
    std::vector<uint64_t> indexes;
    if (!ReadManifest(indexes)) return 2;
    for (uint64_t index : indexes) {
        Pace(0);
        const ErrorCode error = client.Delete(Key(index));
        if (error != ErrorCode::OK) {
            std::cerr << "delete_failed index=" << index
                      << " error=" << toString(error) << '\n';
            return 20;
        }
    }
    std::cout << "summary mode=delete delete_ok=" << indexes.size()
              << " delete_fail=0\n";
    return 0;
}

int VerifyAbsent(ClientTestWrapper& client) {
    std::vector<uint64_t> indexes;
    if (!ReadManifest(indexes)) return 2;
    for (uint64_t index : indexes) {
        Pace(0);
        std::string value;
        const ErrorCode error = client.Get(Key(index), value);
        if (error != ErrorCode::OBJECT_NOT_FOUND) {
            std::cerr << "unexpected_present index=" << index
                      << " error=" << toString(error) << '\n';
            return 21;
        }
    }
    std::cout << "summary mode=verify-absent absent_ok=" << indexes.size()
              << " absent_fail=0\n";
    return 0;
}

int Pressure(ClientTestWrapper& client) {
    AckManifest manifest(FLAGS_manifest);
    if (!manifest.valid()) return 2;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_duration_sec);
    uint64_t index = FLAGS_start_index;
    uint64_t put_ok = 0;
    uint64_t get_ok = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        const std::string payload = Payload(index);
        Pace(payload.size());
        const ErrorCode error = client.Put(Key(index), payload);
        if (error != ErrorCode::OK || !manifest.Append(index)) {
            std::cerr << "pressure_put_failed index=" << index
                      << " error=" << toString(error) << '\n';
            return 20;
        }
        ++put_ok;
        if (!VerifyOne(client, index)) return 21;
        ++get_ok;
        ++index;
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sleep_ms));
    }
    if (put_ok == 0 || get_ok == 0) return 22;
    std::cout << "summary mode=pressure put_ok=" << put_ok
              << " put_fail=0 get_ok=" << get_ok << " get_fail=0 mismatch=0\n";
    return 0;
}

int Provider(ClientTestWrapper& client) {
    void* buffer = nullptr;
    const ErrorCode error = client.Mount(FLAGS_segment_size, buffer);
    if (error != ErrorCode::OK) {
        std::cerr << "provider_mount_failed error=" << toString(error) << '\n';
        return 20;
    }
    std::cout << "provider_ready size=" << FLAGS_segment_size << std::endl;
    while (true) pause();
}

// Keep a single HA client alive across failover. Transport errors are recorded;
// corrupt successful reads fail immediately. Each line is flushed for auditing.
int Observe(ClientTestWrapper& client) {
    std::vector<uint64_t> indexes;
    if (!ReadManifest(indexes)) return 2;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_duration_sec);
    uint64_t ok = 0, failed = 0, iteration = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto index = indexes[iteration++ % indexes.size()];
        const auto expected = Payload(index);
        Pace(expected.size());
        const auto start = std::chrono::steady_clock::now();
        const auto unix_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        std::string actual;
        const auto error = client.Get(Key(index), actual);
        const bool mismatch = error == ErrorCode::OK && actual != expected;
        const auto elapsed_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start)
                .count();
        std::cout << "observation start_unix_us=" << unix_us
                  << " latency_us=" << elapsed_us << " index=" << index
                  << " error=" << static_cast<int>(error)
                  << " mismatch=" << mismatch << std::endl;
        if (mismatch) return 21;
        if (error == ErrorCode::OK)
            ++ok;
        else
            ++failed;
    }
    std::cout << "summary mode=observe get_ok=" << ok << " get_fail=" << failed
              << " mismatch=0" << std::endl;
    return ok ? 0 : 21;
}

// Unique keys make a failed mutation independently reconcilable: never issue
// another mutation for that key after a non-OK response.
int Mutate(ClientTestWrapper& client) {
    const int fd =
        open(FLAGS_manifest.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return 2;
    auto record = [fd](const std::string& text) {
        const std::string line = text + "\n";
        size_t offset = 0;
        while (offset < line.size()) {
            const auto n =
                write(fd, line.data() + offset, line.size() - offset);
            if (n <= 0) throw std::runtime_error("journal write failed");
            offset += n;
        }
        if (fsync(fd)) throw std::runtime_error("journal fsync failed");
        std::cout << line << std::flush;
    };
    struct Expected {
        uint64_t index;
        int before;
        int after;
        bool uncertain;
    };
    std::vector<Expected> expected;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_duration_sec);
    try {
        for (uint64_t index = 0;
             index < FLAGS_count && std::chrono::steady_clock::now() < deadline;
             ++index) {
            int state = 0;
            // Keep PUT-only, deleted, and recreated cohorts.
            const int steps = static_cast<int>(index % 3) + 1;
            for (int step = 0; step < steps; ++step) {
                const int next = step == 1 ? 0 : (step == 0 ? 1 : 2);
                FLAGS_payload_version = next == 2 ? "v2" : "v1";
                const auto value = Payload(index);
                Pace(step == 1 ? 0 : value.size());
                const auto us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
                const auto fields = " index=" + std::to_string(index) +
                                    " step=" + std::to_string(step);
                record("intent" + fields + " time_us=" + std::to_string(us));
                const auto error = step == 1 ? client.Delete(Key(index))
                                             : client.Put(Key(index), value);
                record("result" + fields +
                       " error=" + std::to_string(static_cast<int>(error)));
                if (error != ErrorCode::OK) {
                    expected.push_back({index, state, next, true});
                    break;
                }
                state = next;
                if (step + 1 == steps)
                    expected.push_back({index, state, state, false});
            }
        }
        // Freeze writes and allow any timed-out operation to settle before
        // reads.
        record("writes_finished count=" + std::to_string(expected.size()));
        std::this_thread::sleep_for(std::chrono::seconds(15));
        for (const auto& item : expected) {
            Pace(FLAGS_payload_size);
            std::string actual;
            const auto error = client.Get(Key(item.index), actual);
            int observed = -1;
            if (error == ErrorCode::OBJECT_NOT_FOUND) observed = 0;
            if (error == ErrorCode::OK) {
                FLAGS_payload_version = "v1";
                if (actual == Payload(item.index)) observed = 1;
                FLAGS_payload_version = "v2";
                if (actual == Payload(item.index)) observed = 2;
            }
            record("reconcile index=" + std::to_string(item.index) +
                   " observed=" + std::to_string(observed) +
                   " error=" + std::to_string(static_cast<int>(error)));
            if (observed != item.after &&
                !(item.uncertain && observed == item.before)) {
                close(fd);
                return 21;
            }
        }
        record("summary reconciled=" + std::to_string(expected.size()));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        close(fd);
        return 22;
    }
    close(fd);
    return expected.empty() ? 22 : 0;
}

}  // namespace
}  // namespace mooncake::testing

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    // The default async appender has no consumer until explicitly initialized.
    easylog::init_log(easylog::Severity::WARN, "", false, true);
    if (!mooncake::testing::ParsePayloadSizes() ||
        (FLAGS_mode != "provider" && FLAGS_mode != "seed" &&
         FLAGS_mode != "verify" && FLAGS_mode != "delete" &&
         FLAGS_mode != "verify-absent" && FLAGS_mode != "pressure" &&
         FLAGS_mode != "observe" && FLAGS_mode != "mutate") ||
        (FLAGS_mode != "provider" && FLAGS_manifest.empty()) ||
        FLAGS_key_prefix.empty() || FLAGS_payload_size == 0 ||
        FLAGS_connect_timeout_sec == 0 ||
        (FLAGS_mode == "provider" && FLAGS_segment_size == 0) ||
        (FLAGS_mode == "seed" && FLAGS_count == 0) ||
        ((FLAGS_mode == "pressure" || FLAGS_mode == "observe") &&
         FLAGS_duration_sec == 0)) {
        std::cerr << "invalid arguments\n";
        return 2;
    }
    auto client = mooncake::testing::CreateClient();
    if (!client) return 10;
    if (FLAGS_mode == "provider") return mooncake::testing::Provider(*client);
    if (FLAGS_mode == "seed") return mooncake::testing::Seed(*client);
    if (FLAGS_mode == "verify") return mooncake::testing::Verify(*client);
    if (FLAGS_mode == "observe") return mooncake::testing::Observe(*client);
    if (FLAGS_mode == "mutate") return mooncake::testing::Mutate(*client);
    if (FLAGS_mode == "delete") return mooncake::testing::Delete(*client);
    if (FLAGS_mode == "verify-absent")
        return mooncake::testing::VerifyAbsent(*client);
    return mooncake::testing::Pressure(*client);
}
