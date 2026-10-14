#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace mock_client {

struct ObjectIdentity {
    std::string key;
    uint64_t generation;
    uint64_t logical_size;
    std::optional<uint64_t> checksum;
};

// The namespace is hex-encoded in the key. Integer fields are emitted in
// fixed-width hexadecimal, so namespace boundaries and key prefixes are
// stable across processes and standard-library implementations.
std::string DeterministicKey(uint64_t seed, std::string_view namespace_name,
                             uint32_t client_index, uint64_t object_index);

// Select a deterministic size in the inclusive [min_size, max_size] range.
// Throws std::invalid_argument when min_size is greater than max_size.
uint64_t DeterministicSize(uint64_t seed, std::string_view namespace_name,
                           uint32_t client_index, uint64_t object_index,
                           uint64_t min_size, uint64_t max_size);

// Return a deterministic metadata checksum over the fixed-width identity
// inputs. This is a reproducibility token, not a payload integrity checksum.
uint64_t DeterministicChecksum(uint64_t seed,
                               std::string_view namespace_name,
                               uint32_t client_index, uint64_t object_index,
                               uint64_t generation, uint64_t logical_size);

ObjectIdentity MakeObjectIdentity(uint64_t seed,
                                  std::string_view namespace_name,
                                  uint32_t client_index,
                                  uint64_t object_index,
                                  uint64_t generation,
                                  uint64_t logical_size);

}  // namespace mock_client
