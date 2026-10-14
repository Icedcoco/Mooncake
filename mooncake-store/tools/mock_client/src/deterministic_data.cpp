#include "mock_client/deterministic_data.h"

#include <array>
#include <stdexcept>
#include <type_traits>

namespace mock_client {
namespace {

constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;
constexpr std::string_view kKeyDomain = "mooncake.mock-client.key.v1";
constexpr std::string_view kSizeDomain = "mooncake.mock-client.size.v1";
constexpr std::string_view kChecksumDomain =
    "mooncake.mock-client.checksum.v1";

void HashByte(uint64_t& hash, uint8_t byte) {
    hash ^= byte;
    hash *= kFnvPrime;
}

void HashBytes(uint64_t& hash, std::string_view bytes) {
    for (unsigned char byte : bytes) {
        HashByte(hash, byte);
    }
}

template <typename Integer>
void HashFixedWidth(uint64_t& hash, Integer value) {
    static_assert(std::is_unsigned_v<Integer>);
    for (std::size_t byte_index = 0; byte_index < sizeof(Integer);
         ++byte_index) {
        HashByte(hash, static_cast<uint8_t>(value & 0xff));
        value >>= 8;
    }
}

void HashString(uint64_t& hash, std::string_view value) {
    HashFixedWidth(hash, static_cast<uint64_t>(value.size()));
    HashBytes(hash, value);
}

uint64_t FinishHash(uint64_t hash) {
    // A fixed 64-bit finalizer keeps adjacent input values well distributed.
    hash ^= hash >> 30;
    hash *= 0xbf58476d1ce4e5b9ULL;
    hash ^= hash >> 27;
    hash *= 0x94d049bb133111ebULL;
    hash ^= hash >> 31;
    return hash;
}

uint64_t StartHash(std::string_view domain, uint64_t seed,
                   std::string_view namespace_name, uint32_t client_index,
                   uint64_t object_index) {
    uint64_t hash = kFnvOffsetBasis;
    HashString(hash, domain);
    HashFixedWidth(hash, seed);
    HashString(hash, namespace_name);
    HashFixedWidth(hash, client_index);
    HashFixedWidth(hash, object_index);
    return hash;
}

void AppendHexByte(std::string& output, uint8_t value) {
    constexpr std::array<char, 16> digits = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    output.push_back(digits[value >> 4]);
    output.push_back(digits[value & 0x0f]);
}

template <typename Integer>
void AppendFixedHex(std::string& output, Integer value) {
    static_assert(std::is_unsigned_v<Integer>);
    constexpr std::size_t kWidth = sizeof(Integer) * 2;
    constexpr std::array<char, 16> digits = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    for (std::size_t nibble = 0; nibble < kWidth; ++nibble) {
        const auto shift = static_cast<unsigned>((kWidth - nibble - 1) * 4);
        output.push_back(digits[(value >> shift) & 0x0f]);
    }
}

}  // namespace

std::string DeterministicKey(uint64_t seed, std::string_view namespace_name,
                             uint32_t client_index, uint64_t object_index) {
    std::string key = "mc/";
    key.reserve(3 + namespace_name.size() * 2 + 2 + 16 + 2 + 8 + 2 + 16);
    for (unsigned char byte : namespace_name) {
        AppendHexByte(key, byte);
    }
    key += "/s";
    AppendFixedHex(key, seed);
    key += "/c";
    AppendFixedHex(key, client_index);
    key += "/o";
    AppendFixedHex(key, object_index);
    return key;
}

uint64_t DeterministicSize(uint64_t seed, std::string_view namespace_name,
                           uint32_t client_index, uint64_t object_index,
                           uint64_t min_size, uint64_t max_size) {
    if (min_size > max_size) {
        throw std::invalid_argument("min_size must not exceed max_size");
    }

    uint64_t hash = StartHash(kSizeDomain, seed, namespace_name, client_index,
                              object_index);
    HashFixedWidth(hash, min_size);
    HashFixedWidth(hash, max_size);
    hash = FinishHash(hash);

    const uint64_t range = max_size - min_size + 1;
    if (range == 0) {
        // The only overflowing inclusive range is the full uint64_t domain.
        return hash;
    }
    return min_size + (hash % range);
}

uint64_t DeterministicChecksum(uint64_t seed,
                               std::string_view namespace_name,
                               uint32_t client_index, uint64_t object_index,
                               uint64_t generation, uint64_t logical_size) {
    uint64_t hash = StartHash(kChecksumDomain, seed, namespace_name,
                              client_index, object_index);
    HashFixedWidth(hash, generation);
    HashFixedWidth(hash, logical_size);
    return FinishHash(hash);
}

ObjectIdentity MakeObjectIdentity(uint64_t seed,
                                  std::string_view namespace_name,
                                  uint32_t client_index,
                                  uint64_t object_index,
                                  uint64_t generation,
                                  uint64_t logical_size) {
    return ObjectIdentity{
        DeterministicKey(seed, namespace_name, client_index, object_index),
        generation,
        logical_size,
        DeterministicChecksum(seed, namespace_name, client_index, object_index,
                             generation, logical_size)};
}

}  // namespace mock_client
