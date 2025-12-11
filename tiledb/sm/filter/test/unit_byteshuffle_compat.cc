#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "../byteshuffle_filter.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"

#include <random>

#include "blosc/tiledb-shuffle.h"

using namespace tiledb::sm;

static void require_equal_buffers(
    span<const std::byte> left, span<const std::byte> right) {
  REQUIRE(left.size() == right.size());
  if (memcmp(left.data(), left.data(), left.size()) != 0) {
    for (size_t i = 0; i < left.size(); i++) {
      if (left[i] != right[i]) {
        FAIL(
            "Mismatch at index " + std::to_string(i) + ": " +
            std::to_string((uint8_t)left[i]) +
            " != " + std::to_string((uint8_t)right[i]));
      }
    }
  }
}

TEST_CASE("Byteshuffle compatibility test", "[byteshuffle][compat]") {
  int size = GENERATE(1 << 10, 8 << 10, 1 << 20);
  // Test with not round block sizes. There's no reason to test non-multiples of
  // 8, as the byteshuffle filter does not pass these to the byteshuffle
  // implementation.
  size += GENERATE(0, 8, 16);
  Datatype type = GENERATE(
      Datatype::UINT8, Datatype::UINT16, Datatype::UINT32, Datatype::UINT64);
  auto typesize = datatype_size(type);
  std::vector<std::byte> source_data(size);

  std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<uint16_t> dist(0, 255);
  for (auto& byte : source_data) {
    byte = (std::byte)dist(rng);
  }

  std::vector<std::byte> data = source_data;
  Buffer blosc2_shuffled(size);
  blosc2_shuffled.advance_size(size);
  std::vector<std::byte> bshuf_shuffled(size);

  // Test shuffle implementations produce the same output.
  throw_if_not_ok(
      ByteshuffleFilter::shuffle_part(
          type, ConstBuffer(data.data(), data.size()), blosc2_shuffled, false));
  blosc::shuffle(
      typesize, size, (uint8_t*)data.data(), (uint8_t*)bshuf_shuffled.data());
  require_equal_buffers(
      std::as_bytes(blosc2_shuffled.cur_span()), bshuf_shuffled);
}
