/**
 * @file unit_double_delta_pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This set of unit tests checks running the filter pipeline with the webp
 * filter.
 */

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include "filter_test_support.h"
#include "test/support/oxidize/rust_std.h"
#include "test/support/rapidcheck/datatype.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE(
    "Filter: Double Delta overflow detection unsigned",
    "[filter][rapidcheck]") {
  auto doit = []<typename Asserter = tiledb::test::AsserterCatch>(
                  uint64_t a, uint64_t b)
                  ->std::optional<int64_t> {
    const std::optional<int64_t> detect =
        double_delta::Integral64<uint64_t>::delta(a, b);

    std::optional<int64_t> detect_rust;
    {
      int64_t value;
      if (rust::std::u64_checked_sub(a, b, value)) {
        detect_rust.emplace(value);
      }
    }
    ASSERTER(detect == detect_rust);

    return detect;
  };

  SECTION("Example") {
    CHECK(doit(0, 0) == 0);
    CHECK(doit(0, 1) == -1);
    CHECK(doit(0, 0x7FFFFFFFFFFFFFFF) == 0x8000000000000001);
    CHECK(doit(0, 0x8000000000000000) == 0x8000000000000000);
    CHECK(doit(0, 0x8000000000000001) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFE) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0x8000000000000000) == 0x7FFFFFFFFFFFFFFF);
  }

  SECTION("Rapidcheck") {
    rc::prop("Integral64<uint64_t>::delta", [doit](uint64_t a, uint64_t b) {
      doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
    });
  }
}

TEST_CASE(
    "Filter: Double Delta overflow detection signed", "[filter][rapidcheck]") {
  auto doit =
      []<typename Asserter = tiledb::test::AsserterCatch>(int64_t a, int64_t b)
          ->std::optional<int64_t> {
    const std::optional<int64_t> detect =
        double_delta::Integral64<int64_t>::delta(a, b);

    std::optional<int64_t> detect_rust;
    {
      int64_t value;
      if (rust::std::i64_checked_sub(a, b, value)) {
        detect_rust.emplace(value);
      }
    }
    ASSERTER(detect == detect_rust);

    return detect;
  };

  SECTION("Example") {
    CHECK(doit(0, 0) == 0);
    CHECK(doit(0, 1) == -1);
    CHECK(doit(0, 0x7FFFFFFFFFFFFFFF) == 0x8000000000000001);
    CHECK(doit(0, 0x8000000000000000) == std::nullopt);
    CHECK(doit(0, 0x8000000000000001) == 0x7FFFFFFFFFFFFFFF);
    CHECK(doit(-1, 0) == -1);
    CHECK(doit(-1, 0x7FFFFFFFFFFFFFFE) == 0x8000000000000001);
    CHECK(doit(-1, 0x7FFFFFFFFFFFFFFF) == 0x8000000000000000);
    CHECK(doit(-1, 0x8000000000000000) == 0x7FFFFFFFFFFFFFFF);
    CHECK(doit(0x7FFFFFFFFFFFFFFF, 0) == 0x7FFFFFFFFFFFFFFF);
    CHECK(doit(0x7FFFFFFFFFFFFFFF, -1) == std::nullopt);
  }

  SECTION("Rapidcheck") {
    rc::prop("Integral64<iint64_t>::delta", [doit](int64_t a, int64_t b) {
      doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
    });
  }
}

namespace rc {
static Gen<Datatype> make_reinterpret_datatype(Datatype input_type) {
  auto gen_reinterpret_datatype = gen::suchThat(
      gen::arbitrary<Datatype>(), [input_type](auto reinterpret_type) {
        // see filter_buffer.cc
        switch (reinterpret_type) {
          case Datatype::FLOAT32:
          case Datatype::FLOAT64:
          case Datatype::CHAR:
          case Datatype::STRING_ASCII:
          case Datatype::STRING_UTF8:
          case Datatype::STRING_UTF16:
          case Datatype::STRING_UTF32:
          case Datatype::STRING_UCS2:
          case Datatype::STRING_UCS4:
            return false;
          default:
            return datatype_size(input_type) %
                       datatype_size(reinterpret_type) ==
                   0;
        }
      });
  if (datatype_is_real(input_type)) {
    return gen::suchThat(gen_reinterpret_datatype, [](auto reinterpret_type) {
      return reinterpret_type != Datatype::ANY;
    });
  } else {
    return gen_reinterpret_datatype;
  }
}

static Gen<std::vector<uint8_t>> make_input_bytes(Datatype input_type) {
  auto gen_elt = rc::gen::container<std::vector<uint8_t>>(
      datatype_size(input_type), gen::arbitrary<uint8_t>());
  return gen::map(
      gen::nonEmpty(gen::container<std::vector<std::vector<uint8_t>>>(gen_elt)),
      [input_type](std::vector<std::vector<uint8_t>> elts) {
        std::vector<uint8_t> flat;
        flat.reserve(elts.size() * datatype_size(input_type));
        for (const std::vector<uint8_t>& elt : elts) {
          flat.insert(flat.end(), elt.begin(), elt.end());
        }
        return flat;
      });
}

}  // namespace rc

static shared_ptr<WriterTile> to_writer_tile(
    shared_ptr<MemoryTracker> tracker,
    Datatype input_type,
    std::span<const uint8_t> bytes) {
  auto wt = make_shared<WriterTile>(
      HERE(),
      constants::format_version,
      input_type,
      datatype_size(input_type),  // TODO: fix for cell val num
      bytes.size(),
      tracker);
  wt->write(&bytes[0], 0, bytes.size());
  return wt;
}

template <typename Asserter>
class VecDataGenerator : public TileDataGenerator {
  Datatype datatype_;
  const std::vector<uint8_t>& bytes_;

 public:
  VecDataGenerator(Datatype dt, const std::vector<uint8_t>& bytes)
      : datatype_(dt)
      , bytes_(bytes) {
  }

  uint64_t cell_size() const override {
    return datatype_size(datatype_);
  }

  void check_tile_data(const Tile& tile) const override {
    ASSERTER(tile.size() == bytes_.size());

    // compare inversion in chunks (so that there's a bit more context on each
    // line in the event of a failure)
    constexpr uint64_t chunk_size = 128;
    for (uint64_t i = 0; i < bytes_.size(); i += chunk_size) {
      const uint64_t offset = i;
      const uint64_t n = std::min(chunk_size, bytes_.size() - offset);

      std::vector<uint8_t> chunk_in(
          bytes_.begin() + offset, bytes_.begin() + offset + n);
      std::vector<uint8_t> chunk_out(n);
      tile.read(&chunk_out[0], offset, n);

      ASSERTER(chunk_in == chunk_out);
    }
  }

  Datatype datatype() const override {
    return datatype_;
  }

  std::tuple<shared_ptr<WriterTile>, std::optional<shared_ptr<WriterTile>>>
  create_writer_tiles(shared_ptr<MemoryTracker> memory_tracker) const override {
    return std::make_pair(
        to_writer_tile(memory_tracker, datatype(), bytes_), std::nullopt);
  }

  uint64_t original_tile_size() const override {
    return bytes_.size();
  }
};

TEST_CASE("Filter: Round trip Compressor DoubleDelta", "[filter][rapidcheck]") {
  tiledb::sm::Config config;

  ThreadPool thread_pool(4);
  auto tracker = tiledb::test::create_test_memory_tracker();

  auto doit = [&]<typename Asserter>(
                  int level,
                  Datatype input_type,
                  Datatype reinterpret_datatype,
                  const std::vector<uint8_t>& data) {
    VecDataGenerator<Asserter> tile_gen(input_type, data);

    auto [input_tile, offsets_tile] = tile_gen.create_writer_tiles(tracker);

    FilterPipeline pipeline;
    pipeline.add_filter(CompressionFilter(
        Compressor::DOUBLE_DELTA, level, input_type, reinterpret_datatype));

    check_run_pipeline_roundtrip(
        config,
        thread_pool,
        input_tile,
        offsets_tile,
        pipeline,
        &tile_gen,
        tracker);
  };

  SECTION("Example") {
    const std::vector<uint8_t> data = {0, 0, 0, 0, 0, 0, 0, 1};
    doit.operator()<tiledb::test::AsserterCatch>(
        0, Datatype::UINT64, Datatype::UINT64, data);
  }

  SECTION("Shrinking") {
    // overflow scenario
    if (true) {
      const std::vector<uint8_t> data = {0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0};
      doit.operator()<tiledb::test::AsserterCatch>(
          0, Datatype::UINT64, Datatype::UINT64, data);
    }
    // ???
    // FIXME: this is a regression from the checked sub code
    // step through with gdb, find the encoded values,
    // pop stash, see what the new ones are
    // deltas: [-1, 1]
    // double: [2]
    {
      const std::vector<uint8_t> data = {1, 0, 1};
      doit.operator()<tiledb::test::AsserterCatch>(
          0, Datatype::UINT8, Datatype::ANY, data);
    }
  }

  SECTION("Rapidcheck") {
    rc::prop("Filter: Round trip Compressor Double Delta", [doit]() {
      const int level = *rc::gen::arbitrary<int>();
      const auto datatype = *rc::gen::arbitrary<Datatype>();
      const auto reinterpret = *rc::make_reinterpret_datatype(datatype);
      const auto bytes = *rc::make_input_bytes(datatype);

      doit.operator()<tiledb::test::AsserterRapidcheck>(
          level, datatype, reinterpret, bytes);
    });
  }
}
