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
 * This set of unit tests checks running the filter pipeline with the double
 * delta filter.
 */

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include "filter_test_support.h"
#include "test/support/rapidcheck/datatype.h"
#include "test/support/src/mem_helpers.h"
#include "tile_data_generator.h"
#include "tiledb/common/arithmetic.h"
#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/type/datatype_queries.h"

using namespace tiledb::common;
using namespace tiledb::sm;

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

}  // namespace rc

/**
 * @return true if we should expect `bytes` to overflow when interpreted
 * using `filter_type`
 */
static bool expect_overflow(
    Datatype filter_type, std::span<const uint8_t> data) {
  if (datatype_size(filter_type) % sizeof(uint64_t) != 0) {
    return false;
  }

  std::vector<int64_t> deltae;
  if (tiledb::type::has_signed_value_type(filter_type)) {
    std::span<const int64_t> values(
        reinterpret_cast<const int64_t*>(&data[0]),
        data.size() / sizeof(int64_t));
    if (values.size() <= 2) {
      return false;
    }
    for (uint64_t i = 1; i < values.size(); i++) {
      std::optional<int64_t> delta =
          checked_arithmetic<int64_t>::sub(values[i], values[i - 1]);
      if (delta.has_value()) {
        deltae.push_back(delta.value());
      } else {
        return true;
      }
    }
  } else {
    std::span<const uint64_t> values(
        reinterpret_cast<const uint64_t*>(&data[0]),
        data.size() / sizeof(int64_t));
    if (values.size() <= 2) {
      return false;
    }
    for (uint64_t i = 1; i < values.size(); i++) {
      std::optional<int64_t> delta =
          checked_arithmetic<int64_t>::sub_signed(values[i], values[i - 1]);
      if (delta.has_value()) {
        deltae.push_back(delta.value());
      } else {
        return true;
      }
    }
  }
  for (uint64_t i = 1; i < deltae.size(); i++) {
    if (!checked_arithmetic<int64_t>::sub(deltae[i], deltae[i - 1])
             .has_value()) {
      return true;
    }
  }
  return false;
}

TEST_CASE("Filter: Round trip Compressor DoubleDelta", "[filter][rapidcheck]") {
  tiledb::sm::Config config;

  ThreadPool thread_pool(4);
  auto tracker = tiledb::test::create_test_memory_tracker();

  auto doit = [&]<typename Asserter>(
                  Datatype input_type,
                  Datatype reinterpret_datatype,
                  const std::vector<uint8_t>& data) {
    VecDataGenerator<Asserter> tile_gen(input_type, data);

    auto [input_tile, offsets_tile] = tile_gen.create_writer_tiles(tracker);

    FilterPipeline pipeline;
    pipeline.add_filter(CompressionFilter(
        Compressor::DOUBLE_DELTA, 0, input_type, reinterpret_datatype));

    bool overflowed = false;
    try {
      check_run_pipeline_roundtrip(
          config,
          thread_pool,
          input_tile,
          offsets_tile,
          pipeline,
          &tile_gen,
          tracker);
    } catch (const StatusException& e) {
      const std::string what(e.what());
      if (what.find("Cannot compress with DoubleDelta: delta exceeds range "
                    "of int64_t") != std::string::npos) {
        overflowed = true;
      } else if (
          what.find("Some negative double delta is out of bounds") !=
          std::string::npos) {
        overflowed = true;
      } else {
        throw;
      }
    }
    const Datatype interpret_type =
        (reinterpret_datatype == Datatype::ANY ? input_type :
                                                 reinterpret_datatype);
    ASSERTER(overflowed == expect_overflow(interpret_type, data));
  };

  SECTION("Example") {
    const std::vector<uint8_t> data = {0, 0, 0, 0, 0, 0, 0, 1};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::UINT64, Datatype::UINT64, data);
  }

  SECTION("Shrinking 1") {
    // overflow scenario
    const std::vector<uint8_t> data = {0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::UINT64, Datatype::UINT64, data);
  }
  SECTION("Shrinking 2") {
    const std::vector<uint8_t> data = {1, 0, 1};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::UINT8, Datatype::ANY, data);
  }
  SECTION("Shrinking 3") {
    const std::vector<uint8_t> data = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::INT64, Datatype::ANY, data);
  }
  SECTION("Shrinking 4") {
    const std::vector<uint8_t> data = {0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 93};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::INT64, Datatype::INT64, data);
  }
  SECTION("Shrinking 5") {
    const std::vector<uint8_t> data = {
        2, 0, 2, 1, 2, 3, 0, 2, 1, 3, 1, 0, 3, 0, 1, 2};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::DATETIME_DAY, Datatype::UINT64, data);
  }
  SECTION("Shrinking 6") {
    const std::vector<uint8_t> data = {0, 1, 0, 0, 1, 1, 1, 0};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::DATETIME_MONTH, Datatype::UINT64, data);
  }
  SECTION("Shrinking 7") {
    const std::vector<uint8_t> data = {
        0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 128};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::DATETIME_MONTH, Datatype::UINT64, data);
  }

  SECTION("Shrinking CORE-407") {
    const std::vector<uint8_t> data = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128};
    doit.operator()<tiledb::test::AsserterCatch>(
        Datatype::TIME_NS, Datatype::UINT64, data);
  }

  SECTION("Rapidcheck all") {
    rc::prop("Filter: Round trip Compressor Double Delta", [doit]() {
      const auto datatype = *rc::gen::arbitrary<Datatype>();
      const auto reinterpret = *rc::make_reinterpret_datatype(datatype);
      const auto bytes = *rc::make_input_bytes(datatype);

      doit.operator()<tiledb::test::AsserterRapidcheck>(
          datatype, reinterpret, bytes);
    });
  }

  SECTION("Rapidcheck non-overflowing") {
    rc::prop(
        "Filter: Round Trip Compressor Double Delta non-overflowing", [doit]() {
          const auto [datatype, reinterpret] = *rc::gen::suchThat(
              rc::gen::mapcat(
                  rc::gen::arbitrary<Datatype>(),
                  [](Datatype input) {
                    return rc::gen::pair(
                        rc::gen::just(input),
                        rc::make_reinterpret_datatype(input));
                  }),
              [](std::pair<Datatype, Datatype> types) {
                if (datatype_size(types.first) < sizeof(int64_t)) {
                  return true;
                } else if (
                    types.second != Datatype::ANY &&
                    datatype_size(types.second) < sizeof(int64_t)) {
                  return true;
                } else {
                  return false;
                }
              });

          const auto bytes = *rc::make_input_bytes(datatype);

          doit.operator()<tiledb::test::AsserterRapidcheck>(
              datatype, reinterpret, bytes);
        });
  }
}
