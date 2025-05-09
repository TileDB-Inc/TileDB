/**
 * @file unit_65154.cc
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
 * This set of unit tests checks running the filter pipeline with the
 * filter pipeline used in SC-65154:
 * `[DoubleDeltaFilter(reinterp_dtype=None),
 * BitWidthReductionFilter(window=256), ZstdFilter(level=9)]`.
 */

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include "filter_test_support.h"
#include "test/support/rapidcheck/datatype.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("Filter: Round trip SC-65154", "[filter][rapidcheck]") {
  tiledb::sm::Config config;

  ThreadPool thread_pool(4);
  auto tracker = tiledb::test::create_test_memory_tracker();

  auto doit = [&]<typename Asserter>(
                  Datatype input_type, const std::vector<uint8_t>& data) {
    VecDataGenerator<Asserter> tile_gen(input_type, data);

    auto [input_tile, offsets_tile] = tile_gen.create_writer_tiles(tracker);

    FilterPipeline pipeline;
    pipeline.add_filter(CompressionFilter(
        Compressor::DOUBLE_DELTA, 0, input_type, Datatype::ANY));
    pipeline.add_filter(BitWidthReductionFilter(input_type));
    pipeline.add_filter(CompressionFilter(Compressor::ZSTD, 9, input_type));

    check_run_pipeline_roundtrip(
        config,
        thread_pool,
        input_tile,
        offsets_tile,
        pipeline,
        &tile_gen,
        tracker);
  };

  SECTION("Shrinking") {
    const std::vector<uint8_t> bytes = {
        0,
        0,
        128,
        64,
        128,
        255,
        127,
        0,
        0,
        0,
        0,
        0,
    };

    doit.operator()<tiledb::test::AsserterCatch>(Datatype::INT32, bytes);
  }

  SECTION("Rapidcheck int32") {
    rc::prop("Filter: Round trip SC-65154 int32", [doit]() {
      const auto bytes = *rc::make_input_bytes(Datatype::INT32);
      doit.operator()<tiledb::test::AsserterRapidcheck>(Datatype::INT32, bytes);
    });
  }

  SECTION("Rapidcheck uint32") {
    rc::prop("Filter: Round trip SC-65154 uint32", [doit]() {
      const auto bytes = *rc::make_input_bytes(Datatype::UINT32);
      doit.operator()<tiledb::test::AsserterRapidcheck>(
          Datatype::UINT32, bytes);
    });
  }
}
