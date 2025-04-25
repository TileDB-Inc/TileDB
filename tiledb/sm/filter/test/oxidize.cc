/**
 * @file tiledb/sm/filter/test/oxidize.cc
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
 * Provides definitions for functions used in the Rust FFI bindings.
 */

#ifndef TILEDB_FILTER_TEST_OXIDIZE_H
#define TILEDB_FILTER_TEST_OXIDIZE_H

#include "tiledb/sm/filter/test/oxidize.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/filter/test/filter_test_support.h"
#include "tiledb/sm/filter/test/oxidize/cxxbridge/rust/cxx.h"
#include "tiledb/tiledb_export.h"

#include <test/support/assert_helpers.h>

namespace tiledb::sm::test {

/**
 * @return a filter pipeline used in SC-65154
 */
TILEDB_EXPORT std::unique_ptr<FilterPipeline> build_pipeline_65154() {
  const Datatype input_type = Datatype::INT32;
  FilterPipeline pipeline;
  pipeline.add_filter(CompressionFilter(
      Compressor::DOUBLE_DELTA, 0, input_type, Datatype::ANY));
  pipeline.add_filter(BitWidthReductionFilter(input_type));
  pipeline.add_filter(CompressionFilter(Compressor::ZSTD, 9, input_type));

  return std::unique_ptr<FilterPipeline>(
      new FilterPipeline(std::move(pipeline)));
}

/**
 * Runs `check_run_pipeline_roundtrip` from `filter_test_support.h` against
 * some Rust data
 */
TILEDB_EXPORT void filter_pipeline_roundtrip(
    const FilterPipeline& pipeline, rust::Slice<const uint8_t> data) {
  tiledb::sm::Config config;

  ThreadPool thread_pool(4);
  auto tracker = tiledb::test::create_test_memory_tracker();

  const Datatype input_type = Datatype::INT32;  // FIXME
  VecDataGenerator<tiledb::test::AsserterRuntimeException> tile_gen(
      input_type, std::span<const uint8_t>(data.begin(), data.end()));
  auto [input_tile, offsets_tile] = tile_gen.create_writer_tiles(tracker);

  check_run_pipeline_roundtrip(
      config,
      thread_pool,
      input_tile,
      offsets_tile,
      pipeline,
      &tile_gen,
      tracker);
}

}  // namespace tiledb::sm::test

#endif
