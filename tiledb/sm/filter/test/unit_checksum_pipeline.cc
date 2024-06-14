/**
 * @file unit_checksum_pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This set of unit tests checks running the filter pipeline with the checksum
 * filters.
 *
 */

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include "../checksum_md5_filter.h"
#include "filter_test_support.h"
#include "test/support/src/mem_helpers.h"

using namespace tiledb::sm;

TEST_CASE(
    "Filter: Test skip checksum validation",
    "[filter][skip-checksum-validation]") {
  tiledb::sm::Config config;
  REQUIRE(config.set("sm.skip_checksum_validation", "true").ok());

  auto tracker = tiledb::test::create_test_memory_tracker();

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts, tracker);

  // MD5
  FilterPipeline md5_pipeline;
  ThreadPool tp(4);
  ChecksumMD5Filter md5_filter(Datatype::UINT64);
  md5_pipeline.add_filter(md5_filter);
  md5_pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
  CHECK(tile->size() == 0);
  CHECK(tile->filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
  run_reverse(config, tp, unfiltered_tile, md5_pipeline);

  for (uint64_t n = 0; n < nelts; n++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == n);
  }

  // SHA256
  auto tile2 = make_increasing_tile(nelts, tracker);

  FilterPipeline sha_256_pipeline;
  ChecksumMD5Filter sha_256_filter(Datatype::UINT64);
  sha_256_pipeline.add_filter(sha_256_filter);
  sha_256_pipeline.run_forward(&dummy_stats, tile2.get(), nullptr, &tp);
  CHECK(tile2->size() == 0);
  CHECK(tile2->filtered_buffer().size() != 0);

  auto unfiltered_tile2 = create_tile_for_unfiltering(nelts, tile2, tracker);
  run_reverse(config, tp, unfiltered_tile2, sha_256_pipeline);
  for (uint64_t n = 0; n < nelts; n++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile2.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == n);
  }
}
