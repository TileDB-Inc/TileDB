/**
 * @file unit_encryption_pipeline.cc
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
 * This set of unit tests checks running the filter pipeline with the encryption
 * filters.
 *
 */

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include "../encryption_aes256gcm_filter.h"
#include "filter_test_support.h"

using namespace tiledb::sm;

TEST_CASE("Filter: Test encryption", "[filter][encryption]") {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set up test data
  const uint64_t nelts = 1000;
  auto tile = make_increasing_tile(nelts, tracker);

  SECTION("- AES-256-GCM") {
    FilterPipeline pipeline;
    ThreadPool tp(4);
    pipeline.add_filter(EncryptionAES256GCMFilter(Datatype::UINT64));

    // No key set
    CHECK_THROWS(pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp));

    // Create and set a key
    char key[32];
    for (unsigned i = 0; i < 32; i++)
      key[i] = (char)i;
    auto filter = pipeline.get_filter<EncryptionAES256GCMFilter>();
    filter->set_key(key);

    // Check success
    pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }

    // Check error decrypting with wrong key.
    tile = make_increasing_tile(nelts, tracker);
    pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
    key[0]++;
    filter->set_key(key);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile2, pipeline, false);

    // Fix key and check success.
    auto unfiltered_tile3 = create_tile_for_unfiltering(nelts, tile, tracker);

    key[0]--;
    filter->set_key(key);
    run_reverse(config, tp, unfiltered_tile3, pipeline);

    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile3.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }
}
