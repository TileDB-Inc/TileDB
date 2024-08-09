/**
 * @file unit_webp_pipeline.cc
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
 * This set of unit tests checks running the filter pipeline with the webp
 * filter.
 */

#include <catch2/catch_test_macros.hpp>
#include "filter_test_support.h"
#include "test/support/src/mem_helpers.h"
#include "test/support/src/whitebox_helpers.h"
#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::sm;

TEST_CASE("Filter: Round trip WebpFilter RGB data", "[filter][webp]") {
  tiledb::sm::Config config;
  auto tracker = tiledb::test::create_test_memory_tracker();

  uint64_t height = 100;
  uint64_t width = 100;
  uint64_t row_stride = width * 3;
  auto tile = make_shared<WriterTile>(
      HERE(),
      constants::format_version,
      Datatype::UINT8,
      sizeof(uint8_t),
      height * row_stride,
      tracker);

  // Write full image in a single tile with chunking enabled.
  std::vector<uint8_t> data{0, 125, 255};
  std::vector<uint8_t> expected_result(height * row_stride, 0);
  for (uint64_t i = 0; i < height * width; i++) {
    // Write three values for each RGB pixel.
    for (uint64_t j = 0; j < 3; j++) {
      CHECK_NOTHROW(tile->write(&data[j], i * 3 + j, sizeof(uint8_t)));
      expected_result[i * 3 + j] = data[j];
    }
  }
  // For the write process 10 rows at a time using tile chunking.
  // The row stride is 300 bytes, so the tile chunk size is 3000 bytes.
  uint64_t extent_y = 10;
  WhiteboxWriterTile::set_max_tile_chunk_size(extent_y * row_stride);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  float quality = 100.0f;
  bool lossless = true;
  // NOTE: The extent_y parameter must respect chunk size or risk OOB access.
  // This sets WebpFilter::extents_ which is passed to WebP API during encoding.
  // If we set this to `height`, webp will reach past the end of chunked data.
  pipeline.add_filter(WebpFilter(
      quality,
      WebpInputFormat::WEBP_RGB,
      lossless,
      extent_y,
      width * 3,
      Datatype::UINT8));
  bool use_chunking = true;
  pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp, use_chunking);

  // Check the original unfiltered data was removed.
  CHECK(tile->size() == 0);
  CHECK(tile->filtered_buffer().size() != 0);

  // Read the full image back with chunking disabled.
  // NOTE: For the read case, WebP decoding APIs don't require height and width.
  // Instead, WebP takes references to these values during unfiltering and sets
  // them to the correct values after decoding is finished.
  WhiteboxWriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
  auto unfiltered_tile =
      create_tile_for_unfiltering(height * row_stride, tile, tracker);
  run_reverse(config, tp, unfiltered_tile, pipeline);

  for (uint64_t i = 0; i < height * row_stride; i++) {
    uint8_t value;
    CHECK_NOTHROW(unfiltered_tile.read(&value, i, sizeof(uint8_t)));
    CHECK(value == expected_result[i]);
  }
}
