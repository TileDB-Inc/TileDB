/**
 * @file tiledb/sm/filter/test/unit_filter_pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file tests FilterPipeline class
 *
 */

#include <catch.hpp>

#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"

#include "../bit_width_reduction_filter.h"
#include "../bitshuffle_filter.h"
#include "../compression_filter.h"
#include "../filter_pipeline.h"

using namespace tiledb::sm;

TEST_CASE(
    "FilterPipeline: Test if filter list has a filter", "[filter-pipeline]") {
  FilterPipeline fp;
  fp.add_filter(CompressionFilter(Compressor::ZSTD, 2));
  fp.add_filter(BitWidthReductionFilter());
  fp.add_filter(CompressionFilter(Compressor::RLE, 1));
  fp.add_filter(CompressionFilter(Compressor::LZ4, 1));

  // Check that filters are searched correctly
  CHECK(fp.has_filter(CompressionFilter(Compressor::RLE, 0)));
  CHECK(fp.has_filter(BitWidthReductionFilter()));
  CHECK_FALSE(fp.has_filter(CompressionFilter(Compressor::GZIP, 0)));
  CHECK_FALSE(fp.has_filter(BitshuffleFilter()));

  // Check no error when pipeline empty
  FilterPipeline fp2;
  CHECK_FALSE(fp2.has_filter(CompressionFilter(Compressor::RLE, 0)));
}

TEST_CASE(
    "FilterPipeline: Test if tile chunking should occur in filtering",
    "[filter-pipeline]") {
  // pipeline that contains an RLE compressor
  FilterPipeline fp_with_rle;
  fp_with_rle.add_filter(CompressionFilter(Compressor::ZSTD, 2));
  fp_with_rle.add_filter(BitWidthReductionFilter());
  fp_with_rle.add_filter(CompressionFilter(Compressor::RLE, 1));

  // pipeline that doesn't contain an RLE compressor
  FilterPipeline fp_without_rle;
  fp_without_rle.add_filter(CompressionFilter(Compressor::ZSTD, 2));
  fp_without_rle.add_filter(BitWidthReductionFilter());

  bool is_dimension = true;
  bool is_var_sized = true;

  // Do not chunk the Tile for filtering if RLE is used for var-sized string
  // dimensions
  CHECK_FALSE(fp_with_rle.use_tile_chunking(
      is_dimension, is_var_sized, Datatype::STRING_ASCII));

  // Chunk in any other case
  CHECK(fp_without_rle.use_tile_chunking(
      is_dimension, is_var_sized, Datatype::STRING_ASCII));
  CHECK(fp_with_rle.use_tile_chunking(
      !is_dimension, is_var_sized, Datatype::STRING_ASCII));
  CHECK(fp_with_rle.use_tile_chunking(
      is_dimension, !is_var_sized, Datatype::STRING_ASCII));
  CHECK(fp_with_rle.use_tile_chunking(
      !is_dimension, !is_var_sized, Datatype::STRING_ASCII));
  CHECK(fp_with_rle.use_tile_chunking(
      is_dimension, is_var_sized, Datatype::TIME_MS));
  CHECK(fp_with_rle.use_tile_chunking(
      is_dimension, is_var_sized, Datatype::DATETIME_AS));
  CHECK(fp_with_rle.use_tile_chunking(
      is_dimension, is_var_sized, Datatype::BLOB));
  CHECK(fp_with_rle.use_tile_chunking(
      is_dimension, is_var_sized, Datatype::INT32));
  CHECK(fp_with_rle.use_tile_chunking(
      is_dimension, is_var_sized, Datatype::FLOAT64));
}