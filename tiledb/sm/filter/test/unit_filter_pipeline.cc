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
 */

#include <catch.hpp>

#include "../bit_width_reduction_filter.h"
#include "../bitshuffle_filter.h"
#include "../byteshuffle_filter.h"
#include "../checksum_md5_filter.h"
#include "../checksum_sha256_filter.h"
#include "../compression_filter.h"
#include "../encryption_aes256gcm_filter.h"
#include "../filter.h"
#include "../filter_pipeline.h"
#include "../noop_filter.h"
#include "../positive_delta_filter.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"

using namespace tiledb::sm;

template <class T, int n>
inline T& filters_buffer_offset(void* p) {
  return *static_cast<T*>(static_cast<void*>(static_cast<char*>(p) + n));
}

TEST_CASE(
    "FilterPipeline: Test deserialization", "[filter_pipeline][deserialize]") {
  uint32_t max_chunk_size = 4096;
  uint32_t num_filters = 3;

  // Filter1: zstd
  int32_t compressor_level1 = 1;
  Compressor compressor1 = Compressor::ZSTD;
  FilterType filtertype1 = FilterType::FILTER_ZSTD;

  // Filter2: rle
  Compressor compressor2 = Compressor::RLE;
  FilterType filtertype2 = FilterType::FILTER_RLE;

  // Filter3: gzip
  int32_t compressor_level3 = 1;
  Compressor compressor3 = Compressor::GZIP;
  FilterType filtertype3 = FilterType::FILTER_GZIP;

  char serialized_buffer[38];
  char* p = &serialized_buffer[0];

  filters_buffer_offset<uint32_t, 0>(p) = max_chunk_size;
  // Set number of filters
  filters_buffer_offset<uint32_t, 4>(p) = num_filters;

  // Set filter1
  filters_buffer_offset<uint8_t, 8>(p) = static_cast<uint8_t>(filtertype1);
  filters_buffer_offset<uint32_t, 9>(p) =
      sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
  filters_buffer_offset<uint8_t, 13>(p) = static_cast<uint8_t>(compressor1);
  filters_buffer_offset<int32_t, 14>(p) = compressor_level1;

  // Set filter2
  filters_buffer_offset<uint8_t, 18>(p) = static_cast<uint8_t>(filtertype2);
  filters_buffer_offset<uint32_t, 19>(p) =
      sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
  filters_buffer_offset<uint8_t, 23>(p) = static_cast<uint8_t>(compressor2);

  // Set filter3
  filters_buffer_offset<uint8_t, 28>(p) = static_cast<uint8_t>(filtertype3);
  filters_buffer_offset<uint32_t, 29>(p) =
      sizeof(uint8_t) + sizeof(int32_t);  // metadata_length
  filters_buffer_offset<uint8_t, 33>(p) = static_cast<uint8_t>(compressor3);
  filters_buffer_offset<int32_t, 34>(p) = compressor_level3;

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_filters, filters]{
      FilterPipeline::deserialize(&constbuffer, constants::format_version)};
  REQUIRE(st_filters.ok());

  CHECK(filters.value().max_chunk_size() == max_chunk_size);
  CHECK(filters.value().size() == num_filters);

  Filter* filter1 = filters.value().get_filter(0);
  CHECK(filter1->type() == filtertype1);
  int level1 = 0;
  REQUIRE(filter1->get_option(FilterOption::COMPRESSION_LEVEL, &level1).ok());
  CHECK(level1 == compressor_level1);

  Filter* filter2 = filters.value().get_filter(1);
  CHECK(filter2->type() == filtertype2);

  Filter* filter3 = filters.value().get_filter(2);
  CHECK(filter3->type() == filtertype3);
  int level3 = 0;
  REQUIRE(filter3->get_option(FilterOption::COMPRESSION_LEVEL, &level3).ok());
  CHECK(level3 == compressor_level3);
}

TEST_CASE(
    "FilterPipeline: Test if filter list has a filter", "[filter-pipeline]") {
  FilterPipeline fp;
  fp.add_filter(CompressionFilter(Compressor::ZSTD, 2));
  fp.add_filter(BitWidthReductionFilter());
  fp.add_filter(CompressionFilter(Compressor::RLE, 1));
  fp.add_filter(CompressionFilter(Compressor::LZ4, 1));

  // Check that filters are searched correctly
  CHECK(fp.has_filter(FilterType::FILTER_RLE));
  CHECK(fp.has_filter(FilterType::FILTER_BIT_WIDTH_REDUCTION));
  CHECK_FALSE(fp.has_filter(FilterType::FILTER_GZIP));
  CHECK_FALSE(fp.has_filter(FilterType::FILTER_BITSHUFFLE));

  // Check no error when pipeline empty
  FilterPipeline fp2;
  CHECK_FALSE(fp2.has_filter(FilterType::FILTER_RLE));
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

  bool is_var_sized = true;

  // Do not chunk the Tile for filtering if RLE is used for var-sized strings
  CHECK_FALSE(
      fp_with_rle.use_tile_chunking(is_var_sized, Datatype::STRING_ASCII));

  // Chunk in any other case
  CHECK(fp_without_rle.use_tile_chunking(is_var_sized, Datatype::STRING_ASCII));
  CHECK(fp_with_rle.use_tile_chunking(!is_var_sized, Datatype::STRING_ASCII));
  CHECK(fp_with_rle.use_tile_chunking(is_var_sized, Datatype::TIME_MS));
  CHECK(fp_with_rle.use_tile_chunking(is_var_sized, Datatype::DATETIME_AS));
  CHECK(fp_with_rle.use_tile_chunking(is_var_sized, Datatype::BLOB));
  CHECK(fp_with_rle.use_tile_chunking(is_var_sized, Datatype::INT32));
  CHECK(fp_with_rle.use_tile_chunking(is_var_sized, Datatype::FLOAT64));
}

TEST_CASE(
    "FilterPipeline: Test if offset filtering should be skipped",
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

  // Do not filter offsets if RLE is used for var-sized strings for schema
  // version >= 12
  CHECK(fp_with_rle.skip_offsets_filtering(Datatype::STRING_ASCII, 12));
  CHECK(fp_with_rle.skip_offsets_filtering(Datatype::STRING_ASCII, 13));

  // Filter offsets in any other case
  CHECK_FALSE(
      fp_without_rle.skip_offsets_filtering(Datatype::STRING_ASCII, 12));
  CHECK_FALSE(fp_with_rle.skip_offsets_filtering(Datatype::STRING_ASCII, 11));
  CHECK_FALSE(fp_with_rle.skip_offsets_filtering(Datatype::TIME_MS, 12));
  CHECK_FALSE(fp_with_rle.skip_offsets_filtering(Datatype::DATETIME_AS, 12));
  CHECK_FALSE(fp_with_rle.skip_offsets_filtering(Datatype::BLOB, 12));
  CHECK_FALSE(fp_with_rle.skip_offsets_filtering(Datatype::INT32, 12));
  CHECK_FALSE(fp_with_rle.skip_offsets_filtering(Datatype::FLOAT64, 12));
}