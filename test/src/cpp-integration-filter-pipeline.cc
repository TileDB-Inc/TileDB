/**
 * @file cpp-integration-filter-pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the `FilterPipeline` class.
 */

#include "test/support/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/stdx/utility/to_underlying.h"

#include <test/support/tdb_catch.h>
#include <functional>
#include <random>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE(
    "C++ API: Pipeline with filtered type conversions",
    "[cppapi][filter][pipeline]") {
  tiledb::Context& ctx = vanilla_context_cpp();
  tiledb::VFS vfs(ctx);
  std::string array_name = "cpp_test_array";
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  tiledb::Domain domain(ctx);
  float domain_lo = static_cast<float>(std::numeric_limits<int64_t>::min());
  float domain_hi = static_cast<float>(std::numeric_limits<int64_t>::max() - 1);

  // Create and initialize dimension.
  auto d1 = tiledb::Dimension::create<float>(
      ctx, "d1", {{domain_lo, domain_hi}}, 2048);

  tiledb::Filter float_scale(ctx, TILEDB_FILTER_SCALE_FLOAT);
  double scale = 1.0f;
  double offset = 0.0f;
  uint64_t byte_width = sizeof(int32_t);

  // Float scale converting tile data from float->int32
  float_scale.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  float_scale.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  float_scale.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  // Delta filter reinterprets int32->uint32
  tiledb::Filter delta(ctx, TILEDB_FILTER_DELTA);

  // Pass uint32 data to BitWidthReduction filter
  tiledb::Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);

  tiledb::FilterList filters(ctx);
  filters.add_filter(float_scale);
  filters.add_filter(delta);
  filters.add_filter(bit_width_reduction);

  // Apply filters to both attribute and dimension.
  REQUIRE_NOTHROW(d1.set_filter_list(filters));
  domain.add_dimension(d1);

  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");
  REQUIRE_NOTHROW(a1.set_filter_list(filters));

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  REQUIRE_NOTHROW(tiledb::Array::create(array_name, schema));
  std::vector<float> d1_data = {
      1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
  std::vector<float> a1_data = {
      1.0f, 2.1f, 3.2f, 4.3f, 5.4f, 6.5f, 7.6f, 8.7f, 9.8f, 10.9f};

  // Write to array.
  {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_data_buffer("d1", d1_data);
    query.set_data_buffer("a1", a1_data);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
  }

  // Read from array.
  {
    std::vector<float> d1_read(10);
    std::vector<float> a1_read(10);
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    tiledb::Subarray sub(ctx, array);
    sub.set_subarray({domain_lo, domain_hi});
    query.set_subarray(sub);
    query.set_data_buffer("a1", a1_read);
    query.set_data_buffer("d1", d1_read);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
    // Some loss of precision from rounding in FloatScale.
    CHECK(
        std::vector<float>{
            1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f} ==
        a1_read);
    CHECK(d1_data == d1_read);
  }

  // Cleanup.
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "C++ API: Filter pipeline validation",
    "[cppapi][filter][pipeline][validation]") {
  tiledb::Context& ctx = vanilla_context_cpp();

  tiledb::Domain domain(ctx);
  float domain_lo = static_cast<float>(std::numeric_limits<int64_t>::min());
  float domain_hi = static_cast<float>(std::numeric_limits<int64_t>::max() - 1);
  auto d1 = tiledb::Dimension::create<float>(
      ctx, "d1", {{domain_lo, domain_hi}}, 2048);
  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");

  // FloatScale used for testing different float->integral pipelines.
  tiledb::Filter float_scale(ctx, TILEDB_FILTER_SCALE_FLOAT);
  double scale = 1.0f;
  double offset = 0.0f;
  uint64_t byte_width = sizeof(int32_t);
  // Float scale converting tile data from float->int32
  float_scale.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  float_scale.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  float_scale.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  tiledb::FilterList filters(ctx);
  SECTION("- FloatScale filter accepts float or double byte width input") {
    auto d2 = tiledb::Dimension::create<int8_t>(ctx, "d2", {{1, 100}}, 10);
    auto a2 = tiledb::Attribute::create<int32_t>(ctx, "a2");
    filters.add_filter(float_scale);
    CHECK_THROWS(d2.set_filter_list(filters));
    CHECK_NOTHROW(a2.set_filter_list(filters));
  }

  SECTION("- Delta filters do not accept real datatypes") {
    auto test_filter = GENERATE(
        TILEDB_FILTER_POSITIVE_DELTA,
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA);
    tiledb::Filter delta_filter(ctx, test_filter);
    filters.add_filter(delta_filter);
    // Delta compressors don't accept floats. Should fail without FloatScale.
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));

    // Test using FloatScale to convert to integral is accepted.
    tiledb::FilterList filters2(ctx);
    filters2.add_filter(float_scale);
    filters2.add_filter(delta_filter);
    CHECK_NOTHROW(d1.set_filter_list(filters2));
    CHECK_NOTHROW(a1.set_filter_list(filters2));
  }

  SECTION("- Webp filter supports only uint8 attributes") {
    if (webp_filter_exists) {
      tiledb::Filter webp(ctx, TILEDB_FILTER_WEBP);
      filters.add_filter(webp);
      CHECK_THROWS(d1.set_filter_list(filters));
      CHECK_THROWS(a1.set_filter_list(filters));
    }
  }

  SECTION("- Bit width reduction filter supports integral input") {
    tiledb::Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);
    filters.add_filter(bit_width_reduction);
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));

    // Test using FloatScale to convert to integral is accepted.
    tiledb::FilterList filters2(ctx);
    filters2.add_filter(float_scale);
    filters2.add_filter(bit_width_reduction);
    CHECK_NOTHROW(d1.set_filter_list(filters2));
    CHECK_NOTHROW(a1.set_filter_list(filters2));
  }

  SECTION("- XOR filter interprets datatype as integral") {
    // Datatype byte size must match size of int8, int16, int32, or int64
    tiledb::Filter xor_filter(ctx, TILEDB_FILTER_XOR);
    filters.add_filter(xor_filter);
    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));
  }

  SECTION("- Multiple compressors") {
    tiledb::Filter bzip(ctx, TILEDB_FILTER_BZIP2);
    auto compressor = GENERATE(
        TILEDB_FILTER_GZIP,
        TILEDB_FILTER_LZ4,
        TILEDB_FILTER_RLE,
        TILEDB_FILTER_ZSTD);
    tiledb::Filter compressor_filter(ctx, compressor);
    filters.add_filter(bzip);
    filters.add_filter(compressor_filter);

    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));

    // Should throw without FloatScale to convert float->int32.
    auto delta_compressor = GENERATE(
        TILEDB_FILTER_POSITIVE_DELTA,
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA);
    tiledb::Filter delta_filter(ctx, delta_compressor);
    filters.add_filter(delta_filter);
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));
  }

  SECTION("- Multiple compressors following type conversion") {
    auto compressor = GENERATE(
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA,
        TILEDB_FILTER_GZIP,
        TILEDB_FILTER_LZ4,
        TILEDB_FILTER_RLE,
        TILEDB_FILTER_ZSTD);
    tiledb::Filter compressor_filter(ctx, compressor);
    tiledb::Filter bzip(ctx, TILEDB_FILTER_BZIP2);
    filters.add_filter(float_scale);
    filters.add_filter(bzip);
    filters.add_filter(compressor_filter);

    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));
  }
}
