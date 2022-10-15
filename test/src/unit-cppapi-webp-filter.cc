/**
 * @file   unit-cppapi-webp-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the C++ API for webp filter related functions.
 */

#ifdef TILEDB_WEBP

#include <random>
#include <vector>

#include "catch.hpp"
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/filter_type.h"

using namespace tiledb;

static const std::string webp_array_name = "cpp_unit_array_webp";

TEST_CASE("C++ API: WEBP Filter", "[cppapi][filter][webp]") {
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(webp_array_name))
    vfs.remove_dir(webp_array_name);

  auto rows = Dimension::create<int>(ctx, "rows", {{1, 4}}, 4);
  auto cols = Dimension::create<int>(ctx, "cols", {{1, 4}}, 4);
  Domain domain(ctx);
  domain.add_dimensions(rows, cols);

  Filter filter(ctx, TILEDB_FILTER_WEBP);
  REQUIRE(filter.filter_type() == TILEDB_FILTER_WEBP);
  REQUIRE(
      filter.to_str(filter.filter_type()) == sm::constants::filter_webp_str);

  // Check WEBP_QUALITY option
  float quality_found;
  REQUIRE_NOTHROW(
      filter.get_option<float>(TILEDB_WEBP_QUALITY, &quality_found));
  REQUIRE(100.0f == quality_found);

  float quality_expected = 1.0f;
  REQUIRE_NOTHROW(filter.set_option(TILEDB_WEBP_QUALITY, quality_expected));
  REQUIRE_NOTHROW(
      filter.get_option<float>(TILEDB_WEBP_QUALITY, &quality_found));
  REQUIRE(quality_expected == quality_found);

  // Check WEBP_INPUT_FORMAT option
  uint8_t format_found;
  REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_INPUT_FORMAT, &format_found));
  REQUIRE(TILEDB_WEBP_NONE == format_found);

  uint8_t format_expected = TILEDB_WEBP_RGBA;
  REQUIRE_NOTHROW(
      filter.set_option(TILEDB_WEBP_INPUT_FORMAT, &format_expected));
  REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_INPUT_FORMAT, &format_found));
  REQUIRE(format_expected == format_found);

  // Check WEBP_LOSSLESS option
  uint8_t lossless_found;
  REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_LOSSLESS, &lossless_found));
  REQUIRE(false == lossless_found);

  REQUIRE(false == lossless_found);
  uint8_t lossless_expected = 1;
  REQUIRE_NOTHROW(filter.set_option(TILEDB_WEBP_LOSSLESS, &format_expected));
  REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_LOSSLESS, &lossless_found));
  REQUIRE(lossless_expected == lossless_found);

  FilterList filterList(ctx);
  filterList.add_filter(filter);

  auto a = Attribute::create<int>(ctx, "a1");
  a.set_filter_list(filterList);

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  Array::create(webp_array_name, schema);
}

TEST_CASE("C API: WEBP Filter", "[capi][filter][webp]") {
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(nullptr, &ctx);
  tiledb_vfs_t* vfs;
  tiledb_vfs_alloc(ctx, nullptr, &vfs);
  int32_t is_dir = false;
  tiledb_vfs_is_dir(ctx, vfs, webp_array_name.c_str(), &is_dir);
  if (is_dir)
    tiledb_vfs_remove_dir(ctx, vfs, webp_array_name.c_str());

  tiledb_filter_t* filter;
  tiledb_filter_alloc(ctx, TILEDB_FILTER_WEBP, &filter);
  tiledb_filter_type_t filterType;
  tiledb_filter_get_type(ctx, filter, &filterType);
  REQUIRE(filterType == TILEDB_FILTER_WEBP);
  const char* filterStr;
  tiledb_filter_type_to_str(filterType, &filterStr);
  REQUIRE(filterStr == sm::constants::filter_webp_str);

  float expected_quality = 100.0f;
  float found_quality;
  auto status = tiledb_filter_get_option(
      ctx, filter, TILEDB_WEBP_QUALITY, &found_quality);
  REQUIRE(status == TILEDB_OK);
  REQUIRE(expected_quality == found_quality);

  expected_quality = 1.0f;
  status = tiledb_filter_set_option(
      ctx, filter, TILEDB_WEBP_QUALITY, &expected_quality);
  REQUIRE(status == TILEDB_OK);
  status = tiledb_filter_get_option(
      ctx, filter, TILEDB_WEBP_QUALITY, &found_quality);
  REQUIRE(status == TILEDB_OK);
  REQUIRE(expected_quality == found_quality);

  uint8_t expected_fmt = TILEDB_WEBP_NONE;
  uint8_t found_fmt;
  status = tiledb_filter_get_option(
      ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &found_fmt);
  REQUIRE(status == TILEDB_OK);
  REQUIRE(expected_fmt == found_fmt);

  expected_fmt = TILEDB_WEBP_RGBA;
  status = tiledb_filter_set_option(
      ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &expected_fmt);
  REQUIRE(status == TILEDB_OK);
  status = tiledb_filter_get_option(
      ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &found_fmt);
  REQUIRE(status == TILEDB_OK);
  REQUIRE(expected_fmt == found_fmt);

  uint8_t expected_lossless = 0;
  uint8_t found_lossless;
  status = tiledb_filter_get_option(
      ctx, filter, TILEDB_WEBP_LOSSLESS, &found_lossless);
  REQUIRE(status == TILEDB_OK);
  REQUIRE(expected_lossless == found_lossless);

  expected_lossless = 1;
  status = tiledb_filter_set_option(
      ctx, filter, TILEDB_WEBP_LOSSLESS, &expected_lossless);
  REQUIRE(status == TILEDB_OK);
  status = tiledb_filter_get_option(
      ctx, filter, TILEDB_WEBP_LOSSLESS, &found_lossless);
  REQUIRE(status == TILEDB_OK);
  REQUIRE(expected_lossless == found_lossless);
}

#endif  // TILEDB_WEBP
