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

#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/filter_type.h"

// For optional visual verification that images appear as expected
#ifdef PNG_FOUND
#include <png.h>
void write_image(
    const std::vector<uint8_t>& data,
    int width,
    int height,
    uint8_t depth,
    uint8_t colorspace) {
  std::vector<uint8_t*> png_buffer;
  for (int y = 0; y < height; y++)
    png_buffer.push_back(
        (uint8_t*)std::malloc(width * depth * sizeof(uint8_t)));

  int row_stride = width * depth;
  for (int y = 0; y < height; y++) {
    uint8_t* row = png_buffer[y];
    for (int x = 0; x < width * depth; x++) {
      row[x] = data[(y * row_stride) + x];
    }
  }

  // The test images will overwrite one another to avoid creating a gallery
  FILE* fp = fopen("cpp_unit_array_webp.png", "wb");
  if (!fp)
    abort();

  png_structp png =
      png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
  if (!png)
    abort();

  png_infop info = png_create_info_struct(png);
  if (!info)
    abort();

  if (setjmp(png_jmpbuf(png)))
    abort();

  png_init_io(png, fp);

  int color_type =
      colorspace < TILEDB_WEBP_RGBA ? PNG_COLOR_TYPE_RGB : PNG_COLOR_TYPE_RGBA;
  // libpng doesn't provide PNG_COLOR_TYPE_BGR(A)
  png_set_IHDR(
      png,
      info,
      width,
      height,
      8,
      color_type,
      PNG_INTERLACE_NONE,
      PNG_COMPRESSION_TYPE_DEFAULT,
      PNG_FILTER_TYPE_DEFAULT);
  png_write_info(png, info);

  png_write_image(png, png_buffer.data());
  png_write_end(png, NULL);

  fclose(fp);
  png_destroy_write_struct(&png, &info);
}
#endif  // PNG_FOUND

using namespace tiledb;

static const std::string webp_array_name = "cpp_unit_array_webp";

TEST_CASE("C++ API: WEBP Filter", "[cppapi][filter][webp]") {
  Context ctx;
  VFS vfs(ctx);
  if (vfs.is_dir(webp_array_name))
    vfs.remove_dir(webp_array_name);

  uint8_t format_expected = GENERATE(
      TILEDB_WEBP_RGB, TILEDB_WEBP_RGBA, TILEDB_WEBP_BGR, TILEDB_WEBP_BGRA);

  // Size of test image is 40x40
  unsigned size = 40;
  unsigned pixel_depth = format_expected < TILEDB_WEBP_RGBA ? 3 : 4;
  auto y = Dimension::create<unsigned>(ctx, "y", {{1, size}}, size / 2);
  auto x = Dimension::create<unsigned>(
      ctx, "x", {{1, size * pixel_depth}}, (size / 2) * pixel_depth);
  Domain domain(ctx);
  domain.add_dimensions(y, x);

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

  REQUIRE_NOTHROW(
      filter.set_option(TILEDB_WEBP_INPUT_FORMAT, &format_expected));
  REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_INPUT_FORMAT, &format_found));
  REQUIRE(format_expected == format_found);

  // Check WEBP_LOSSLESS option
  uint8_t lossless_found;
  REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_LOSSLESS, &lossless_found));
  REQUIRE(0 == lossless_found);

  uint8_t lossless_expected = GENERATE(1, 0);
  REQUIRE_NOTHROW(filter.set_option(TILEDB_WEBP_LOSSLESS, &lossless_expected));
  REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_LOSSLESS, &lossless_found));
  REQUIRE(lossless_expected == lossless_found);

  FilterList filterList(ctx);
  filterList.add_filter(filter);

  // This attribute is used for all colorspace formats: RGB, RGBA, BGR, BGRA
  auto a = Attribute::create<uint8_t>(ctx, "rgb");
  a.set_filter_list(filterList);

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  Array::create(webp_array_name, schema);

  // Construct data for 40x40 test image
  // + Each quadrant of the image will be solid R, G, B, or W
  // + Draw a black border between quadrants
  // This vector is used for all colorspace formats: RGB, RGBA, BGR, BGRA
  std::vector<uint8_t> rgb(size * (size * pixel_depth), 0);
  // Size of test image, midpoint between quadrants
  unsigned mid = size / 2;
  int stride = size * pixel_depth;
  for (unsigned row = 0; row < size; row++) {
    for (unsigned col = 0; col < size; col++) {
      int pos = (stride * row) + (col * pixel_depth);
      if (row < mid && col < mid) {
        // Red (Blue) top-left
        rgb[pos] = 255;
      } else if (row < mid && col > mid) {
        // Green top-right
        rgb[pos + 1] = 255;
      } else if (row > mid && col < mid) {
        // Blue (Red) bottom-left
        rgb[pos + 2] = 255;
      } else if (row > mid && col > mid) {
        // White bottom-right
        rgb[pos] = 255;
        rgb[pos + 1] = 255;
        rgb[pos + 2] = 255;
      } else if (row == mid || col == mid) {
        // Black cell border; vector elements already initialized to 0
      }

      // Add an alpha value for RGBA / BGRA
      if (pixel_depth > 3) {
        rgb[pos + 3] = 255;
      }
    }
  }

  // Write pixel data to the array
  Array array(ctx, webp_array_name, TILEDB_WRITE);
  Query write(ctx, array);
  write.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("rgb", rgb);
  write.submit();
  array.close();
  REQUIRE(Query::Status::COMPLETE == write.query_status());

  array.open(TILEDB_READ);
  std::vector<uint8_t> read_rgb((size * pixel_depth) * size);
  std::vector<unsigned> subarray = {1, size, 1, (size * pixel_depth)};
  Query read(ctx, array);
  read.set_layout(TILEDB_ROW_MAJOR)
      .set_subarray(subarray)
      .set_data_buffer("rgb", read_rgb);
  read.submit();
  array.close();
  REQUIRE(Query::Status::COMPLETE == read.query_status());

  if (lossless_expected == 1) {
    // Lossless compression should be exact
    REQUIRE_THAT(read_rgb, Catch::Matchers::Equals(rgb));
  } else {
    // Lossy compression at 100.0f quality should be approx

    // Would something like this work?
    // TODO: REQUIRE_THAT(read_rgb, Catch::Matchers::Approx(rgb));
  }

  write_image(read_rgb, size, size, pixel_depth, format_expected);
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
