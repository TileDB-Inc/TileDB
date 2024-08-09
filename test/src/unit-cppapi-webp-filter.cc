/**
 * @file unit-cppapi-webp-filter.cc
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

#include <random>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/webp_filter.h"

// For optional visual verification that images appear as expected.
#ifdef PNG_FOUND
#include <png.h>

void write_image(
    const std::vector<uint8_t>& data,
    unsigned width,
    unsigned height,
    uint8_t depth,
    uint8_t colorspace,
    const char* path) {
  std::vector<uint8_t*> png_buffer;
  for (unsigned y = 0; y < height; y++)
    png_buffer.push_back(
        (uint8_t*)std::malloc(width * depth * sizeof(uint8_t)));

  unsigned row_stride = width * depth;
  for (size_t y = 0; y < height; y++) {
    uint8_t* row = png_buffer[y];
    for (size_t x = 0; x < width * depth; x++) {
      row[x] = data[(y * row_stride) + x];
    }
  }

  // The test images will overwrite one another to avoid creating a gallery.
  FILE* fp = fopen(path != nullptr ? path : "cpp_unit_webp.png", "wb");
  if (!fp)
    abort();

  png_structp png =
      png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
  if (!png)
    abort();

  png_infop info = png_create_info_struct(png);
  if (!info)
    abort();

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4611)  // "interaction between '_setjmp' and C++
                                 // object destruction is non-portable"
#endif
  if (setjmp(png_jmpbuf(png)))
    abort();
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

  png_init_io(png, fp);

  int color_type =
      colorspace < TILEDB_WEBP_RGBA ? PNG_COLOR_TYPE_RGB : PNG_COLOR_TYPE_RGBA;

  // Enable BGR(A) format
  if (colorspace == TILEDB_WEBP_BGR || colorspace == TILEDB_WEBP_BGRA) {
    png_set_bgr(png);
  }

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

  for (size_t i = 0; i < height; i++)
    std::free(png_buffer[i]);

  fclose(fp);
  png_destroy_write_struct(&png, &info);
}

constexpr bool png_found = true;
#else
void write_image(
    const std::vector<uint8_t>& data,
    unsigned width,
    unsigned height,
    uint8_t depth,
    uint8_t colorspace,
    const char* path);

constexpr bool png_found = false;
#endif  // PNG_FOUND

using namespace tiledb;

static const std::string webp_array_name = "cpp_unit_array_webp";

std::vector<uint8_t> create_image(
    size_t width, size_t height, uint8_t pixel_depth) {
  // Construct data for test image.
  // + Each quadrant of the image will be solid R, G, B, or W.
  // + Draw a black border between quadrants.
  // This vector is used for all colorspace formats: RGB, RGBA, BGR, BGRA.
  std::vector<uint8_t> rgb(height * (width * pixel_depth), 0);
  size_t mid_y = height / 2;
  size_t mid_x = width / 2;
  size_t stride = width * pixel_depth;
  for (size_t row = 0; row < height; row++) {
    for (size_t col = 0; col < width; col++) {
      size_t pos = (stride * row) + (col * pixel_depth);
      if (row < mid_y && col < mid_x) {
        // Red (Blue) top-left.
        rgb[pos] = 255;
      } else if (row < mid_y && col > mid_x) {
        // Green top-right.
        rgb[pos + 1] = 255;
      } else if (row > mid_y && col < mid_x) {
        // Blue (Red) bottom-left.
        rgb[pos + 2] = 255;
      } else if (row > mid_y && col > mid_x) {
        // White bottom-right.
        rgb[pos] = 255;
        rgb[pos + 1] = 255;
        rgb[pos + 2] = 255;
      } else if (row == mid_y || col == mid_x) {
        // Black cell border; vector elements already initialized to 0.
      }

      // Add an alpha value for RGBA / BGRA.
      if (pixel_depth > 3) {
        rgb[pos + 3] = 255;
      }
    }
  }
  return rgb;
}

// These templates will not be used if built with TILEDB_WEBP=OFF
template <typename T>
[[maybe_unused]] Domain create_domain(const Context& ctx, uint8_t format) {
  T height = GENERATE(131, 217);
  T width = GENERATE(103, 277);
  uint8_t pixel_depth = format < TILEDB_WEBP_RGBA ? 3 : 4;
  auto y = Dimension::create<T>(ctx, "y", {{1, height}}, height / 2);
  auto x = Dimension::create<T>(
      ctx, "x", {{1, (T)(width * pixel_depth)}}, (width / 2) * pixel_depth);
  Domain domain(ctx);
  domain.add_dimensions(y, x);
  return domain;
}

template <>
[[maybe_unused]] Domain create_domain<int8_t>(
    const Context& ctx, uint8_t format) {
  int8_t height = GENERATE(9, 11, 15);
  int8_t width = GENERATE(5, 7, 9, 17);
  uint8_t pixel_depth = format < TILEDB_WEBP_RGBA ? 3 : 4;

  int8_t w = width * pixel_depth;
  auto y = Dimension::create<int8_t>(ctx, "y", {{1, height}}, height / 2);
  auto x =
      Dimension::create<int8_t>(ctx, "x", {{1, w}}, (width / 2) * pixel_depth);
  Domain domain(ctx);
  domain.add_dimensions(y, x);
  return domain;
}

template <>
[[maybe_unused]] Domain create_domain<uint8_t>(
    const Context& ctx, uint8_t format) {
  uint8_t height = GENERATE(13, 35, 47, 61);
  uint8_t width = GENERATE(10, 11, 23, 39, 60);
  uint8_t pixel_depth = format < TILEDB_WEBP_RGBA ? 3 : 4;
  auto y = Dimension::create<uint8_t>(ctx, "y", {{1, height}}, height / 2);
  auto x = Dimension::create<uint8_t>(
      ctx,
      "x",
      {{1, (uint8_t)(width * pixel_depth)}},
      (width / 2) * pixel_depth);
  Domain domain(ctx);
  domain.add_dimensions(y, x);
  return domain;
}

using TestTypes = std::tuple<uint16_t, int16_t, int32_t, int64_t, uint32_t>;
TEMPLATE_LIST_TEST_CASE(
    "C++ API: WEBP filter schema validation",
    "[cppapi][filter][webp]",
    TestTypes) {
  if constexpr (webp_filter_exists) {
    Context ctx;
    VFS vfs(ctx);
    if (vfs.is_dir(webp_array_name)) {
      vfs.remove_dir(webp_array_name);
    }
    Filter filter(ctx, TILEDB_FILTER_WEBP);
    FilterList filterList(ctx);
    filterList.add_filter(filter);

    // Create valid attribute, domain, and schema.
    auto valid_attr = Attribute::create<uint8_t>(ctx, "rgb");
    valid_attr.set_filter_list(filterList);

    Domain valid_domain(ctx);
    valid_domain.add_dimension(
        Dimension::create<uint64_t>(ctx, "y", {{1, 100}}, 90));
    valid_domain.add_dimension(
        Dimension::create<uint64_t>(ctx, "x", {{1, 100}}, 90));

    ArraySchema valid_schema(ctx, TILEDB_DENSE);
    valid_schema.set_domain(valid_domain);
    valid_schema.add_attribute(valid_attr);

    // Create an invalid attribute for use with WebP filter.
    auto invalid_attr = Attribute::create<TestType>(ctx, "rgb");
    REQUIRE_THROWS_WITH(
        invalid_attr.set_filter_list(filterList),
        Catch::Matchers::ContainsSubstring(
            "Filter WEBP does not accept input type"));

    // WebP filter requires exactly 2 dimensions for Y, X.
    {
      Domain invalid_domain(ctx);
      invalid_domain.add_dimension(
          Dimension::create<uint64_t>(ctx, "y", {{1, 100}}, 90));

      // Test with < 2 dimensions.
      ArraySchema invalid_schema(ctx, TILEDB_DENSE);
      invalid_schema.set_domain(invalid_domain);
      invalid_schema.add_attribute(valid_attr);
      REQUIRE_THROWS_WITH(
          Array::create(webp_array_name, invalid_schema),
          Catch::Matchers::ContainsSubstring(
              "WebP filter requires exactly 2 dimensions Y, X"));

      // Test with > 2 dimensions.
      invalid_domain.add_dimensions(
          Dimension::create<uint64_t>(ctx, "x", {{1, 100}}, 90),
          Dimension::create<uint64_t>(ctx, "z", {{1, 100}}, 90));
      invalid_schema.set_domain(invalid_domain);
      REQUIRE_THROWS_WITH(
          Array::create(webp_array_name, invalid_schema),
          Catch::Matchers::ContainsSubstring(
              "WebP filter requires exactly 2 dimensions Y, X"));
    }

    // In dense arrays, all dimensions must have matching datatype.
    {
      Domain invalid_domain(ctx);
      invalid_domain.add_dimension(
          Dimension::create<uint64_t>(ctx, "y", {{1, 100}}, 90));
      invalid_domain.add_dimension(
          Dimension::create<TestType>(ctx, "x", {{1, 100}}, 90));

      ArraySchema invalid_schema(ctx, TILEDB_DENSE);

      // This is also enforced by ArraySchema::check_webp_filter.
      REQUIRE_THROWS_WITH(
          invalid_schema.set_domain(invalid_domain),
          Catch::Matchers::ContainsSubstring(
              "In dense arrays, all dimensions must have the same datatype"));
    }

    // WebP filter can only be applied to dense arrays.
    {
      ArraySchema invalid_schema(ctx, TILEDB_SPARSE);
      invalid_schema.set_domain(valid_domain);
      invalid_schema.add_attribute(valid_attr);

      REQUIRE_THROWS_WITH(
          Array::create(webp_array_name, invalid_schema),
          Catch::Matchers::ContainsSubstring(
              "WebP filter can only be applied to dense arrays"));
    }

    Array::create(webp_array_name, valid_schema);

    if (vfs.is_dir(webp_array_name)) {
      vfs.remove_dir(webp_array_name);
    }
  }
}

using DimensionTypes = std::tuple<
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t>;
TEMPLATE_LIST_TEST_CASE(
    "C++ API: WEBP Filter",
    "[cppapi][filter][webp][longtest]",
    DimensionTypes) {
  if constexpr (webp_filter_exists) {
    Context ctx;
    VFS vfs(ctx);
    if (vfs.is_dir(webp_array_name)) {
      vfs.remove_dir(webp_array_name);
    }

    uint8_t format_expected = GENERATE(
        TILEDB_WEBP_RGB, TILEDB_WEBP_RGBA, TILEDB_WEBP_BGR, TILEDB_WEBP_BGRA);
    uint8_t lossless_expected = GENERATE(1, 0);

    Filter filter(ctx, TILEDB_FILTER_WEBP);
    REQUIRE(filter.filter_type() == TILEDB_FILTER_WEBP);
    REQUIRE(
        filter.to_str(filter.filter_type()) == sm::constants::filter_webp_str);

    // Check WEBP_QUALITY option.
    float quality_found = 0;

    REQUIRE_NOTHROW(
        filter.get_option<float>(TILEDB_WEBP_QUALITY, &quality_found));
    REQUIRE(100.0f == quality_found);
    REQUIRE(quality_found == filter.get_option<float>(TILEDB_WEBP_QUALITY));

    float quality_expected = 1.0f;
    REQUIRE_NOTHROW(filter.set_option(TILEDB_WEBP_QUALITY, quality_expected));
    REQUIRE_NOTHROW(
        filter.get_option<float>(TILEDB_WEBP_QUALITY, &quality_found));
    REQUIRE(quality_expected == quality_found);
    REQUIRE(quality_found == filter.get_option<float>(TILEDB_WEBP_QUALITY));

    // Set invalid options for TILEDB_WEBP_QUALITY.
    REQUIRE_THROWS(filter.set_option(TILEDB_WEBP_QUALITY, -1.0f));
    REQUIRE_THROWS(filter.set_option(TILEDB_WEBP_QUALITY, 101.0f));

    // Set lossy quality back to 100 to test highest quality lossy compression.
    REQUIRE_NOTHROW(filter.set_option(TILEDB_WEBP_QUALITY, 100.0f));
    REQUIRE_NOTHROW(
        filter.get_option<float>(TILEDB_WEBP_QUALITY, &quality_found));
    REQUIRE(100.0f == quality_found);
    REQUIRE(quality_found == filter.get_option<float>(TILEDB_WEBP_QUALITY));

    // Check WEBP_INPUT_FORMAT option.
    uint8_t format_found = 0;
    REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_INPUT_FORMAT, &format_found));
    REQUIRE(TILEDB_WEBP_NONE == format_found);
    REQUIRE(
        format_found == filter.get_option<uint8_t>(TILEDB_WEBP_INPUT_FORMAT));
    REQUIRE(
        (tiledb_filter_webp_format_t)format_found ==
        filter.get_option<tiledb_filter_webp_format_t>(
            TILEDB_WEBP_INPUT_FORMAT));

    // Set invalid option for WEBP_INPUT_FORMAT.
    REQUIRE_THROWS(filter.set_option(TILEDB_WEBP_INPUT_FORMAT, (uint8_t)255));

    REQUIRE_NOTHROW(
        filter.set_option(TILEDB_WEBP_INPUT_FORMAT, &format_expected));
    REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_INPUT_FORMAT, &format_found));
    REQUIRE(format_expected == format_found);
    REQUIRE(
        format_found == filter.get_option<uint8_t>(TILEDB_WEBP_INPUT_FORMAT));

    // Check WEBP_LOSSLESS option.
    uint8_t lossless_found = 0;
    REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_LOSSLESS, &lossless_found));
    REQUIRE(0 == lossless_found);
    REQUIRE(lossless_found == filter.get_option<uint8_t>(TILEDB_WEBP_LOSSLESS));

    REQUIRE_THROWS(filter.set_option(TILEDB_WEBP_LOSSLESS, (uint8_t)2));

    REQUIRE_NOTHROW(
        filter.set_option<uint8_t>(TILEDB_WEBP_LOSSLESS, lossless_expected));
    REQUIRE_NOTHROW(filter.get_option(TILEDB_WEBP_LOSSLESS, &lossless_found));
    REQUIRE(lossless_expected == lossless_found);
    REQUIRE(lossless_found == filter.get_option<uint8_t>(TILEDB_WEBP_LOSSLESS));

    // Test against images of different sizes.
    Domain domain = create_domain<TestType>(ctx, format_expected);
    uint8_t pixel_depth = format_expected < TILEDB_WEBP_RGBA ? 3 : 4;
    TestType height = domain.dimension(0).template domain<TestType>().second;
    TestType width =
        domain.dimension(1).template domain<TestType>().second / pixel_depth;

    FilterList filterList(ctx);
    filterList.add_filter(filter);

    // This attribute is used for all colorspace formats: RGB, RGBA, BGR, BGRA.
    auto a = Attribute::create<uint8_t>(ctx, "rgb");
    a.set_filter_list(filterList);

    ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain);
    schema.add_attribute(a);
    Array::create(webp_array_name, schema);

    auto rgb = create_image(width, height, pixel_depth);

    // Write pixel data to the array.
    Array array(ctx, webp_array_name, TILEDB_WRITE);
    Query write(ctx, array);
    write.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("rgb", rgb);
    write.submit();
    array.close();
    REQUIRE(Query::Status::COMPLETE == write.query_status());

    array.open(TILEDB_READ);
    std::vector<uint8_t> read_rgb((width * pixel_depth) * height);
    std::vector<TestType> subarray = {
        1, height, 1, (TestType)(width * pixel_depth)};
    Query read(ctx, array);
    read.set_layout(TILEDB_ROW_MAJOR)
        .set_subarray(Subarray(ctx, array).set_subarray(subarray))
        .set_data_buffer("rgb", read_rgb);
    read.submit();
    array.close();
    REQUIRE(Query::Status::COMPLETE == read.query_status());

    if (lossless_expected == 1) {
      // Lossless compression should be exact.
      REQUIRE_THAT(read_rgb, Catch::Matchers::Equals(rgb));
    } else {
      // Lossy compression at 100.0f quality should be approx.
      REQUIRE_THAT(read_rgb, Catch::Matchers::Approx(rgb).margin(200));
    }

    if constexpr (png_found) {
      write_image(
          read_rgb, width, height, pixel_depth, format_expected, nullptr);
    }

    if (vfs.is_dir(webp_array_name)) {
      vfs.remove_dir(webp_array_name);
    }
  }
}

TEST_CASE("C API: WEBP Filter", "[capi][filter][webp]") {
  if constexpr (webp_filter_exists) {
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(nullptr, &ctx);
    tiledb_vfs_t* vfs;
    tiledb_vfs_alloc(ctx, nullptr, &vfs);
    int32_t is_dir = false;
    tiledb_vfs_is_dir(ctx, vfs, webp_array_name.c_str(), &is_dir);
    if (is_dir) {
      tiledb_vfs_remove_dir(ctx, vfs, webp_array_name.c_str());
    }

    uint8_t expected_lossless = GENERATE(1, 0);
    uint8_t expected_fmt = GENERATE(
        TILEDB_WEBP_RGB, TILEDB_WEBP_RGBA, TILEDB_WEBP_BGR, TILEDB_WEBP_BGRA);

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

    // Set lossy quality back to 100 to test highest quality lossy compression.
    expected_quality = 100.0f;
    status = tiledb_filter_set_option(
        ctx, filter, TILEDB_WEBP_QUALITY, &expected_quality);
    REQUIRE(status == TILEDB_OK);
    status = tiledb_filter_get_option(
        ctx, filter, TILEDB_WEBP_QUALITY, &found_quality);
    REQUIRE(status == TILEDB_OK);
    REQUIRE(expected_quality == found_quality);

    uint8_t found_fmt;
    status = tiledb_filter_get_option(
        ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &found_fmt);
    REQUIRE(status == TILEDB_OK);
    REQUIRE(TILEDB_WEBP_NONE == found_fmt);

    tiledb_filter_webp_format_t set_fmt{};
    REQUIRE(
        tiledb_filter_set_option(
            ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &set_fmt) == TILEDB_OK);
    tiledb_filter_webp_format_t get_fmt{};
    REQUIRE(
        tiledb_filter_get_option(
            ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &get_fmt) == TILEDB_OK);
    REQUIRE(set_fmt == get_fmt);

    status = tiledb_filter_set_option(
        ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &expected_fmt);
    REQUIRE(status == TILEDB_OK);
    status = tiledb_filter_get_option(
        ctx, filter, TILEDB_WEBP_INPUT_FORMAT, &found_fmt);
    REQUIRE(status == TILEDB_OK);
    REQUIRE(expected_fmt == found_fmt);

    uint8_t found_lossless;
    status = tiledb_filter_get_option(
        ctx, filter, TILEDB_WEBP_LOSSLESS, &found_lossless);
    REQUIRE(status == TILEDB_OK);
    REQUIRE(0 == found_lossless);

    status = tiledb_filter_set_option(
        ctx, filter, TILEDB_WEBP_LOSSLESS, &expected_lossless);
    REQUIRE(status == TILEDB_OK);
    status = tiledb_filter_get_option(
        ctx, filter, TILEDB_WEBP_LOSSLESS, &found_lossless);
    REQUIRE(status == TILEDB_OK);
    REQUIRE(expected_lossless == found_lossless);

    // Size of test images are 40x40, 40x20, 40x80.
    unsigned height = 40;
    unsigned width = GENERATE(40, 20, 80);
    unsigned pixel_depth = expected_fmt < TILEDB_WEBP_RGBA ? 3 : 4;
    auto rgb = create_image(width, height, pixel_depth);

    tiledb_filter_list_t* filter_list;
    tiledb_filter_list_alloc(ctx, &filter_list);
    tiledb_filter_list_add_filter(ctx, filter_list, filter);

    unsigned bounds[] = {1, height, 1, (width * pixel_depth)};
    unsigned extents[] = {height / 2, (width / 2) * pixel_depth};
    tiledb_dimension_t* y;
    tiledb_dimension_alloc(ctx, "y", TILEDB_INT32, &bounds[0], &extents[0], &y);
    tiledb_dimension_t* x;
    tiledb_dimension_alloc(ctx, "x", TILEDB_INT32, &bounds[2], &extents[1], &x);

    tiledb_domain_t* domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, y);
    tiledb_domain_add_dimension(ctx, domain, x);

    tiledb_attribute_t* a;
    tiledb_attribute_alloc(ctx, "rgb", TILEDB_UINT8, &a);
    tiledb_attribute_set_filter_list(ctx, a, filter_list);

    tiledb_array_schema_t* schema;
    tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &schema);
    tiledb_array_schema_set_cell_order(ctx, schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_tile_order(ctx, schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_domain(ctx, schema, domain);
    tiledb_array_schema_add_attribute(ctx, schema, a);

    tiledb_array_create(ctx, webp_array_name.c_str(), schema);

    tiledb_filter_free(&filter);
    tiledb_filter_list_free(&filter_list);
    tiledb_attribute_free(&a);
    tiledb_dimension_free(&y);
    tiledb_dimension_free(&x);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&schema);

    // Write to array.
    uint64_t data_size = rgb.size() * sizeof(uint8_t);
    {
      tiledb_array_t* array;
      tiledb_array_alloc(ctx, webp_array_name.c_str(), &array);
      tiledb_array_open(ctx, array, TILEDB_WRITE);
      tiledb_query_t* write;
      tiledb_query_alloc(ctx, array, TILEDB_WRITE, &write);
      tiledb_query_set_layout(ctx, write, TILEDB_ROW_MAJOR);
      tiledb_query_set_data_buffer(ctx, write, "rgb", rgb.data(), &data_size);
      tiledb_query_submit(ctx, write);
      tiledb_array_close(ctx, array);

      tiledb_array_free(&array);
      tiledb_query_free(&write);
    }

    // Read from array.
    std::vector<uint8_t> read_rgb(data_size);
    {
      tiledb_array_t* array;
      tiledb_array_alloc(ctx, webp_array_name.c_str(), &array);
      tiledb_array_open(ctx, array, TILEDB_READ);
      tiledb_query_t* read;
      tiledb_query_alloc(ctx, array, TILEDB_READ, &read);
      tiledb_query_set_layout(ctx, read, TILEDB_ROW_MAJOR);
      unsigned sub[] = {1, height, 1, (width * pixel_depth)};
      tiledb_subarray_t* subarray;
      tiledb_subarray_alloc(ctx, array, &subarray);
      tiledb_subarray_set_subarray(ctx, subarray, &sub);
      tiledb_query_set_subarray_t(ctx, read, subarray);
      tiledb_query_set_data_buffer(
          ctx, read, "rgb", read_rgb.data(), &data_size);
      tiledb_query_submit(ctx, read);
      tiledb_array_close(ctx, array);

      tiledb_array_free(&array);
      tiledb_query_free(&read);
      tiledb_subarray_free(&subarray);
    }

    if (expected_lossless == 1) {
      // Lossless compression should be exact.
      REQUIRE_THAT(read_rgb, Catch::Matchers::Equals(rgb));
    } else {
      // Lossy compression at 100.0f quality should be approx.
      REQUIRE_THAT(read_rgb, Catch::Matchers::Approx(rgb).margin(100));
    }

    if constexpr (png_found) {
      write_image(read_rgb, width, height, pixel_depth, expected_fmt, nullptr);
    }

    tiledb_vfs_is_dir(ctx, vfs, webp_array_name.c_str(), &is_dir);
    if (is_dir) {
      tiledb_vfs_remove_dir(ctx, vfs, webp_array_name.c_str());
    }
  }
}
