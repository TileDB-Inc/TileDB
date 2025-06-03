/**
 * @file tiledb/api/c_api/subarray/test/unit_capi_subarray.cc
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
 * Validates the arguments for the Subarray C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../../../c_api_test_support/testsupport_capi_subarray.h"
#include "../subarray_api_experimental.h"
#include "../subarray_api_external.h"
#include "../subarray_api_internal.h"

using namespace tiledb::api::test_support;

TEST_CASE(
    "C API: tiledb_subarray_alloc argument validation", "[capi][subarray]") {
  capi_return_t rc;
  ordinary_array x{};
  x.open();  // The array must be open.
  tiledb_subarray_handle_t* subarray{};

  SECTION("success") {
    rc = tiledb_subarray_alloc(x.ctx(), x.array, &subarray);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_subarray_free(&subarray);
    CHECK(subarray == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_alloc(nullptr, x.array, &subarray);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_subarray_alloc(x.ctx(), nullptr, &subarray);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid array") {
    x.close();
    rc = tiledb_subarray_alloc(x.ctx(), x.array, &subarray);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_alloc(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_free argument validation", "[capi][subarray]") {
  ordinary_array x{};
  x.open();  // The array must be open.
  tiledb_subarray_handle_t* subarray{};
  auto rc = tiledb_subarray_alloc(x.ctx(), x.array, &subarray);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    REQUIRE_NOTHROW(tiledb_subarray_free(&subarray));
    CHECK(subarray == nullptr);
  }
  SECTION("null subarray") {
    /*
     * `tiledb_subarray_free` is a void function, otherwise we would check
     * for an error.
     */
    REQUIRE_NOTHROW(tiledb_subarray_free(nullptr));
  }
}

TEST_CASE(
    "C API: tiledb_subarray_set_config argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  tiledb_config_handle_t* config;
  tiledb_error_handle_t* err;
  rc = tiledb_config_alloc(&config, &err);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("success") {
    rc = tiledb_subarray_set_config(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_set_config(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_set_config(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_set_config(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_set_coalesce_ranges argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  SECTION("success") {
    rc = tiledb_subarray_set_coalesce_ranges(x.ctx(), x.subarray, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_set_coalesce_ranges(nullptr, x.subarray, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_set_coalesce_ranges(x.ctx(), nullptr, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_set_subarray argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  int subarray_v[] = {1, 4};  // The domain range
  SECTION("success") {
    rc = tiledb_subarray_set_subarray(x.ctx(), x.subarray, subarray_v);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_set_subarray(nullptr, x.subarray, subarray_v);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_set_subarray(x.ctx(), nullptr, subarray_v);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid subarray_v") {
    int subarray_inv[] = {10, 20};
    rc = tiledb_subarray_set_subarray(x.ctx(), x.subarray, subarray_inv);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_add_point_ranges argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  int ranges[] = {1, 4};  // The domain range
  SECTION("success") {
    rc = tiledb_subarray_add_point_ranges(x.ctx(), x.subarray, 0, ranges, 2);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);

    // validate range_num
    uint64_t range_num;
    rc = tiledb_subarray_get_range_num(x.ctx(), x.subarray, 0, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(range_num == 2);

    // validate range
    const void* start;
    const void* end;
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 0, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(*static_cast<const int*>(start) == 1);
    CHECK(*static_cast<const int*>(end) == 1);
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 1, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(*static_cast<const int*>(start) == 4);
    CHECK(*static_cast<const int*>(end) == 4);
    // there are only two ranges
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 2, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_point_ranges(nullptr, x.subarray, 0, ranges, 2);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_point_ranges(x.ctx(), nullptr, 0, ranges, 2);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc = tiledb_subarray_add_point_ranges(x.ctx(), x.subarray, 3, ranges, 2);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid ranges") {
    int ranges_inv[] = {10, 20};
    rc =
        tiledb_subarray_add_point_ranges(x.ctx(), x.subarray, 0, ranges_inv, 2);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid count" section here;
   * There is no way to programmatically (in)validate the count. An invalid
   * value will result in a segfault from an OOB memcpy.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_add_point_ranges_var argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray_var x{};
  const char* buffer = "aabcccddee";
  uint64_t buffer_size = 10;
  uint64_t offsets[] = {0, 2, 3, 6, 8};
  uint64_t offsets_size = 5;

  // According to the data passed, the expected ranges are:
  // (aa, aa), (b, b), (ccc, ccc), (dd, dd), (ee, ee)

  SECTION("success") {
    rc = tiledb_subarray_add_point_ranges_var(
        x.ctx(), x.subarray, 0, buffer, buffer_size, offsets, offsets_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);

    // validate range_num
    uint64_t range_num;
    rc = tiledb_subarray_get_range_num(x.ctx(), x.subarray, 0, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(range_num == 5);

    // validate range
    const void* start;
    const void* end;
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 0, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(std::string(static_cast<const char*>(start), 2) == "aa");
    CHECK(std::string(static_cast<const char*>(end), 2) == "aa");
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 1, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(std::string(static_cast<const char*>(start), 1) == "b");
    CHECK(std::string(static_cast<const char*>(end), 1) == "b");
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 2, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(std::string(static_cast<const char*>(start), 3) == "ccc");
    CHECK(std::string(static_cast<const char*>(end), 3) == "ccc");
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 3, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(std::string(static_cast<const char*>(start), 2) == "dd");
    CHECK(std::string(static_cast<const char*>(end), 2) == "dd");
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 4, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(std::string(static_cast<const char*>(start), 2) == "ee");
    CHECK(std::string(static_cast<const char*>(end), 2) == "ee");
    // there are only five ranges
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 5, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_point_ranges_var(
        nullptr, x.subarray, 0, buffer, buffer_size, offsets, offsets_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_point_ranges_var(
        x.ctx(), nullptr, 0, buffer, buffer_size, offsets, offsets_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc = tiledb_subarray_add_point_ranges_var(
        x.ctx(), x.subarray, 3, buffer, buffer_size, offsets, offsets_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null buffer_val") {
    rc = tiledb_subarray_add_point_ranges_var(
        x.ctx(), x.subarray, 0, nullptr, buffer_size, offsets, offsets_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null buffer_off") {
    rc = tiledb_subarray_add_point_ranges_var(
        x.ctx(), x.subarray, 0, buffer, buffer_size, nullptr, offsets_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  /**
   * No "invalid buffer_val_size" and "invalid buffer_off_size" sections here;
   * There is no way to programmatically (in)validate the size. An invalid
   * value will result in a segfault from an OOB memcpy.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_add_range argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  int start = 1, end = 4;        // The domain range
  const void* stride = nullptr;  // Stride is not yet supported.
  SECTION("success") {
    rc =
        tiledb_subarray_add_range(x.ctx(), x.subarray, 0, &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc =
        tiledb_subarray_add_range(nullptr, x.subarray, 0, &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range(x.ctx(), nullptr, 0, &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc =
        tiledb_subarray_add_range(x.ctx(), x.subarray, 2, &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid start") {
    int start_inv = 10;
    rc = tiledb_subarray_add_range(
        x.ctx(), x.subarray, 2, &start_inv, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid end") {
    int end_inv = 20;
    rc = tiledb_subarray_add_range(
        x.ctx(), x.subarray, 2, &start, &end_inv, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid stride" section here;
   * The stride is currently unsupported. All usage resolves to `nullptr`.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_add_range_by_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  const char* dim_name = "dim";  // The dimension name
  int start = 1, end = 4;        // The domain range
  const void* stride = nullptr;  // Stride is not yet supported.
  SECTION("success") {
    rc = tiledb_subarray_add_range_by_name(
        x.ctx(), x.subarray, dim_name, &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_range_by_name(
        nullptr, x.subarray, dim_name, &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range_by_name(
        x.ctx(), nullptr, dim_name, &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_name") {
    rc = tiledb_subarray_add_range_by_name(
        x.ctx(), x.subarray, "invalid", &start, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid start") {
    int start_inv = 10;
    rc = tiledb_subarray_add_range_by_name(
        x.ctx(), x.subarray, dim_name, &start_inv, &end, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid end") {
    int end_inv = 10;
    rc = tiledb_subarray_add_range_by_name(
        x.ctx(), x.subarray, dim_name, &start, &end_inv, stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid stride" section here;
   * The stride is currently unsupported. All usage resolves to `nullptr`.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_add_range_var argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray_var x{};
  char start[] = "start", end[] = "end";
  SECTION("success") {
    rc =
        tiledb_subarray_add_range_var(x.ctx(), x.subarray, 0, start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc =
        tiledb_subarray_add_range_var(nullptr, x.subarray, 0, start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range_var(x.ctx(), nullptr, 0, start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc =
        tiledb_subarray_add_range_var(x.ctx(), x.subarray, 2, start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_subarray_add_range_var(
        x.ctx(), x.subarray, 0, nullptr, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_subarray_add_range_var(
        x.ctx(), x.subarray, 0, start, 5, nullptr, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid [start, end]_size" sections here;
   * All values, including 0 (empty range), are valid.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_add_range_var_by_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray_var x{};
  const char* dim_name = "dim";  // The dimension name
  char start[] = "start", end[] = "end";
  SECTION("success") {
    rc = tiledb_subarray_add_range_var_by_name(
        x.ctx(), x.subarray, dim_name, start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_range_var_by_name(
        nullptr, x.subarray, dim_name, start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range_var_by_name(
        x.ctx(), nullptr, dim_name, start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_name") {
    rc = tiledb_subarray_add_range_var_by_name(
        x.ctx(), x.subarray, "invalid", start, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_subarray_add_range_var_by_name(
        x.ctx(), x.subarray, dim_name, nullptr, 5, end, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_subarray_add_range_var_by_name(
        x.ctx(), x.subarray, dim_name, start, 5, nullptr, 3);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid [start, end]_size" sections here;
   * All values, including 0 (empty range), are valid.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_get_range_num argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  uint64_t range_num;
  SECTION("success") {
    rc = tiledb_subarray_get_range_num(x.ctx(), x.subarray, 0, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_num(nullptr, x.subarray, 0, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_num(x.ctx(), nullptr, 0, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc = tiledb_subarray_get_range_num(x.ctx(), x.subarray, 2, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null range_num") {
    rc = tiledb_subarray_get_range_num(x.ctx(), x.subarray, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_get_range_num_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  const char* dim_name = "dim";  // The dimension name
  uint64_t range_num;
  SECTION("success") {
    rc = tiledb_subarray_get_range_num_from_name(
        x.ctx(), x.subarray, dim_name, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_num_from_name(
        nullptr, x.subarray, dim_name, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_num_from_name(
        x.ctx(), nullptr, dim_name, &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_name") {
    rc = tiledb_subarray_get_range_num_from_name(
        x.ctx(), x.subarray, "invalid", &range_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null range_num") {
    rc = tiledb_subarray_get_range_num_from_name(
        x.ctx(), x.subarray, dim_name, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_get_range argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  const void *start, *end, *stride;
  SECTION("success") {
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range(
        nullptr, x.subarray, 0, 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range(
        x.ctx(), nullptr, 0, 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 2, 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid range_idx") {
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 2, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 0, nullptr, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_subarray_get_range(
        x.ctx(), x.subarray, 0, 0, &start, nullptr, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid stride" section here;
   * The stride is currently unsupported. All usage resolves to `nullptr`.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_get_range_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};
  const char* dim_name = "dim";  // The dimension name
  const void *start, *end, *stride;
  SECTION("success") {
    rc = tiledb_subarray_get_range_from_name(
        x.ctx(), x.subarray, dim_name, 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_from_name(
        nullptr, x.subarray, dim_name, 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_from_name(
        x.ctx(), nullptr, dim_name, 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_name") {
    rc = tiledb_subarray_get_range_from_name(
        x.ctx(), x.subarray, "invalid", 0, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid range_idx") {
    rc = tiledb_subarray_get_range_from_name(
        x.ctx(), x.subarray, dim_name, 2, &start, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_subarray_get_range_from_name(
        x.ctx(), x.subarray, dim_name, 0, nullptr, &end, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_subarray_get_range_from_name(
        x.ctx(), x.subarray, dim_name, 0, &start, nullptr, &stride);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid stride" section here;
   * The stride is currently unsupported. All usage resolves to `nullptr`.
   */
}

TEST_CASE(
    "C API: tiledb_subarray_get_range_var_size argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray_var x{};
  uint64_t start_size, end_size;
  SECTION("success") {
    rc = tiledb_subarray_get_range_var_size(
        x.ctx(), x.subarray, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var_size(
        nullptr, x.subarray, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var_size(
        x.ctx(), nullptr, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc = tiledb_subarray_get_range_var_size(
        x.ctx(), x.subarray, 2, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid range_idx") {
    rc = tiledb_subarray_get_range_var_size(
        x.ctx(), x.subarray, 0, 2, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_subarray_get_range_var_size(
        x.ctx(), x.subarray, 0, 0, nullptr, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_subarray_get_range_var_size(
        x.ctx(), x.subarray, 0, 0, &start_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_get_range_var_size_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray_var x{};
  const char* dim_name = "dim";  // The dimension name
  uint64_t start_size, end_size;
  SECTION("success") {
    rc = tiledb_subarray_get_range_var_size_from_name(
        x.ctx(), x.subarray, dim_name, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var_size_from_name(
        nullptr, x.subarray, dim_name, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var_size_from_name(
        x.ctx(), nullptr, dim_name, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_name") {
    rc = tiledb_subarray_get_range_var_size_from_name(
        x.ctx(), x.subarray, "invalid", 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid range_idx") {
    rc = tiledb_subarray_get_range_var_size_from_name(
        x.ctx(), x.subarray, "invalid", 2, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_subarray_get_range_var_size_from_name(
        x.ctx(), x.subarray, dim_name, 0, nullptr, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_subarray_get_range_var_size_from_name(
        x.ctx(), x.subarray, dim_name, 0, &start_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_get_range_var argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray_var x{};

  // Add a range to be fetched
  char start[] = "start", end[] = "end";
  rc = tiledb_subarray_add_range_var(x.ctx(), x.subarray, 0, start, 5, end, 3);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("success") {
    rc = tiledb_subarray_get_range_var(x.ctx(), x.subarray, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var(nullptr, x.subarray, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var(x.ctx(), nullptr, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_idx") {
    rc = tiledb_subarray_get_range_var(x.ctx(), x.subarray, 2, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid range_idx") {
    rc = tiledb_subarray_get_range_var(x.ctx(), x.subarray, 0, 2, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc =
        tiledb_subarray_get_range_var(x.ctx(), x.subarray, 0, 0, nullptr, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_subarray_get_range_var(
        x.ctx(), x.subarray, 0, 0, &start, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_subarray_get_range_var_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray_var x{};
  const char* dim_name = "dim";  // The dimension name

  // Add a range to be fetched
  char start[] = "start", end[] = "end";
  rc = tiledb_subarray_add_range_var_by_name(
      x.ctx(), x.subarray, dim_name, start, 5, end, 3);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("success") {
    rc = tiledb_subarray_get_range_var_from_name(
        x.ctx(), x.subarray, dim_name, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var_from_name(
        nullptr, x.subarray, dim_name, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var_from_name(
        x.ctx(), nullptr, dim_name, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dim_name") {
    rc = tiledb_subarray_get_range_var_from_name(
        x.ctx(), x.subarray, "invalid", 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid range_idx") {
    rc = tiledb_subarray_get_range_var_from_name(
        x.ctx(), x.subarray, dim_name, 2, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_subarray_get_range_var_from_name(
        x.ctx(), x.subarray, dim_name, 0, nullptr, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_subarray_get_range_var_from_name(
        x.ctx(), x.subarray, dim_name, 0, &start, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}
