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

/*TEST_CASE(
    "C API: tiledb_subarray_set_coalesce_ranges argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_set_coalesce_ranges(
  //  tiledb_subarray_t* subarray, int coalesce_ranges)

  SECTION("success") {
    rc = tiledb_subarray_set_coalesce_ranges(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_set_coalesce_ranges(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_set_coalesce_ranges(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_set_coalesce_ranges(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_set_subarray argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_set_subarray(
  // tiledb_subarray_t* subarray, const void* subarray_vals)

  SECTION("success") {
    rc = tiledb_subarray_set_subarray(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_set_subarray(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_set_subarray(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_set_subarray(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_add_point_ranges argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_add_point_ranges(
  //  tiledb_subarray_t* subarray,
  //  uint32_t dim_idx,
  //  const void* start,
  //  uint64_t count)

  SECTION("success") {
    rc = tiledb_subarray_add_point_ranges(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_point_ranges(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_point_ranges(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_add_point_ranges(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_add_range argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_add_range(
  //  tiledb_subarray_t* subarray,
  //  uint32_t dim_idx,
  //  const void* start,
  //  const void* end,
  //  const void* stride)

  SECTION("success") {
    rc = tiledb_subarray_add_range(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_range(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_add_range(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_add_range_by_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_add_range_by_name(
  //  tiledb_subarray_t* subarray,
  //  const char* dim_name,
  //  const void* start,
  //  const void* end,
  //  const void* stride)

  SECTION("success") {
    rc = tiledb_subarray_add_range_by_name(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_range_by_name(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range_by_name(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_add_range_by_name(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_add_range_var argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_add_range_var(
  //  tiledb_subarray_t* subarray,
  //  uint32_t dim_idx,
  //  const void* start,
  //  uint64_t start_size,
  //  const void* end,
  //  uint64_t end_size)

  SECTION("success") {
    rc = tiledb_subarray_add_range_var(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_range_var(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range_var(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_add_range_var(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_add_range_var_by_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_add_range_var_by_name(
  //  tiledb_subarray_t* subarray,
  //  const char* dim_name,
  //  const void* start,
  //  uint64_t start_size,
  //  const void* end,
  //  uint64_t end_size)

  SECTION("success") {
    rc = tiledb_subarray_add_range_var_by_name(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_add_range_var_by_name(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_add_range_var_by_name(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_add_range_var_by_name(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range_num argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range_num(
  //  const tiledb_subarray_t* subarray, uint32_t dim_idx, uint64_t* range_num)

  SECTION("success") {
    rc = tiledb_subarray_get_range_num(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_num(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_num(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range_num(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range_num_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range_num_from_name(
  //  const tiledb_subarray_t* subarray,
  //  const char* dim_name,
  //  uint64_t* range_num)

  SECTION("success") {
    rc = tiledb_subarray_get_range_num_from_name(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_num_from_name(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_num_from_name(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range_num_from_name(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range(
  //  const tiledb_subarray_t* subarray,
  //  uint32_t dim_idx,
  //  uint64_t range_idx,
  //  const void** start,
  //  const void** end,
  //  const void** stride)

  SECTION("success") {
    rc = tiledb_subarray_get_range(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range_from_name(
  //  const tiledb_subarray_t* subarray,
  //  const char* dim_name,
  //  uint64_t range_idx,
  //  const void** start,
  //  const void** end,
  //  const void** stride)

  SECTION("success") {
    rc = tiledb_subarray_get_range_from_name(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_from_name(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_from_name(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range_from_name(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range_var_size argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range_var_size(
  //  const tiledb_subarray_t* subarray,
  //  uint32_t dim_idx,
  //  uint64_t range_idx,
  //  uint64_t* start_size,
  //  uint64_t* end_size)

  SECTION("success") {
    rc = tiledb_subarray_get_range_var_size(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var_size(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var_size(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range_var_size(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range_var_size_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range_var_size_from_name(
  //  const tiledb_subarray_t* subarray,
  //  const char* dim_name,
  //  uint64_t range_idx,
  //  uint64_t* start_size,
  //  uint64_t* end_size)

  SECTION("success") {
    rc = tiledb_subarray_get_range_var_size_from_name(x.ctx(), x.subarray,
config); REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var_size_from_name(nullptr, x.subarray,
config); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var_size_from_name(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range_var_size_from_name(x.ctx(), x.subarray,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range_var argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range_var(
  //  const tiledb_subarray_t* subarray,
  //  uint32_t dim_idx,
  //  uint64_t range_idx,
  //  void* start,
  //  void* end)

  SECTION("success") {
    rc = tiledb_subarray_get_range_var(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range_var(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_subarray_get_range_var_from_name argument validation",
    "[capi][subarray]") {
  capi_return_t rc;
  ordinary_subarray x{};

  //tiledb_subarray_get_range_var_from_name(
  //  const tiledb_subarray_t* subarray,
  //  const char* dim_name,
  //  uint64_t range_idx,
  //  void* start,
  //  void* end)

  SECTION("success") {
    rc = tiledb_subarray_get_range_var_from_name(x.ctx(), x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_config_free(&config));
    CHECK(config == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_subarray_get_range_var_from_name(nullptr, x.subarray, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null subarray") {
    rc = tiledb_subarray_get_range_var_from_name(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_subarray_get_range_var_from_name(x.ctx(), x.subarray, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/
