/**
 * @file tiledb/api/c_api/dimension/test/unit_capi_dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../../../c_api_test_support/testsupport_capi_context.h"
#include "../../../c_api_test_support/testsupport_capi_datatype.h"
#include "../../filter_list/filter_list_api_internal.h"
#include "../dimension_api_internal.h"

using namespace tiledb::api::test_support;

TEST_CASE(
    "C API: tiledb_dimension_alloc argument validation", "[capi][dimension]") {
  ordinary_context ctx{};
  tiledb_dimension_t* dimension;
  uint32_t constraint[2]{0, 10};
  SECTION("success") {
    capi_return_t rc = tiledb_dimension_alloc(
        ctx.context, "name", TILEDB_UINT32, constraint, nullptr, &dimension);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_dimension_free(&dimension);
  }
  SECTION("null context") {
    capi_return_t rc = tiledb_dimension_alloc(
        nullptr, "name", TILEDB_UINT32, constraint, nullptr, &dimension);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null name") {
    capi_return_t rc = tiledb_dimension_alloc(
        ctx.context, nullptr, TILEDB_UINT32, constraint, nullptr, &dimension);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /*
   * SECTION("null domain") NULL domain arguments are allowed for string-type
   * dimensions, but not for others. Consistency is not checked in API code and
   * not tested here.
   */
  /*
   * SECTION("null extent") NULL tile extent is allowed.
   */
  SECTION("invalid data type") {
    capi_return_t rc = tiledb_dimension_alloc(
        ctx.context,
        "name",
        TILEDB_INVALID_TYPE(),
        constraint,
        nullptr,
        &dimension);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dimension") {
    capi_return_t rc = tiledb_dimension_alloc(
        ctx.context, "name", TILEDB_UINT32, constraint, nullptr, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_dimension_free argument validation", "[capi][dimension]") {
  ordinary_context ctx{};
  tiledb_dimension_t* dimension;
  uint32_t constraint[2]{0, 10};
  SECTION("success") {
    capi_return_t rc = tiledb_dimension_alloc(
        ctx.context, "name", TILEDB_UINT32, constraint, nullptr, &dimension);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_dimension_free(&dimension));
    CHECK(dimension == nullptr);
  }
  SECTION("null dimension") {
    REQUIRE_NOTHROW(tiledb_dimension_free(nullptr));
  }
}

struct ordinary_dimension_1 {
  ordinary_context ctx{};
  tiledb_dimension_t* dimension{};
  const uint32_t constraint[2]{0, 10};
  ordinary_dimension_1() {
    capi_return_t rc = tiledb_dimension_alloc(
        ctx.context, "name", TILEDB_UINT32, constraint, nullptr, &dimension);
    if (tiledb_status(rc) != TILEDB_OK) {
      throw std::runtime_error("error creating test dimension");
    }
  }
  ~ordinary_dimension_1() {
    tiledb_dimension_free(&dimension);
  }
};

TEST_CASE(
    "C API: tiledb_dimension_set_filter_list argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  auto fp{
      tiledb_filter_list_handle_t::make_handle(tiledb::sm::FilterPipeline())};
  SECTION("success") {
    capi_return_t rc =
        tiledb_dimension_set_filter_list(dim.ctx.context, dim.dimension, fp);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_dimension_set_filter_list(nullptr, dim.dimension, fp);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc =
        tiledb_dimension_set_filter_list(dim.ctx.context, nullptr, fp);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter list") {
    capi_return_t rc = tiledb_dimension_set_filter_list(
        dim.ctx.context, dim.dimension, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_filter_list_handle_t::break_handle(fp);
}

TEST_CASE(
    "C API: tiledb_dimension_set_cell_val_num argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  SECTION("success") {
    capi_return_t rc =
        tiledb_dimension_set_cell_val_num(dim.ctx.context, dim.dimension, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_dimension_set_cell_val_num(nullptr, dim.dimension, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc =
        tiledb_dimension_set_cell_val_num(dim.ctx.context, nullptr, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("bad number") {
    /*
     * It's an error to have any number other than 1 in non-string dimensions.
     */
    capi_return_t rc =
        tiledb_dimension_set_cell_val_num(dim.ctx.context, dim.dimension, 2);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_dimension_get_name argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  const char* name;
  SECTION("success") {
    capi_return_t rc =
        tiledb_dimension_get_name(dim.ctx.context, dim.dimension, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc = tiledb_dimension_get_name(nullptr, dim.dimension, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc =
        tiledb_dimension_get_name(dim.ctx.context, nullptr, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    capi_return_t rc =
        tiledb_dimension_get_name(dim.ctx.context, dim.dimension, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_dimension_get_type argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  tiledb_datatype_t type;
  SECTION("success") {
    capi_return_t rc =
        tiledb_dimension_get_type(dim.ctx.context, dim.dimension, &type);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc = tiledb_dimension_get_type(nullptr, dim.dimension, &type);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc =
        tiledb_dimension_get_type(dim.ctx.context, nullptr, &type);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null type") {
    capi_return_t rc =
        tiledb_dimension_get_type(dim.ctx.context, dim.dimension, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_dimension_get_domain argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  const void* constraint;
  SECTION("success") {
    capi_return_t rc = tiledb_dimension_get_domain(
        dim.ctx.context, dim.dimension, &constraint);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_dimension_get_domain(nullptr, dim.dimension, &constraint);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc =
        tiledb_dimension_get_domain(dim.ctx.context, nullptr, &constraint);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    capi_return_t rc =
        tiledb_dimension_get_domain(dim.ctx.context, dim.dimension, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_dimension_get_tile_extent argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  const void* tile_extent;
  SECTION("success") {
    capi_return_t rc = tiledb_dimension_get_tile_extent(
        dim.ctx.context, dim.dimension, &tile_extent);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_dimension_get_tile_extent(nullptr, dim.dimension, &tile_extent);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc = tiledb_dimension_get_tile_extent(
        dim.ctx.context, nullptr, &tile_extent);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null tile extent") {
    capi_return_t rc = tiledb_dimension_get_tile_extent(
        dim.ctx.context, dim.dimension, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_dimension_get_filter_list argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  tiledb_filter_list_handle_t* fl;
  SECTION("success") {
    capi_return_t rc =
        tiledb_dimension_get_filter_list(dim.ctx.context, dim.dimension, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_dimension_get_filter_list(nullptr, dim.dimension, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc =
        tiledb_dimension_get_filter_list(dim.ctx.context, nullptr, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter list") {
    capi_return_t rc = tiledb_dimension_get_filter_list(
        dim.ctx.context, dim.dimension, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_dimension_get_cell_val_num argument validation",
    "[capi][dimension]") {
  ordinary_dimension_1 dim;
  uint32_t N;
  SECTION("success") {
    capi_return_t rc =
        tiledb_dimension_get_cell_val_num(dim.ctx.context, dim.dimension, &N);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_dimension_get_cell_val_num(nullptr, dim.dimension, &N);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    capi_return_t rc =
        tiledb_dimension_get_cell_val_num(dim.ctx.context, nullptr, &N);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null cell number") {
    capi_return_t rc = tiledb_dimension_get_cell_val_num(
        dim.ctx.context, dim.dimension, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

// For `stderr`
#include <cstdio>

TEST_CASE(
    "C API: tiledb_dimension_dump argument validation", "[capi][dimension]") {
  ordinary_dimension_1 dim;
  // SECTION("success") omitted to avoid log noise
  SECTION("null context") {
    tiledb_string_t* tdb_string;
    capi_return_t rc =
        tiledb_dimension_dump_str(nullptr, dim.dimension, &tdb_string);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    tiledb_string_t* tdb_string;
    capi_return_t rc =
        tiledb_dimension_dump_str(dim.ctx.context, nullptr, &tdb_string);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  // SECTION("null file pointer") `nullptr` is allowed; it's mapped to `stdout`
}
