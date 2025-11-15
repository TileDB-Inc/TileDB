/**
 * @file tiledb/api/c_api/attribute/test/unit_capi_attribute.cc
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
#include "../attribute_api_external.h"

using namespace tiledb::api::test_support;

TEST_CASE(
    "C API: tiledb_attribute_alloc argument validation", "[capi][attribute]") {
  ordinary_context ctx{};
  tiledb_attribute_handle_t* attribute{};
  SECTION("success") {
    capi_return_t rc =
        tiledb_attribute_alloc(ctx.context, "name", TILEDB_UINT32, &attribute);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_attribute_free(&attribute);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_alloc(nullptr, "name", TILEDB_UINT32, &attribute);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null name") {
    capi_return_t rc =
        tiledb_attribute_alloc(ctx.context, nullptr, TILEDB_UINT32, &attribute);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid type") {
    capi_return_t rc = tiledb_attribute_alloc(
        ctx.context, "name", TILEDB_INVALID_TYPE(), &attribute);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_alloc(ctx.context, "name", TILEDB_UINT32, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_free argument validation", "[capi][attribute]") {
  ordinary_context ctx{};
  tiledb_attribute_handle_t* attribute{};
  SECTION("success") {
    capi_return_t rc =
        tiledb_attribute_alloc(ctx.context, "name", TILEDB_UINT32, &attribute);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_attribute_free(&attribute));
    CHECK(attribute == nullptr);
  }
  SECTION("null attribute") {
    /*
     * `tiledb_attribute_free` is a void function, otherwise we would check for
     * an error.
     */
    REQUIRE_NOTHROW(tiledb_attribute_free(nullptr));
  }
}

/**
 * Test setup for ordinary Attribute tests.
 *
 * Anticipating a C.41 `class Attribute`, we construct everything required for
 * an attribute.
 */
struct ordinary_attribute_1 {
  ordinary_context ctx{};
  tiledb_attribute_t* attribute{};
  tiledb_filter_list_t* filter_list{};

  ordinary_attribute_1() {
    capi_return_t rc =
        tiledb_attribute_alloc(ctx.context, "name", TILEDB_UINT32, &attribute);
    if (tiledb_status(rc) != TILEDB_OK) {
      throw std::runtime_error("error creating test attribute");
    }
    rc = tiledb_filter_list_alloc(context(), &filter_list);
    if (tiledb_status(rc) != TILEDB_OK) {
      throw std::runtime_error("error creating filter list for test attribute");
    }
  }
  ~ordinary_attribute_1() {
    tiledb_filter_list_free(&filter_list);
    tiledb_attribute_free(&attribute);
  }
  [[nodiscard]] tiledb_ctx_handle_t* context() const {
    return ctx.context;
  }
};

TEST_CASE(
    "C API: tiledb_attribute_set_nullable argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  SECTION("success") {
    capi_return_t rc =
        tiledb_attribute_set_nullable(attr.context(), attr.attribute, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_set_nullable(nullptr, attr.attribute, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_set_nullable(attr.context(), nullptr, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /*
   * SECTION("invalid nullable"): All values of `nullable` are valid. Internally
   * the argument is converted to `bool`.
   */
}

TEST_CASE(
    "C API: tiledb_attribute_set_filter_list argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  SECTION("success") {
    capi_return_t rc = tiledb_attribute_set_filter_list(
        attr.context(), attr.attribute, attr.filter_list);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc = tiledb_attribute_set_filter_list(
        nullptr, attr.attribute, attr.filter_list);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc = tiledb_attribute_set_filter_list(
        attr.context(), nullptr, attr.filter_list);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter list") {
    capi_return_t rc = tiledb_attribute_set_filter_list(
        attr.context(), attr.attribute, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_set_cell_val_num argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  SECTION("success") {
    capi_return_t rc =
        tiledb_attribute_set_cell_val_num(attr.context(), attr.attribute, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_set_cell_val_num(nullptr, attr.attribute, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_set_cell_val_num(attr.context(), nullptr, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /*
   * SECTION("invalid cell_val_num"): All values of `cell_val_num` may be valid
   * in certain circumstances. Not checked in the API code and not tested here.
   */
}

TEST_CASE(
    "C API: tiledb_attribute_get_name argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  const char* name;
  SECTION("success") {
    capi_return_t rc =
        tiledb_attribute_get_name(attr.context(), attr.attribute, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(name != nullptr);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_get_name(nullptr, attr.attribute, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_get_name(attr.context(), nullptr, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    capi_return_t rc =
        tiledb_attribute_get_name(attr.context(), attr.attribute, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_get_type argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  tiledb_datatype_t type;
  SECTION("success") {
    capi_return_t rc =
        tiledb_attribute_get_type(attr.context(), attr.attribute, &type);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_get_type(nullptr, attr.attribute, &type);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_get_type(attr.context(), nullptr, &type);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null type") {
    capi_return_t rc =
        tiledb_attribute_get_type(attr.context(), attr.attribute, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_get_nullable argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  uint8_t nullable;
  SECTION("success") {
    capi_return_t rc = tiledb_attribute_get_nullable(
        attr.context(), attr.attribute, &nullable);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_get_nullable(nullptr, attr.attribute, &nullable);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_get_nullable(attr.context(), nullptr, &nullable);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null nullable") {
    capi_return_t rc =
        tiledb_attribute_get_nullable(attr.context(), attr.attribute, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_get_filter_list argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  tiledb_filter_list_handle_t* filter_list;
  SECTION("success") {
    capi_return_t rc = tiledb_attribute_get_filter_list(
        attr.context(), attr.attribute, &filter_list);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_get_filter_list(nullptr, attr.attribute, &filter_list);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_get_filter_list(attr.context(), nullptr, &filter_list);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null nullable") {
    capi_return_t rc = tiledb_attribute_get_filter_list(
        attr.context(), attr.attribute, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_get_cell_val_num argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  uint32_t cell_val_num;
  SECTION("success") {
    capi_return_t rc = tiledb_attribute_get_cell_val_num(
        attr.context(), attr.attribute, &cell_val_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc = tiledb_attribute_get_cell_val_num(
        nullptr, attr.attribute, &cell_val_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc = tiledb_attribute_get_cell_val_num(
        attr.context(), nullptr, &cell_val_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null cell_val_num") {
    capi_return_t rc = tiledb_attribute_get_cell_val_num(
        attr.context(), attr.attribute, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_get_cell_size argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  uint64_t cell_size;
  SECTION("success") {
    capi_return_t rc = tiledb_attribute_get_cell_size(
        attr.context(), attr.attribute, &cell_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc =
        tiledb_attribute_get_cell_size(nullptr, attr.attribute, &cell_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc =
        tiledb_attribute_get_cell_size(attr.context(), nullptr, &cell_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null cell_size") {
    capi_return_t rc =
        tiledb_attribute_get_cell_size(attr.context(), attr.attribute, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_dump_str argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  // SECTION("success") omitted to avoid log noise
  SECTION("null context") {
    tiledb_string_t* tdb_string;
    capi_return_t rc =
        tiledb_attribute_dump_str(nullptr, attr.attribute, &tdb_string);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null dimension") {
    tiledb_string_t* tdb_string;
    capi_return_t rc =
        tiledb_attribute_dump_str(attr.context(), nullptr, &tdb_string);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  // SECTION("null file pointer") `nullptr` is allowed; it's mapped to `stdout`
}

TEST_CASE(
    "C API: tiledb_attribute_set_fill_value argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  uint32_t fill{0};
  constexpr uint64_t fill_size{sizeof(fill)};
  SECTION("success") {
    capi_return_t rc = tiledb_attribute_set_fill_value(
        attr.context(), attr.attribute, &fill, fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc = tiledb_attribute_set_fill_value(
        nullptr, attr.attribute, &fill, fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc = tiledb_attribute_set_fill_value(
        attr.context(), nullptr, &fill, fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    capi_return_t rc = tiledb_attribute_set_fill_value(
        attr.context(), attr.attribute, nullptr, fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("zero size") {
    capi_return_t rc = tiledb_attribute_set_fill_value(
        attr.context(), attr.attribute, &fill, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_get_fill_value argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  const void* fill;
  uint64_t fill_size;
  SECTION("success") {
    capi_return_t rc = tiledb_attribute_get_fill_value(
        attr.context(), attr.attribute, &fill, &fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc = tiledb_attribute_get_fill_value(
        nullptr, attr.attribute, &fill, &fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    capi_return_t rc = tiledb_attribute_get_fill_value(
        attr.context(), nullptr, &fill, &fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    capi_return_t rc = tiledb_attribute_get_fill_value(
        attr.context(), attr.attribute, nullptr, &fill_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("zero size") {
    capi_return_t rc = tiledb_attribute_get_fill_value(
        attr.context(), attr.attribute, &fill, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_attribute_set_fill_value_nullable argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  capi_return_t rc =
      tiledb_attribute_set_nullable(attr.context(), attr.attribute, true);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  uint32_t fill{0};
  constexpr uint64_t fill_size{sizeof(fill)};
  uint8_t validity{1};
  SECTION("success") {
    rc = tiledb_attribute_set_fill_value_nullable(
        attr.context(), attr.attribute, &fill, fill_size, validity);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_attribute_set_fill_value_nullable(
        nullptr, attr.attribute, &fill, fill_size, validity);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    rc = tiledb_attribute_set_fill_value_nullable(
        attr.context(), nullptr, &fill, fill_size, validity);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    rc = tiledb_attribute_set_fill_value_nullable(
        attr.context(), attr.attribute, nullptr, fill_size, validity);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("zero size") {
    rc = tiledb_attribute_set_fill_value_nullable(
        attr.context(), attr.attribute, &fill, 0, validity);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  // SECTION("invalid validity") is absent because all values are valid
}

TEST_CASE(
    "C API: tiledb_attribute_get_fill_value_nullable argument validation",
    "[capi][attribute]") {
  ordinary_attribute_1 attr{};
  capi_return_t rc =
      tiledb_attribute_set_nullable(attr.context(), attr.attribute, true);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  const void* fill;
  uint64_t fill_size;
  uint8_t validity;
  SECTION("success") {
    rc = tiledb_attribute_get_fill_value_nullable(
        attr.context(), attr.attribute, &fill, &fill_size, &validity);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_attribute_get_fill_value_nullable(
        nullptr, attr.attribute, &fill, &fill_size, &validity);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null attribute") {
    rc = tiledb_attribute_get_fill_value_nullable(
        attr.context(), nullptr, &fill, &fill_size, &validity);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    rc = tiledb_attribute_get_fill_value_nullable(
        attr.context(), attr.attribute, nullptr, &fill_size, &validity);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null size") {
    rc = tiledb_attribute_get_fill_value_nullable(
        attr.context(), attr.attribute, &fill, 0, &validity);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null validity") {
    rc = tiledb_attribute_get_fill_value_nullable(
        attr.context(), attr.attribute, &fill, &fill_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}
