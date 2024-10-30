/**
 * @file tiledb/api/c_api/array_schema/test/unit_capi_array_schema.cc
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
 * Validates the arguments for the ArraySchema C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../../../c_api_test_support/testsupport_capi_array_schema.h"
#include "../../../c_api_test_support/testsupport_capi_context.h"
#include "../../attribute/attribute_api_external_experimental.h"
#include "../array_schema_api_experimental.h"
#include "../array_schema_api_external.h"
#include "../array_schema_api_internal.h"

using namespace tiledb::api::test_support;

TEST_CASE(
    "C API: tiledb_array_schema_alloc argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_context ctx{};
  tiledb_array_schema_handle_t* schema{};
  SECTION("success") {
    rc = tiledb_array_schema_alloc(ctx.context, TILEDB_DENSE, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_array_schema_free(&schema);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_alloc(nullptr, TILEDB_DENSE, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  /*
   * SECTION("invalid array_type"): All values of `array_type` may be valid
   * in certain circumstances. Not checked in the API code and not tested here.
   */
  SECTION("null schema") {
    rc = tiledb_array_schema_alloc(ctx.context, TILEDB_DENSE, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_free argument validation",
    "[capi][array_schema]") {
  ordinary_context ctx{};
  tiledb_array_schema_handle_t* schema{};
  auto rc = tiledb_array_schema_alloc(ctx.context, TILEDB_DENSE, &schema);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    REQUIRE_NOTHROW(tiledb_array_schema_free(&schema));
    CHECK(schema == nullptr);
  }
  SECTION("null schema") {
    /*
     * `tiledb_array_schema_free` is a void function, otherwise we would check
     * for an error.
     */
    REQUIRE_NOTHROW(tiledb_array_schema_free(nullptr));
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_add_attribute argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_attribute_t* attr{};
  rc = tiledb_attribute_alloc(x.ctx(), "a", TILEDB_INT32, &attr);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_array_schema_add_attribute(x.ctx(), x.schema, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_add_attribute(nullptr, x.schema, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_add_attribute(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_array_schema_add_attribute(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_attribute_free(&attr));
  CHECK(attr == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_set_allows_dups argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{TILEDB_DENSE};
  SECTION("success") {
    rc = tiledb_array_schema_set_allows_dups(x.ctx(), x.schema, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_allows_dups(nullptr, x.schema, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_allows_dups(x.ctx(), nullptr, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid allows_dups") {
    /*
     * This API is applicable _only_ to sparse arrays.
     * As such, any non-zero value set on a dense array is considered invalid.
     */
    rc = tiledb_array_schema_set_allows_dups(x.ctx(), x.schema, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_allows_dups argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  int allows_dups;
  SECTION("success") {
    rc = tiledb_array_schema_get_allows_dups(x.ctx(), x.schema, &allows_dups);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_allows_dups(nullptr, x.schema, &allows_dups);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_allows_dups(x.ctx(), nullptr, &allows_dups);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null allows_dups") {
    rc = tiledb_array_schema_get_allows_dups(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_version argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  uint32_t version;
  SECTION("success") {
    rc = tiledb_array_schema_get_version(x.ctx(), x.schema, &version);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_version(nullptr, x.schema, &version);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_version(x.ctx(), nullptr, &version);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null version") {
    rc = tiledb_array_schema_get_version(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_set_domain argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_domain_t* domain{};
  rc = tiledb_domain_alloc(x.ctx(), &domain);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  ordinary_dimension_d1 dim;
  rc = tiledb_domain_add_dimension(x.ctx(), domain, dim.dimension);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_array_schema_set_domain(x.ctx(), x.schema, domain);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_domain(nullptr, x.schema, domain);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_domain(x.ctx(), nullptr, domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    rc = tiledb_array_schema_set_domain(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_domain_free(&domain));
  CHECK(domain == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_set_capacity argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  SECTION("success") {
    rc = tiledb_array_schema_set_capacity(x.ctx(), x.schema, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_capacity(nullptr, x.schema, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_capacity(x.ctx(), nullptr, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid capacity") {
    /* The capacity may not be zero. */
    rc = tiledb_array_schema_set_capacity(x.ctx(), x.schema, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_set_cell_order argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  SECTION("success") {
    rc = tiledb_array_schema_set_cell_order(x.ctx(), x.schema, layout);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_cell_order(nullptr, x.schema, layout);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_cell_order(x.ctx(), nullptr, layout);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid layout") {
    /* A cell order of TILEDB_UNORDERED is not yet supported. */
    rc =
        tiledb_array_schema_set_cell_order(x.ctx(), x.schema, TILEDB_UNORDERED);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_set_tile_order argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{TILEDB_DENSE};
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  SECTION("success") {
    rc = tiledb_array_schema_set_tile_order(x.ctx(), x.schema, layout);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_tile_order(nullptr, x.schema, layout);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_tile_order(x.ctx(), nullptr, layout);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid layout") {
    /* ordinary_array_schema is dense, which disallows unordered layouts. */
    rc =
        tiledb_array_schema_set_tile_order(x.ctx(), x.schema, TILEDB_UNORDERED);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_timestamp_range argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  uint64_t lo;
  uint64_t hi;
  SECTION("success") {
    rc = tiledb_array_schema_timestamp_range(x.ctx(), x.schema, &lo, &hi);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_timestamp_range(nullptr, x.schema, &lo, &hi);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_timestamp_range(x.ctx(), nullptr, &lo, &hi);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null lo") {
    rc = tiledb_array_schema_timestamp_range(x.ctx(), x.schema, nullptr, &hi);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null hi") {
    rc = tiledb_array_schema_timestamp_range(x.ctx(), x.schema, &lo, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_add_enumeration argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_enumeration_t* enumeration;
  int32_t values[5] = {1, 2, 3, 4, 5};
  rc = tiledb_enumeration_alloc(
      x.ctx(),
      "enumeration",
      TILEDB_UINT32,
      1,
      0,
      values,
      sizeof(uint32_t) * 5,
      nullptr,
      0,
      &enumeration);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_array_schema_add_enumeration(x.ctx(), x.schema, enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_add_enumeration(nullptr, x.schema, enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_add_enumeration(x.ctx(), nullptr, enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null enumeration") {
    rc = tiledb_array_schema_add_enumeration(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_enumeration_free(&enumeration));
  CHECK(enumeration == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_get_enumeration_from_name argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_enumeration_t* enumeration;
  SECTION("null context") {
    rc = tiledb_array_schema_get_enumeration_from_name(
        nullptr, x.schema, "primes", &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_enumeration_from_name(
        x.ctx(), nullptr, "primes", &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_array_schema_get_enumeration_from_name(
        x.ctx(), x.schema, nullptr, &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null enumeration") {
    rc = tiledb_array_schema_get_enumeration_from_name(
        x.ctx(), x.schema, "primes", nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("success") {
    int32_t values[5] = {2, 3, 5, 7, 11};
    rc = tiledb_enumeration_alloc(
        x.ctx(),
        "primes",
        TILEDB_UINT32,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    rc = tiledb_array_schema_add_enumeration(x.ctx(), x.schema, enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_enumeration_free(&enumeration));
    CHECK(enumeration == nullptr);

    rc = tiledb_array_schema_get_enumeration_from_name(
        x.ctx(), x.schema, "primes", &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(enumeration != nullptr);

    tiledb_string_t* tiledb_name(nullptr);
    rc = tiledb_enumeration_get_name(x.ctx(), enumeration, &tiledb_name);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(tiledb_name != nullptr);

    const char* name;
    size_t length;
    rc = tiledb_string_view(tiledb_name, &name, &length);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(std::string(name, length) == "primes");
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_enumeration_from_attribute_name argument "
    "validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema_with_attr x{};
  tiledb_enumeration_t* enumeration;
  SECTION("null context") {
    rc = tiledb_array_schema_get_enumeration_from_attribute_name(
        nullptr, x.schema, "a", &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_enumeration_from_attribute_name(
        x.ctx(), nullptr, "a", &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_array_schema_get_enumeration_from_attribute_name(
        x.ctx(), x.schema, nullptr, &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null enumeration") {
    rc = tiledb_array_schema_get_enumeration_from_attribute_name(
        x.ctx(), x.schema, "a", nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid attribute") {
    rc = tiledb_array_schema_get_enumeration_from_attribute_name(
        x.ctx(), x.schema, "foobar", nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("success") {
    // create and add enumeration to schema
    int32_t values[5] = {2, 3, 5, 7, 11};
    rc = tiledb_enumeration_alloc(
        x.ctx(),
        "primes",
        TILEDB_UINT32,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    rc = tiledb_array_schema_add_enumeration(x.ctx(), x.schema, enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_enumeration_free(&enumeration));
    CHECK(enumeration == nullptr);

    // add enumeration to the attribute
    tiledb_attribute_t* attribute;
    rc = tiledb_array_schema_get_attribute_from_name(
        x.ctx(), x.schema, "a", &attribute);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    rc = tiledb_attribute_set_enumeration_name(x.ctx(), attribute, "primes");
    REQUIRE(tiledb_status(rc) == TILEDB_OK);

    // then retrieve the enumeration using attribute name
    rc = tiledb_array_schema_get_enumeration_from_attribute_name(
        x.ctx(), x.schema, "a", &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(enumeration != nullptr);

    tiledb_string_t* tiledb_name(nullptr);
    rc = tiledb_enumeration_get_name(x.ctx(), enumeration, &tiledb_name);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(tiledb_name != nullptr);

    const char* name;
    size_t length;
    rc = tiledb_string_view(tiledb_name, &name, &length);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(std::string(name, length) == "primes");
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_set_coords_filter_list argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_filter_list_t* fl;
  rc = tiledb_filter_list_alloc(x.ctx(), &fl);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_array_schema_set_coords_filter_list(x.ctx(), x.schema, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_coords_filter_list(nullptr, x.schema, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_coords_filter_list(x.ctx(), nullptr, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter_list") {
    rc = tiledb_array_schema_set_coords_filter_list(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_filter_list_free(&fl));
  CHECK(fl == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_set_offsets_filter_list argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_filter_list_t* fl;
  rc = tiledb_filter_list_alloc(x.ctx(), &fl);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_array_schema_set_offsets_filter_list(x.ctx(), x.schema, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_offsets_filter_list(nullptr, x.schema, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_offsets_filter_list(x.ctx(), nullptr, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter_list") {
    rc =
        tiledb_array_schema_set_offsets_filter_list(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_filter_list_free(&fl));
  CHECK(fl == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_set_validity_filter_list argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_filter_list_t* fl;
  rc = tiledb_filter_list_alloc(x.ctx(), &fl);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_array_schema_set_validity_filter_list(x.ctx(), x.schema, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_validity_filter_list(nullptr, x.schema, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_validity_filter_list(x.ctx(), nullptr, fl);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter_list") {
    rc = tiledb_array_schema_set_validity_filter_list(
        x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_filter_list_free(&fl));
  CHECK(fl == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_check argument validation",
    "[capi][array_schema]") {
  /*
   * No "success" section here; too much overhead to set up.
   */
  capi_return_t rc;
  ordinary_array_schema x{};
  SECTION("null context") {
    rc = tiledb_array_schema_check(nullptr, x.schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_check(x.ctx(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_array_type argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_array_type_t array_type;
  SECTION("success") {
    rc = tiledb_array_schema_get_array_type(x.ctx(), x.schema, &array_type);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_array_type(nullptr, x.schema, &array_type);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_array_type(x.ctx(), nullptr, &array_type);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null array_type") {
    rc = tiledb_array_schema_get_array_type(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_capacity argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  uint64_t capacity;
  SECTION("success") {
    rc = tiledb_array_schema_get_capacity(x.ctx(), x.schema, &capacity);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_capacity(nullptr, x.schema, &capacity);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_capacity(x.ctx(), nullptr, &capacity);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null capacity") {
    rc = tiledb_array_schema_get_capacity(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_cell_order argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_layout_t cell_order;
  SECTION("success") {
    rc = tiledb_array_schema_get_cell_order(x.ctx(), x.schema, &cell_order);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_cell_order(nullptr, x.schema, &cell_order);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_cell_order(x.ctx(), nullptr, &cell_order);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null cell_order") {
    rc = tiledb_array_schema_get_cell_order(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_coords_filter_list argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_filter_list_t* fl;
  SECTION("success") {
    rc = tiledb_array_schema_get_coords_filter_list(x.ctx(), x.schema, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_coords_filter_list(nullptr, x.schema, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_coords_filter_list(x.ctx(), nullptr, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter_list") {
    rc = tiledb_array_schema_get_coords_filter_list(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_offsets_filter_list argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_filter_list_t* fl;
  SECTION("success") {
    rc = tiledb_array_schema_get_offsets_filter_list(x.ctx(), x.schema, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_offsets_filter_list(nullptr, x.schema, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_offsets_filter_list(x.ctx(), nullptr, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter_list") {
    rc =
        tiledb_array_schema_get_offsets_filter_list(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_validity_filter_list argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_filter_list_t* fl;
  SECTION("success") {
    rc = tiledb_array_schema_get_validity_filter_list(x.ctx(), x.schema, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_validity_filter_list(nullptr, x.schema, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_validity_filter_list(x.ctx(), nullptr, &fl);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filter_list") {
    rc = tiledb_array_schema_get_validity_filter_list(
        x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_domain argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_domain_t* domain;
  SECTION("success") {
    rc = tiledb_array_schema_get_domain(x.ctx(), x.schema, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_domain(nullptr, x.schema, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_domain(x.ctx(), nullptr, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    rc = tiledb_array_schema_get_domain(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_tile_order argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_layout_t tile_order;
  SECTION("success") {
    rc = tiledb_array_schema_get_tile_order(x.ctx(), x.schema, &tile_order);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_tile_order(nullptr, x.schema, &tile_order);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_tile_order(x.ctx(), nullptr, &tile_order);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null tile_order") {
    rc = tiledb_array_schema_get_tile_order(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_attribute_num argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  uint32_t attr_num;
  SECTION("success") {
    rc = tiledb_array_schema_get_attribute_num(x.ctx(), x.schema, &attr_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_attribute_num(nullptr, x.schema, &attr_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_attribute_num(x.ctx(), nullptr, &attr_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute_num") {
    rc = tiledb_array_schema_get_attribute_num(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_dump_str argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_string_t* out;
  /*
   * No "success" section here; omitted to avoid log noise.
   */
  SECTION("null context") {
    rc = tiledb_array_schema_dump_str(nullptr, x.schema, &out);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_dump_str(x.ctx(), nullptr, &out);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /*
   * SECTION("null file pointer"): `nullptr` is allowed; it's mapped to `stdout`
   */
}

TEST_CASE(
    "C API: tiledb_array_schema_get_attribute_from_index argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema_with_attr x{};
  tiledb_attribute_t* attr;
  SECTION("success") {
    rc = tiledb_array_schema_get_attribute_from_index(
        x.ctx(), x.schema, 0, &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_attribute_from_index(
        nullptr, x.schema, 0, &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_attribute_from_index(
        x.ctx(), nullptr, 0, &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid index") {
    rc = tiledb_array_schema_get_attribute_from_index(
        x.ctx(), x.schema, 1, &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_array_schema_get_attribute_from_index(
        x.ctx(), x.schema, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_get_attribute_from_name argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema_with_attr x{};
  tiledb_attribute_t* attr;
  SECTION("success") {
    rc = tiledb_array_schema_get_attribute_from_name(
        x.ctx(), x.schema, "a", &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_attribute_from_name(
        nullptr, x.schema, "a", &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_attribute_from_name(
        x.ctx(), nullptr, "a", &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_array_schema_get_attribute_from_name(
        x.ctx(), x.schema, nullptr, &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid name") {
    rc = tiledb_array_schema_get_attribute_from_name(
        x.ctx(), x.schema, "b", &attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_array_schema_get_attribute_from_name(
        x.ctx(), x.schema, "a", nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_has_attribute argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema_with_attr x{};
  int32_t has_attr;
  SECTION("success") {
    rc = tiledb_array_schema_has_attribute(x.ctx(), x.schema, "a", &has_attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(has_attr == 1);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_has_attribute(nullptr, x.schema, "a", &has_attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_has_attribute(x.ctx(), nullptr, "a", &has_attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid name") {
    // Note: this test is successful, but has_attr will equate to false (0).
    rc = tiledb_array_schema_has_attribute(x.ctx(), x.schema, "b", &has_attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(has_attr == 0);
  }
  SECTION("null has_attr") {
    rc = tiledb_array_schema_has_attribute(x.ctx(), x.schema, "a", nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_schema_set_current_domain argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_current_domain_t* cd{};
  rc = tiledb_current_domain_create(x.ctx(), &cd);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_array_schema_set_current_domain(x.ctx(), x.schema, cd);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_set_current_domain(nullptr, x.schema, cd);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_set_current_domain(x.ctx(), nullptr, cd);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null current_domain") {
    rc = tiledb_array_schema_set_current_domain(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_current_domain_free(&cd));
  CHECK(cd == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_get_current_domain argument validation",
    "[capi][array_schema]") {
  capi_return_t rc;
  ordinary_array_schema x{};
  tiledb_current_domain_t* cd;
  SECTION("success") {
    rc = tiledb_array_schema_get_current_domain(x.ctx(), x.schema, &cd);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_schema_get_current_domain(nullptr, x.schema, &cd);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_get_current_domain(x.ctx(), nullptr, &cd);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null current_domain") {
    rc = tiledb_array_schema_get_current_domain(x.ctx(), x.schema, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}
