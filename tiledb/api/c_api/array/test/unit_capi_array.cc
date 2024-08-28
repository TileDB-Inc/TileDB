/**
 * @file tiledb/api/c_api/array/test/unit_capi_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * Validates the arguments for the Array C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>

#include "../array_api_experimental.h"
#include "../array_api_external.h"
#include "../array_api_internal.h"
#include "tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_internal.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_array.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_context.h"

using namespace tiledb::api::test_support;

const char* TEST_URI = "unit_capi_array";

TEST_CASE(
    "C API: tiledb_array_schema_load argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_context ctx{};
  tiledb_array_schema_t* schema{};
  /*
   * No "success" section here; too much overhead to set up.
   */
  SECTION("null context") {
    rc = tiledb_array_schema_load(nullptr, TEST_URI, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    rc = tiledb_array_schema_load(ctx.context, nullptr, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_load(ctx.context, TEST_URI, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_array_schema_free(&schema));
  CHECK(schema == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_schema_load_with_config argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_context ctx{};
  tiledb_config_t* config{};
  tiledb_array_schema_t* schema{};
  /*
   * No "success" section here; too much overhead to set up.
   */
  SECTION("null context") {
    rc = tiledb_array_schema_load_with_config(
        nullptr, config, TEST_URI, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null config") {
    // Note: a null config is actually valid and will use the context's config.
    // This test case merely fails without the proper overhead setup.
    rc = tiledb_array_schema_load_with_config(
        ctx.context, nullptr, TEST_URI, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    rc = tiledb_array_schema_load_with_config(
        ctx.context, config, nullptr, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null schema") {
    rc = tiledb_array_schema_load_with_config(
        ctx.context, config, TEST_URI, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_array_schema_free(&schema));
  CHECK(schema == nullptr);
}

TEST_CASE("C API: tiledb_array_alloc argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_context ctx{};
  tiledb_array_handle_t* array{};
  SECTION("success") {
    rc = tiledb_array_alloc(ctx.context, TEST_URI, &array);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_alloc(nullptr, TEST_URI, &array);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    rc = tiledb_array_alloc(ctx.context, nullptr, &array);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null schema") {
    rc = tiledb_array_alloc(ctx.context, TEST_URI, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_array_free(&array));
  CHECK(array == nullptr);
}

TEST_CASE("C API: tiledb_array_free argument validation", "[capi][array]") {
  ordinary_context ctx{};
  tiledb_array_handle_t* array{};
  auto rc = tiledb_array_alloc(ctx.context, TEST_URI, &array);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    REQUIRE_NOTHROW(tiledb_array_free(&array));
    CHECK(array == nullptr);
  }
  SECTION("null array") {
    /*
     * `tiledb_array_free` is a void function, otherwise we would check
     * for an error.
     */
    REQUIRE_NOTHROW(tiledb_array_free(nullptr));
  }
}

TEST_CASE("C API: tiledb_array_create argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_context ctx{};
  tiledb_dimension_t* d1;
  tiledb_dimension_t* d2;
  tiledb_domain_t* domain;
  tiledb_attribute_t* attr;
  tiledb_array_schema_t* schema;

  // Set up : create schema
  int dim_domain[] = {1, 4, 1, 4};
  int tile_extents[] = {4, 4};
  REQUIRE_NOTHROW(tiledb_dimension_alloc(
      ctx.context,
      "rows",
      TILEDB_INT32,
      &dim_domain[0],
      &tile_extents[0],
      &d1));
  REQUIRE_NOTHROW(tiledb_dimension_alloc(
      ctx.context,
      "cols",
      TILEDB_INT32,
      &dim_domain[2],
      &tile_extents[1],
      &d2));
  REQUIRE_NOTHROW(tiledb_domain_alloc(ctx.context, &domain));
  REQUIRE_NOTHROW(tiledb_domain_add_dimension(ctx.context, domain, d1));
  REQUIRE_NOTHROW(tiledb_domain_add_dimension(ctx.context, domain, d2));
  REQUIRE_NOTHROW(
      tiledb_attribute_alloc(ctx.context, "a", TILEDB_INT32, &attr));
  REQUIRE_NOTHROW(
      tiledb_array_schema_alloc(ctx.context, TILEDB_DENSE, &schema));
  REQUIRE_NOTHROW(tiledb_array_schema_set_cell_order(
      ctx.context, schema, TILEDB_ROW_MAJOR));
  REQUIRE_NOTHROW(tiledb_array_schema_set_tile_order(
      ctx.context, schema, TILEDB_ROW_MAJOR));
  REQUIRE_NOTHROW(tiledb_array_schema_set_domain(ctx.context, schema, domain));
  REQUIRE_NOTHROW(tiledb_array_schema_add_attribute(ctx.context, schema, attr));

  SECTION("success") {
    rc = tiledb_array_create(ctx.context, TEST_URI, schema);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_create(nullptr, TEST_URI, schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    rc = tiledb_array_create(ctx.context, nullptr, schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null schema") {
    rc = tiledb_array_create(ctx.context, TEST_URI, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  // Clean up
  REQUIRE_NOTHROW(tiledb_array_delete(ctx.context, TEST_URI));
  REQUIRE_NOTHROW(tiledb_dimension_free(&d1));
  CHECK(d1 == nullptr);
  REQUIRE_NOTHROW(tiledb_dimension_free(&d2));
  CHECK(d2 == nullptr);
  REQUIRE_NOTHROW(tiledb_domain_free(&domain));
  CHECK(domain == nullptr);
  REQUIRE_NOTHROW(tiledb_attribute_free(&attr));
  CHECK(attr == nullptr);
  REQUIRE_NOTHROW(tiledb_array_schema_free(&schema));
  CHECK(schema == nullptr);
}

TEST_CASE("C API: tiledb_array_open argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  SECTION("success") {
    rc = tiledb_array_open(x.ctx(), x.array, TILEDB_WRITE);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_open(nullptr, x.array, TILEDB_WRITE);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_open(x.ctx(), nullptr, TILEDB_WRITE);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid query_type") {
    rc = tiledb_array_open(x.ctx(), x.array, tiledb_query_type_t(12));
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_array_is_open argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array_without_schema x{};
  int32_t is_open;
  SECTION("success") {
    rc = tiledb_array_is_open(x.ctx(), x.array, &is_open);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_open == 0);  // array is closed.
  }
  SECTION("null context") {
    rc = tiledb_array_is_open(nullptr, x.array, &is_open);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_is_open(x.ctx(), nullptr, &is_open);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_open") {
    rc = tiledb_array_is_open(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_array_close argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  SECTION("success") {
    rc = tiledb_array_close(x.ctx(), x.array);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_close(nullptr, x.array);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_close(x.ctx(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_array_reopen argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  REQUIRE_NOTHROW(tiledb_array_open(x.ctx(), x.array, TILEDB_READ));
  SECTION("success") {
    rc = tiledb_array_reopen(x.ctx(), x.array);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_reopen(nullptr, x.array);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_reopen(x.ctx(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_array_delete argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  SECTION("success") {
    rc = tiledb_array_delete(x.ctx(), x.uri());
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_delete(nullptr, x.uri());
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    rc = tiledb_array_delete(x.ctx(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_delete_fragments_v2 argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  SECTION("success") {
    rc = tiledb_array_delete_fragments_v2(x.ctx(), x.uri(), 0, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_delete_fragments_v2(nullptr, x.uri(), 0, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    rc = tiledb_array_delete_fragments_v2(x.ctx(), nullptr, 0, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid timestamp_start" or "invalid timestamp_end" sections here;
   * All values will resolve to valid uint64_t timestamps.
   */
}

TEST_CASE(
    "C API: tiledb_array_delete_fragments_list argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_context ctx{};
  const char* fragment_uris[1] = {"unit_capi_array/__fragments/fragment_uri"};
  /*
   * No "success" section here; too much overhead to set up.
   */
  SECTION("null context") {
    rc =
        tiledb_array_delete_fragments_list(nullptr, TEST_URI, fragment_uris, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    rc = tiledb_array_delete_fragments_list(
        ctx.context, nullptr, fragment_uris, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("empty uri") {
    const char* empty_uri = "";
    rc = tiledb_array_delete_fragments_list(
        ctx.context, empty_uri, fragment_uris, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null fragment_uris") {
    const char* null_fragment_uris[1] = {nullptr};
    rc = tiledb_array_delete_fragments_list(
        ctx.context, TEST_URI, null_fragment_uris, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("empty uri") {
    const char* empty_fragment_uris[1] = {""};
    rc = tiledb_array_delete_fragments_list(
        ctx.context, TEST_URI, empty_fragment_uris, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid num_fragments") {
    rc = tiledb_array_delete_fragments_list(
        ctx.context, TEST_URI, fragment_uris, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_set_config argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array_without_schema x{};

  tiledb_config_handle_t* config;
  tiledb_error_handle_t* err;
  rc = tiledb_config_alloc(&config, &err);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("success") {
    rc = tiledb_array_set_config(x.ctx(), x.array, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_set_config(nullptr, x.array, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_set_config(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_array_set_config(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }

  REQUIRE_NOTHROW(tiledb_config_free(&config));
  CHECK(config == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_set_open_timestamp_start argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array_without_schema x{};
  SECTION("success") {
    rc = tiledb_array_set_open_timestamp_start(x.ctx(), x.array, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_set_open_timestamp_start(nullptr, x.array, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_set_open_timestamp_start(x.ctx(), nullptr, 0);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid timestamp" section here;
   * All values will resolve to valid uint64_t timestamps.
   */
}

TEST_CASE(
    "C API: tiledb_array_set_open_timestamp_end argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array_without_schema x{};
  SECTION("success") {
    rc = tiledb_array_set_open_timestamp_end(x.ctx(), x.array, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_set_open_timestamp_end(nullptr, x.array, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_set_open_timestamp_end(x.ctx(), nullptr, 1);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid timestamp" section here;
   * All values will resolve to valid uint64_t timestamps.
   */
}

TEST_CASE(
    "C API: tiledb_array_get_config argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  tiledb_config_t* config;
  SECTION("success") {
    rc = tiledb_array_get_config(x.ctx(), x.array, &config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_get_config(nullptr, x.array, &config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_config(x.ctx(), nullptr, &config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_array_get_config(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_open_timestamp_start argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  uint64_t timestamp;
  SECTION("success") {
    rc = tiledb_array_get_open_timestamp_start(x.ctx(), x.array, &timestamp);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_get_open_timestamp_start(nullptr, x.array, &timestamp);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_open_timestamp_start(x.ctx(), nullptr, &timestamp);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null timestamp") {
    rc = tiledb_array_get_open_timestamp_start(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_open_timestamp_end argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  uint64_t timestamp;
  SECTION("success") {
    rc = tiledb_array_get_open_timestamp_end(x.ctx(), x.array, &timestamp);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_get_open_timestamp_end(nullptr, x.array, &timestamp);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_open_timestamp_end(x.ctx(), nullptr, &timestamp);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null timestamp") {
    rc = tiledb_array_get_open_timestamp_end(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_schema argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  tiledb_array_schema_t* schema;
  x.open();  // the array must be open to retrieve the schema.
  SECTION("success") {
    rc = tiledb_array_get_schema(x.ctx(), x.array, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_get_schema(nullptr, x.array, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_schema(x.ctx(), nullptr, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null schema") {
    rc = tiledb_array_get_schema(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_query_type argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  tiledb_query_type_t query_type;
  SECTION("success") {
    rc = tiledb_array_get_query_type(x.ctx(), x.array, &query_type);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_get_query_type(nullptr, x.array, &query_type);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_query_type(x.ctx(), nullptr, &query_type);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null query_type") {
    rc = tiledb_array_get_query_type(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_array_get_uri argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  const char* array_uri;
  SECTION("success") {
    rc = tiledb_array_get_uri(x.ctx(), x.array, &array_uri);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_get_uri(nullptr, x.array, &array_uri);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_uri(x.ctx(), nullptr, &array_uri);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    rc = tiledb_array_get_uri(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_upgrade_version argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};

  tiledb_config_handle_t* config;
  tiledb_error_handle_t* err;
  rc = tiledb_config_alloc(&config, &err);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("success") {
    rc = tiledb_array_upgrade_version(x.ctx(), x.uri(), config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_upgrade_version(nullptr, x.uri(), config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("invalid uri") {
    rc = tiledb_array_upgrade_version(x.ctx(), "invalid", config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    rc = tiledb_array_upgrade_version(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    // Note: a null config is valid and in fact the default for this API.
    // In this case, the context's config will be used.
    rc = tiledb_array_upgrade_version(x.ctx(), x.uri(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }

  REQUIRE_NOTHROW(tiledb_config_free(&config));
  CHECK(config == nullptr);
}

TEST_CASE(
    "C API: tiledb_array_get_non_empty_domain argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  uint64_t domain[4];
  int32_t is_empty;
  x.open();  // the array must be open to retrieve the non-empty domain.
  SECTION("success") {
    rc =
        tiledb_array_get_non_empty_domain(x.ctx(), x.array, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_empty == 1);  // the array is empty.
  }
  SECTION("null context") {
    rc =
        tiledb_array_get_non_empty_domain(nullptr, x.array, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc =
        tiledb_array_get_non_empty_domain(x.ctx(), nullptr, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    rc =
        tiledb_array_get_non_empty_domain(x.ctx(), x.array, nullptr, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_empty") {
    rc = tiledb_array_get_non_empty_domain(x.ctx(), x.array, &domain, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_non_empty_domain_from_index argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  uint64_t domain[4];
  int32_t is_empty;
  x.open();  // the array must be open to retrieve the non-empty domain.
  SECTION("success") {
    rc = tiledb_array_get_non_empty_domain_from_index(
        x.ctx(), x.array, 0, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_empty == 1);  // the array is empty.
  }
  SECTION("null context") {
    rc = tiledb_array_get_non_empty_domain_from_index(
        nullptr, x.array, 0, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_non_empty_domain_from_index(
        x.ctx(), nullptr, 0, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid index") {
    rc = tiledb_array_get_non_empty_domain_from_index(
        x.ctx(), x.array, 7, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    rc = tiledb_array_get_non_empty_domain_from_index(
        x.ctx(), x.array, 0, nullptr, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_empty") {
    rc = tiledb_array_get_non_empty_domain_from_index(
        x.ctx(), x.array, 0, &domain, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_non_empty_domain_from_name argument validation",
    "[capi][array]") {
  // Note: the validity of this test relies on the name of ordinary_array's
  // dimension. If it changes for any reason, so should this test.
  capi_return_t rc;
  ordinary_array x{};
  const char* name = "dim";  // the name of ordinary_array's dimension.
  uint64_t domain[4];
  int32_t is_empty;
  x.open();  // the array must be open to retrieve the non-empty domain.
  SECTION("success") {
    rc = tiledb_array_get_non_empty_domain_from_name(
        x.ctx(), x.array, name, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_empty == 1);  // the array is empty.
  }
  SECTION("null context") {
    rc = tiledb_array_get_non_empty_domain_from_name(
        nullptr, x.array, name, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_non_empty_domain_from_name(
        x.ctx(), nullptr, name, &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid name") {
    rc = tiledb_array_get_non_empty_domain_from_name(
        x.ctx(), x.array, "invalid", &domain, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    rc = tiledb_array_get_non_empty_domain_from_name(
        x.ctx(), x.array, name, nullptr, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_empty") {
    rc = tiledb_array_get_non_empty_domain_from_name(
        x.ctx(), x.array, name, &domain, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_non_empty_domain_var_size_from_index argument "
    "validation",
    "[capi][array]") {
  capi_return_t rc;
  bool var_size = true;
  ordinary_array x{var_size};  // make the dimensions var-sized.
  uint64_t start_size, end_size;
  int32_t is_empty;
  x.open();  // the array must be open to retrieve the non-empty domain.
  SECTION("success") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.array, 0, &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_empty == 1);  // the array is empty.
  }
  SECTION("null context") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_index(
        nullptr, x.array, 0, &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_index(
        x.ctx(), nullptr, 0, &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid index") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.array, 7, &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.array, 0, nullptr, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.array, 0, &start_size, nullptr, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_empty") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.array, 0, &start_size, &end_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_non_empty_domain_var_size_from_name argument "
    "validation",
    "[capi][array]") {
  // Note: the validity of this test relies on the name of ordinary_array's
  // dimension. If it changes for any reason, so should this test.
  capi_return_t rc;
  bool var_size = true;
  ordinary_array x{var_size};  // make the dimensions var-sized.
  const char* name = "dim";    // the name of ordinary_array's dimension.
  uint64_t start_size, end_size;
  int32_t is_empty;
  x.open();  // the array must be open to retrieve the non-empty domain.
  SECTION("success") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.array, name, &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_empty == 1);  // the array is empty.
  }
  SECTION("null context") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_name(
        nullptr, x.array, name, &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_name(
        x.ctx(), nullptr, name, &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid name") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.array, "invalid", &start_size, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.array, name, nullptr, &end_size, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.array, name, &start_size, nullptr, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_empty") {
    rc = tiledb_array_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.array, name, &start_size, &end_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_non_empty_domain_var_from_index argument "
    "validation",
    "[capi][array]") {
  capi_return_t rc;
  bool var_size = true;
  ordinary_array x{var_size};  // make the dimensions var-sized.
  int start, end;
  int32_t is_empty;
  x.open();  // the array must be open to retrieve the non-empty domain.
  SECTION("success") {
    rc = tiledb_array_get_non_empty_domain_var_from_index(
        x.ctx(), x.array, 0, &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_empty == 1);  // the array is empty.
  }
  SECTION("null context") {
    rc = tiledb_array_get_non_empty_domain_var_from_index(
        nullptr, x.array, 0, &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_non_empty_domain_var_from_index(
        x.ctx(), nullptr, 0, &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid index") {
    rc = tiledb_array_get_non_empty_domain_var_from_index(
        x.ctx(), x.array, 7, &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_array_get_non_empty_domain_var_from_index(
        x.ctx(), x.array, 0, nullptr, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_array_get_non_empty_domain_var_from_index(
        x.ctx(), x.array, 0, &start, nullptr, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_empty") {
    rc = tiledb_array_get_non_empty_domain_var_from_index(
        x.ctx(), x.array, 0, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_non_empty_domain_var_from_name argument "
    "validation",
    "[capi][array]") {
  // Note: the validity of this test relies on the name of ordinary_array's
  // dimension. If it changes for any reason, so should this test.
  capi_return_t rc;
  bool var_size = true;
  ordinary_array x{var_size};  // make the dimensions var-sized.
  const char* name = "dim";    // the name of ordinary_array's dimension.
  int start, end;
  int32_t is_empty;
  x.open();  // the array must be open to retrieve the non-empty domain.
  SECTION("success") {
    rc = tiledb_array_get_non_empty_domain_var_from_name(
        x.ctx(), x.array, name, &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_empty == 1);  // the array is empty.
  }
  SECTION("null context") {
    rc = tiledb_array_get_non_empty_domain_var_from_name(
        nullptr, x.array, name, &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_non_empty_domain_var_from_name(
        x.ctx(), nullptr, name, &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid name") {
    rc = tiledb_array_get_non_empty_domain_var_from_name(
        x.ctx(), x.array, "invalid", &start, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_array_get_non_empty_domain_var_from_name(
        x.ctx(), x.array, name, nullptr, &end, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_array_get_non_empty_domain_var_from_name(
        x.ctx(), x.array, name, &start, nullptr, &is_empty);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null is_empty") {
    rc = tiledb_array_get_non_empty_domain_var_from_name(
        x.ctx(), x.array, name, &start, &end, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_encryption_type argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  tiledb_encryption_type_t enc_type;
  SECTION("success") {
    rc = tiledb_array_encryption_type(x.ctx(), x.uri(), &enc_type);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(enc_type == TILEDB_NO_ENCRYPTION);
  }
  SECTION("null context") {
    rc = tiledb_array_encryption_type(nullptr, x.uri(), &enc_type);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null uri") {
    rc = tiledb_array_encryption_type(x.ctx(), nullptr, &enc_type);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null encryption_type") {
    rc = tiledb_array_encryption_type(x.ctx(), x.uri(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_put_metadata argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  x.open(TILEDB_WRITE);  // the array must be open for modification.
  const char* key = "key";
  int v = 5;
  SECTION("success") {
    rc = tiledb_array_put_metadata(x.ctx(), x.array, key, TILEDB_INT32, 1, &v);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_put_metadata(nullptr, x.array, key, TILEDB_INT32, 1, &v);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_put_metadata(x.ctx(), nullptr, key, TILEDB_INT32, 1, &v);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null key") {
    rc = tiledb_array_put_metadata(
        x.ctx(), x.array, nullptr, TILEDB_INT32, 1, &v);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid value_type") {
    // There is not yet support for metadata of type `ANY`.
    rc = tiledb_array_put_metadata(x.ctx(), x.array, key, TILEDB_ANY, 1, &v);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  /**
   * No "invalid value_num" or "null value" sections here;
   * All values of value_num are considered valid.
   * A value of nullptr resolves to a value_num of 0.
   */
}

TEST_CASE(
    "C API: tiledb_array_delete_metadata argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  x.open(TILEDB_WRITE);  // the array must be open for modification.
  const char* key = "key";
  int v = 5;
  REQUIRE_NOTHROW(
      tiledb_array_put_metadata(x.ctx(), x.array, key, TILEDB_INT32, 1, &v));
  SECTION("success") {
    rc = tiledb_array_delete_metadata(x.ctx(), x.array, key);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_delete_metadata(nullptr, x.array, key);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_delete_metadata(x.ctx(), nullptr, key);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null key") {
    rc = tiledb_array_delete_metadata(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_metadata argument validation", "[capi][array]") {
  // Open the array for WRITE, put metadata, and close the array.
  capi_return_t rc;
  ordinary_array x{};
  x.open(TILEDB_WRITE);
  const char* key = "key";
  int v = 5;
  REQUIRE_NOTHROW(
      tiledb_array_put_metadata(x.ctx(), x.array, key, TILEDB_INT32, 1, &v));
  x.close();

  // Reopen the array in READ mode to retrieve the metadata.
  x.open();
  tiledb_datatype_t value_type;
  uint32_t value_num;
  const void* value;
  SECTION("success") {
    rc = tiledb_array_get_metadata(
        x.ctx(), x.array, key, &value_type, &value_num, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(value_type == TILEDB_INT32);
    CHECK(value_num == 1);
    CHECK(*static_cast<const int*>(value) == 5);
  }
  SECTION("null context") {
    rc = tiledb_array_get_metadata(
        nullptr, x.array, key, &value_type, &value_num, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_metadata(
        x.ctx(), nullptr, key, &value_type, &value_num, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null key") {
    rc = tiledb_array_get_metadata(
        x.ctx(), x.array, nullptr, &value_type, &value_num, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value_type") {
    rc = tiledb_array_get_metadata(
        x.ctx(), x.array, key, nullptr, &value_num, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value_num") {
    rc = tiledb_array_get_metadata(
        x.ctx(), x.array, key, &value_type, nullptr, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    rc = tiledb_array_get_metadata(
        x.ctx(), x.array, key, &value_type, &value_num, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_metadata_num argument validation",
    "[capi][array]") {
  // Open the array for WRITE, put metadata, and close the array.
  capi_return_t rc;
  ordinary_array x{};
  x.open(TILEDB_WRITE);
  const char* key = "key";
  uint32_t v_num = 1;
  int v = 5;
  REQUIRE_NOTHROW(tiledb_array_put_metadata(
      x.ctx(), x.array, key, TILEDB_INT32, v_num, &v));
  x.close();

  // Reopen the array in READ mode to retrieve the metadata_num.
  x.open();
  uint64_t num;
  SECTION("success") {
    rc = tiledb_array_get_metadata_num(x.ctx(), x.array, &num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(num == v_num);
  }
  SECTION("null context") {
    rc = tiledb_array_get_metadata_num(nullptr, x.array, &num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_metadata_num(x.ctx(), nullptr, &num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null num") {
    rc = tiledb_array_get_metadata_num(x.ctx(), x.array, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_metadata_from_index argument validation",
    "[capi][array]") {
  // Open the array for WRITE, put metadata, and close the array.
  capi_return_t rc;
  ordinary_array x{};
  x.open(TILEDB_WRITE);
  uint32_t v_num = 1;
  int v = 5;
  REQUIRE_NOTHROW(tiledb_array_put_metadata(
      x.ctx(), x.array, "key", TILEDB_INT32, v_num, &v));
  x.close();

  // Reopen the array in READ mode to retrieve the metadata.
  x.open();
  uint64_t index = 0;
  const char* key;
  uint32_t key_len;
  tiledb_datatype_t value_type;
  uint32_t value_num;
  const void* value;

  SECTION("success") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(),
        x.array,
        index,
        &key,
        &key_len,
        &value_type,
        &value_num,
        &value);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(strcmp(key, "key") == 0);
    CHECK(value_type == TILEDB_INT32);
    CHECK(value_num == v_num);
    CHECK(*static_cast<const int*>(value) == v);
  }
  SECTION("null context") {
    rc = tiledb_array_get_metadata_from_index(
        nullptr,
        x.array,
        index,
        &key,
        &key_len,
        &value_type,
        &value_num,
        &value);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(),
        nullptr,
        index,
        &key,
        &key_len,
        &value_type,
        &value_num,
        &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid index") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(), x.array, 7, &key, &key_len, &value_type, &value_num, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null key") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(),
        x.array,
        index,
        nullptr,
        &key_len,
        &value_type,
        &value_num,
        &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null key_len") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(),
        x.array,
        index,
        &key,
        nullptr,
        &value_type,
        &value_num,
        &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value_type") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(), x.array, index, &key, &key_len, nullptr, &value_num, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value_num") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(), x.array, index, &key, &key_len, &value_type, nullptr, &value);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    rc = tiledb_array_get_metadata_from_index(
        x.ctx(),
        x.array,
        index,
        &key,
        &key_len,
        &value_type,
        &value_num,
        nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_has_metadata_key argument validation",
    "[capi][array]") {
  // Open the array for WRITE, put metadata, and close the array.
  capi_return_t rc;
  ordinary_array x{};
  x.open(TILEDB_WRITE);
  const char* key = "key";
  int v = 5;
  REQUIRE_NOTHROW(
      tiledb_array_put_metadata(x.ctx(), x.array, key, TILEDB_INT32, 1, &v));
  x.close();

  // Reopen the array in READ mode to check if the key is set.
  x.open();
  tiledb_datatype_t value_type;
  int32_t has_key;

  SECTION("success") {
    rc = tiledb_array_has_metadata_key(
        x.ctx(), x.array, key, &value_type, &has_key);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(value_type == TILEDB_INT32);
    CHECK(has_key == 1);  // the array has the key.
  }
  SECTION("null context") {
    rc = tiledb_array_has_metadata_key(
        nullptr, x.array, key, &value_type, &has_key);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_has_metadata_key(
        x.ctx(), nullptr, key, &value_type, &has_key);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null key") {
    rc = tiledb_array_has_metadata_key(
        x.ctx(), x.array, nullptr, &value_type, &has_key);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value_type") {
    rc =
        tiledb_array_has_metadata_key(x.ctx(), x.array, key, nullptr, &has_key);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null has_key") {
    rc = tiledb_array_has_metadata_key(
        x.ctx(), x.array, key, &value_type, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_array_evolve argument validation", "[capi][array]") {
  capi_return_t rc;
  ordinary_array_without_schema x{};
  tiledb_array_schema_evolution_t schema_evo;
  /*
   * No "success" section here; too much overhead to set up.
   * This test cannot yet invoke tiledb_array_schema_evolution_alloc without
   * introducing cyclic dependendencies.
   */
  SECTION("null context") {
    rc = tiledb_array_evolve(nullptr, x.uri(), &schema_evo);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_evolve(x.ctx(), nullptr, &schema_evo);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null array_schema_evolution") {
    rc = tiledb_array_evolve(x.ctx(), x.uri(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_get_enumeration argument validation",
    "[capi][array]") {
  // Note: the validity of this test relies on the name of ordinary_array's
  // attribute. If it changes for any reason, so should this test.
  capi_return_t rc;
  ordinary_array x{};
  const char* name = "attr";  // the name of ordinary_array's attribute.
  tiledb_enumeration_t* enumeration;
  /*
   * No "success" section here; too much overhead to set up.
   */
  SECTION("null context") {
    rc = tiledb_array_get_enumeration(nullptr, x.array, name, &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_get_enumeration(x.ctx(), nullptr, name, &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_array_get_enumeration(x.ctx(), x.array, nullptr, &enumeration);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null enumeration") {
    rc = tiledb_array_get_enumeration(x.ctx(), x.array, name, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_array_load_all_enumerations argument validation",
    "[capi][array]") {
  capi_return_t rc;
  ordinary_array x{};
  x.open();  // the array must be open to load all enumerations.
  SECTION("success") {
    rc = tiledb_array_load_all_enumerations(x.ctx(), x.array);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_array_load_all_enumerations(nullptr, x.array);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null array") {
    rc = tiledb_array_load_all_enumerations(x.ctx(), nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}
