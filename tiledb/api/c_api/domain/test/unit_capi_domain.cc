/**
 * @file tiledb/api/c_api/dimension/test/unit_capi_domain.cc
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
#include "../domain_api_external.h"
#include "../domain_api_internal.h"

using namespace tiledb::api::test_support;

TEST_CASE("C API: tiledb_domain_alloc argument validation", "[capi][domain]") {
  ordinary_context ctx{};
  tiledb_domain_t* domain;
  SECTION("success") {
    capi_return_t rc{tiledb_domain_alloc(ctx.context, &domain)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    tiledb_domain_free(&domain);
  }
  SECTION("null context") {
    capi_return_t rc{tiledb_domain_alloc(nullptr, &domain)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    capi_return_t rc{tiledb_domain_alloc(ctx.context, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_domain_free argument validation", "[capi][domain]") {
  SECTION("Success") {
    ordinary_context ctx{};
    tiledb_domain_t* domain;
    capi_return_t rc{tiledb_domain_alloc(ctx.context, &domain)};
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_domain_free(&domain));
    CHECK(domain == nullptr);
  }
  SECTION("null domain") {
    REQUIRE_NOTHROW(tiledb_domain_free(nullptr));
  }
}

struct ordinary_domain {
  ordinary_context ctx;
  ordinary_dimension_d1 dim1;
  tiledb_domain_handle_t* domain;
  ordinary_domain()
      : dim1{ctx.context} {
    capi_return_t rc{tiledb_domain_alloc(ctx.context, &domain)};
    if (capi_status_t(rc) != TILEDB_OK) {
      throw std::runtime_error("error creating test domain");
    }
    rc = tiledb_domain_add_dimension(ctx.context, domain, dim1.dimension);
    if (capi_status_t(rc) != TILEDB_OK) {
      throw std::runtime_error("error adding first dimension to test domain");
    }
  }
};

TEST_CASE(
    "C API: tiledb_domain_get_type argument validation", "[capi][domain]") {
  ordinary_domain dom{};
  auto ctx{dom.ctx.context};
  tiledb_datatype_t out_type;
  SECTION("success") {
    capi_return_t rc{tiledb_domain_get_type(ctx, dom.domain, &out_type)};
    CHECK(rc == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc{tiledb_domain_get_type(nullptr, dom.domain, &out_type)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    capi_return_t rc{tiledb_domain_get_type(ctx, nullptr, &out_type)};
    CHECK(rc == TILEDB_ERR);
  }
  SECTION("null type") {
    capi_return_t rc{tiledb_domain_get_type(ctx, dom.domain, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_domain_get_ndim argument validation", "[capi][domain]") {
  ordinary_domain dom{};
  auto ctx{dom.ctx.context};
  uint32_t out_ndim;
  SECTION("success") {
    capi_return_t rc{tiledb_domain_get_ndim(ctx, dom.domain, &out_ndim)};
    CHECK(rc == TILEDB_OK);
  }
  SECTION("null context") {
    capi_return_t rc{tiledb_domain_get_ndim(nullptr, dom.domain, &out_ndim)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    capi_return_t rc{tiledb_domain_get_ndim(ctx, nullptr, &out_ndim)};
    CHECK(rc == TILEDB_ERR);
  }
  SECTION("null number of dimensions") {
    capi_return_t rc{tiledb_domain_get_ndim(ctx, dom.domain, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_domain_add_dimension argument validation",
    "[capi][domain]") {
  ordinary_domain dom{};
  auto ctx{dom.ctx.context};
  ordinary_dimension_d2 dim2{ctx};
  SECTION("success") {
    auto rc{tiledb_domain_add_dimension(ctx, dom.domain, dim2.dimension)};
    CHECK(rc == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_domain_add_dimension(nullptr, dom.domain, dim2.dimension)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    auto rc{tiledb_domain_add_dimension(ctx, nullptr, dim2.dimension)};
    CHECK(rc == TILEDB_ERR);
  }
  SECTION("null dimension") {
    auto rc{tiledb_domain_add_dimension(ctx, dom.domain, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_domain_get_dimension_from_index argument validation",
    "[capi][domain]") {
  ordinary_domain dom{};
  auto ctx{dom.ctx.context};
  tiledb_dimension_handle_t* dim;
  SECTION("success") {
    auto rc{tiledb_domain_get_dimension_from_index(ctx, dom.domain, 0, &dim)};
    CHECK(rc == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{
        tiledb_domain_get_dimension_from_index(nullptr, dom.domain, 0, &dim)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    auto rc{tiledb_domain_get_dimension_from_index(ctx, nullptr, 0, &dim)};
    CHECK(rc == TILEDB_ERR);
  }
  SECTION("invalid index") {
    auto rc{tiledb_domain_get_dimension_from_index(
        ctx, dom.domain, static_cast<uint32_t>(-1), &dim)};
    CHECK(rc == TILEDB_ERR);
  }
  SECTION("null dimension") {
    auto rc{
        tiledb_domain_get_dimension_from_index(ctx, dom.domain, 0, nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_domain_get_dimension_from_name argument validation",
    "[capi][domain]") {
  ordinary_domain dom{};
  auto ctx{dom.ctx.context};
  tiledb_dimension_handle_t* dim;
  SECTION("success") {
    auto rc{tiledb_domain_get_dimension_from_name(ctx, dom.domain, "d1", &dim)};
    CHECK(rc == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{
        tiledb_domain_get_dimension_from_name(nullptr, dom.domain, "d1", &dim)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    auto rc{tiledb_domain_get_dimension_from_name(ctx, nullptr, "d1", &dim)};
    CHECK(rc == TILEDB_ERR);
  }
  // SECTION("null name") is omitted. Empty dimension names are permitted(!).
  SECTION("null dimension") {
    auto rc{
        tiledb_domain_get_dimension_from_name(ctx, dom.domain, "d1", nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_domain_has_dimension argument validation",
    "[capi][domain]") {
  ordinary_domain dom{};
  auto ctx{dom.ctx.context};
  int32_t out_has_dim;
  SECTION("success") {
    auto rc{tiledb_domain_has_dimension(ctx, dom.domain, "d1", &out_has_dim)};
    CHECK(rc == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{
        tiledb_domain_has_dimension(nullptr, dom.domain, "d1", &out_has_dim)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    auto rc{tiledb_domain_has_dimension(ctx, nullptr, "d1", &out_has_dim)};
    CHECK(rc == TILEDB_ERR);
  }
  // SECTION("null name") is omitted. Empty dimension names are permitted(!).
  SECTION("null has_dim") {
    auto rc{tiledb_domain_has_dimension(ctx, dom.domain, "d1", nullptr)};
    CHECK(rc == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_domain_dump argument validation", "[capi][domain]") {
  ordinary_domain dom{};
  auto ctx{dom.ctx.context};
  /*
   * SECTION("success") is omitted. This function is not conducive to a cross-
   * platform test, as it requires a FILE *.
   */
  SECTION("null context") {
    tiledb_string_t* tdb_string;
    auto rc{tiledb_domain_dump_str(nullptr, dom.domain, &tdb_string)};
    CHECK(rc == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null domain") {
    tiledb_string_t* tdb_string;
    auto rc{tiledb_domain_dump_str(ctx, nullptr, &tdb_string)};
    CHECK(rc == TILEDB_ERR);
  }
  // SECTION("null FILE") is omitted. Null FILE* defaults to stderr
}
