/**
 * @file unit-capi-query-condiiton-create.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests the C API for query condition creation functions
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"

TEST_CASE(
    "C API: Test QueryCondition value creation",
    "[capi][query-condition][create]") {
  tiledb_ctx_t* ctx;
  auto rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  const char* name = "attr";
  int32_t value = 42;

  tiledb_query_condition_t* cond;

  SECTION("ctx is nullptr") {
    rc = tiledb_query_condition_create_value(
        nullptr, name, &value, sizeof(value), TILEDB_EQ, &cond);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
  }

  SECTION("name is nullptr") {
    rc = tiledb_query_condition_create_value(
        ctx, nullptr, &value, sizeof(value), TILEDB_EQ, &cond);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("value is nullptr when size > 0") {
    rc = tiledb_query_condition_create_value(
        ctx, name, nullptr, sizeof(value), TILEDB_EQ, &cond);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("cond is nullptr") {
    rc = tiledb_query_condition_create_value(
        ctx, name, &value, sizeof(value), TILEDB_EQ, nullptr);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("success") {
    rc = tiledb_query_condition_create_value(
        ctx, name, &value, sizeof(value), TILEDB_EQ, &cond);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_condition_free(&cond);
  }

  SECTION("success with nullptr for value and 0 for size") {
    rc = tiledb_query_condition_create_value(
        ctx, name, nullptr, 0, TILEDB_EQ, &cond);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_condition_free(&cond);
  }
}

TEST_CASE(
    "C API: Test QueryCondition expression creation",
    "[capi][query-condition][create]") {
  tiledb_ctx_t* ctx;
  auto rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  int32_t v1 = 2;
  int32_t v2 = 3;
  int32_t v3 = 5;

  tiledb_query_condition_t* qc1;
  tiledb_query_condition_t* qc2;
  tiledb_query_condition_t* qc3;

  rc = tiledb_query_condition_create_value(
      ctx, "a1", &v1, sizeof(v1), TILEDB_EQ, &qc1);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_condition_create_value(
      ctx, "a2", &v2, sizeof(v2), TILEDB_EQ, &qc2);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_condition_create_value(
      ctx, "a3", &v3, sizeof(v3), TILEDB_EQ, &qc3);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_condition_t* cond_list[3] = {qc1, qc2, qc3};

  tiledb_query_condition_t* cond;

  SECTION("ctx is nullptr") {
    rc = tiledb_query_condition_create_expression(
        nullptr, cond_list, 3, TILEDB_AND, &cond);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
  }

  SECTION("cond_list is nullptr") {
    rc = tiledb_query_condition_create_expression(
        ctx, nullptr, 3, TILEDB_AND, &cond);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("cond_list has nullptr element") {
    tiledb_query_condition_t* cond_list2[3] = {qc1, nullptr, qc2};
    rc = tiledb_query_condition_create_expression(
        ctx, cond_list2, 3, TILEDB_AND, &cond);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("cond_list has more than 1 element for TILEDB_NOT") {
    tiledb_query_condition_t* cond_list2[2] = {qc1, qc2};
    rc = tiledb_query_condition_create_expression(
        ctx, cond_list2, 2, TILEDB_NOT, &cond);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("cond_list has fewer than 2 elements for TILEDB_AND") {
    tiledb_query_condition_t* cond_list2[1] = {qc1};
    rc = tiledb_query_condition_create_expression(
        ctx, cond_list2, 1, TILEDB_AND, &cond);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("cond_list has fewer than 2 elements for TILEDB_OR") {
    tiledb_query_condition_t* cond_list2[1] = {qc1};
    rc = tiledb_query_condition_create_expression(
        ctx, cond_list2, 1, TILEDB_OR, &cond);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("cond is nullptr") {
    rc = tiledb_query_condition_create_expression(
        ctx, cond_list, 3, TILEDB_AND, nullptr);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("sucesss") {
    rc = tiledb_query_condition_create_expression(
        ctx, cond_list, 3, TILEDB_AND, &cond);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_condition_free(&cond);
  }
}
