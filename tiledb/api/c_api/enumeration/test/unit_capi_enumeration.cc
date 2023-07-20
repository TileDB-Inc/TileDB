/**
 * @file tiledb/api/c_api/enumeration/test/unit_capi_enumeration.cc
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
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../enumeration_api_experimental.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_context.h"
#include "tiledb/sm/misc/constants.h"

// TILEDB_VAR_NUM is defined in tiledb.h which we can't use without linking
// against the entire libtiledb. Instead we'll just declare our own.
#ifndef TILEDB_VAR_NUM
#define TILEDB_VAR_NUM tiledb::sm::constants::var_num
#endif

using namespace tiledb::api::test_support;

struct FixedSizeEnumeration {
  FixedSizeEnumeration() {
    uint32_t values[5] = {1, 2, 3, 4, 5};
    auto rc = tiledb_enumeration_alloc(
        ctx_.context,
        "an_enumeration",
        TILEDB_UINT32,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration_);
    REQUIRE(rc == TILEDB_OK);
  }

  ~FixedSizeEnumeration() {
    tiledb_enumeration_free(&enumeration_);
  }

  ordinary_context ctx_;
  tiledb_enumeration_t* enumeration_;
};

struct VarSizeEnumeration {
  VarSizeEnumeration() {
    const char* values = "foobarbazbingobango";
    uint64_t offsets[5] = {0, 3, 6, 9, 14};
    auto rc = tiledb_enumeration_alloc(
        ctx_.context,
        "an_enumeration",
        TILEDB_STRING_UTF8,
        TILEDB_VAR_NUM,
        0,
        values,
        strlen(values),
        offsets,
        5 * sizeof(uint64_t),
        &enumeration_);
    REQUIRE(rc == TILEDB_OK);
  }

  ~VarSizeEnumeration() {
    tiledb_enumeration_free(&enumeration_);
  }

  ordinary_context ctx_;
  tiledb_enumeration_t* enumeration_;
};

TEST_CASE(
    "C API: tiledb_enumeration_alloc argument validation",
    "[capi][enumeration]") {
  ordinary_context ctx{};
  tiledb_enumeration_t* enumeration = nullptr;

  int32_t values[5] = {1, 2, 3, 4, 5};
  const char* data = "foobarbazbingobango";
  uint64_t offsets[5] = {0, 3, 6, 9, 14};

  SECTION("success - fixed size") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        TILEDB_UINT32,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration);
    REQUIRE(rc == TILEDB_OK);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("success - var size") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        TILEDB_STRING_ASCII,
        TILEDB_VAR_NUM,
        0,
        (void*)data,
        strlen(data),
        offsets,
        sizeof(uint64_t) * 5,
        &enumeration);
    REQUIRE(rc == TILEDB_OK);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - null context") {
    auto rc = tiledb_enumeration_alloc(
        nullptr,
        "an_enumeration",
        TILEDB_UINT32,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - invalid name") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        nullptr,
        TILEDB_UINT32,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - invalid datatype") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        (tiledb_datatype_t)255,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - data nullptr") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        TILEDB_INT32,
        1,
        0,
        nullptr,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        &enumeration);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - data_size == 0") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        TILEDB_INT32,
        1,
        0,
        values,
        0,
        nullptr,
        0,
        &enumeration);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - enumeration nullptr") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        TILEDB_INT32,
        1,
        0,
        values,
        sizeof(uint32_t) * 5,
        nullptr,
        0,
        nullptr);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - offsets nullptr") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        TILEDB_STRING_ASCII,
        TILEDB_VAR_NUM,
        0,
        (void*)data,
        strlen(data),
        nullptr,
        sizeof(uint64_t) * 5,
        &enumeration);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_enumeration_free(&enumeration);
  }

  SECTION("failure - offsets_size == 0") {
    auto rc = tiledb_enumeration_alloc(
        ctx.context,
        "an_enumeration",
        TILEDB_STRING_ASCII,
        TILEDB_VAR_NUM,
        0,
        (void*)data,
        strlen(data),
        offsets,
        0,
        &enumeration);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_enumeration_free(&enumeration);
  }
}

TEST_CASE(
    "C API: tiledb_enumeration_free argument validation",
    "[capi][enumeration]") {
  REQUIRE_NOTHROW(tiledb_enumeration_free(nullptr));
}

TEST_CASE(
    "C API: tiledb_enumeration_get_type argument validation",
    "[capi][enumeration]") {
  FixedSizeEnumeration fe;
  VarSizeEnumeration ve;
  tiledb_datatype_t dt;

  SECTION("success") {
    auto rc =
        tiledb_enumeration_get_type(fe.ctx_.context, fe.enumeration_, &dt);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(dt == TILEDB_UINT32);

    rc = tiledb_enumeration_get_type(ve.ctx_.context, ve.enumeration_, &dt);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(dt == TILEDB_STRING_UTF8);
  }

  SECTION("failure - invalid context") {
    auto rc = tiledb_enumeration_get_type(nullptr, fe.enumeration_, &dt);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
  }

  SECTION("failure - invalid enumeration") {
    auto rc = tiledb_enumeration_get_type(fe.ctx_.context, nullptr, &dt);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("failure - invalid type pointer") {
    auto rc =
        tiledb_enumeration_get_type(fe.ctx_.context, fe.enumeration_, nullptr);
    REQUIRE(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_enumeration_get_cell_val_num argument validation",
    "[capi][enumeration]") {
  FixedSizeEnumeration fe;
  VarSizeEnumeration ve;
  uint32_t cvn;

  SECTION("success") {
    auto rc = tiledb_enumeration_get_cell_val_num(
        fe.ctx_.context, fe.enumeration_, &cvn);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(cvn == 1);

    rc = tiledb_enumeration_get_cell_val_num(
        ve.ctx_.context, ve.enumeration_, &cvn);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(cvn == TILEDB_VAR_NUM);
  }

  SECTION("failure - invalid context") {
    auto rc =
        tiledb_enumeration_get_cell_val_num(nullptr, fe.enumeration_, &cvn);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
  }

  SECTION("failure - invalid enumeration") {
    auto rc =
        tiledb_enumeration_get_cell_val_num(fe.ctx_.context, nullptr, &cvn);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("failure - invalid cell_val_num pointer") {
    auto rc = tiledb_enumeration_get_cell_val_num(
        fe.ctx_.context, fe.enumeration_, nullptr);
    REQUIRE(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_enumeration_get_ordered argument validation",
    "[capi][enumeration]") {
  FixedSizeEnumeration fe;
  VarSizeEnumeration ve;
  int o;

  SECTION("success") {
    auto rc =
        tiledb_enumeration_get_ordered(fe.ctx_.context, fe.enumeration_, &o);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(!o);

    rc = tiledb_enumeration_get_ordered(ve.ctx_.context, ve.enumeration_, &o);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(!o);
  }

  SECTION("failure - invalid context") {
    auto rc = tiledb_enumeration_get_ordered(nullptr, fe.enumeration_, &o);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
  }

  SECTION("failure - invalid enumeration") {
    auto rc = tiledb_enumeration_get_ordered(fe.ctx_.context, nullptr, &o);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("failure - invalid ordered pointer") {
    auto rc = tiledb_enumeration_get_ordered(
        fe.ctx_.context, fe.enumeration_, nullptr);
    REQUIRE(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_enumeration_get_data argument validation",
    "[capi][enumeration]") {
  FixedSizeEnumeration fe;
  VarSizeEnumeration ve;
  const void* d;
  uint64_t ds;

  uint32_t fixed_expect[5] = {1, 2, 3, 4, 5};
  const char* var_expect = "foobarbazbingobango";

  SECTION("success") {
    auto rc =
        tiledb_enumeration_get_data(fe.ctx_.context, fe.enumeration_, &d, &ds);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(std::memcmp(fixed_expect, d, sizeof(uint32_t) * 5) == 0);
    REQUIRE(ds == sizeof(uint32_t) * 5);

    rc = tiledb_enumeration_get_data(ve.ctx_.context, ve.enumeration_, &d, &ds);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(std::memcmp(var_expect, d, strlen(var_expect)) == 0);
    REQUIRE(ds == strlen(var_expect));
  }

  SECTION("failure - invalid context") {
    auto rc = tiledb_enumeration_get_data(nullptr, fe.enumeration_, &d, &ds);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
  }

  SECTION("failure - invalid enumeration") {
    auto rc = tiledb_enumeration_get_data(fe.ctx_.context, nullptr, &d, &ds);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("failure - invalid data pointer") {
    auto rc = tiledb_enumeration_get_data(
        fe.ctx_.context, fe.enumeration_, nullptr, &ds);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("failure - invalid data size pointer") {
    auto rc = tiledb_enumeration_get_data(
        fe.ctx_.context, fe.enumeration_, &d, nullptr);
    REQUIRE(rc == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_enumeration_get_offsets argument validation",
    "[capi][enumeration]") {
  FixedSizeEnumeration fe;
  VarSizeEnumeration ve;
  const void* o;
  uint64_t os;

  uint64_t var_expect[5] = {0, 3, 6, 9, 14};

  SECTION("success") {
    auto rc = tiledb_enumeration_get_offsets(
        fe.ctx_.context, fe.enumeration_, &o, &os);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(o == nullptr);
    REQUIRE(os == 0);

    rc = tiledb_enumeration_get_offsets(
        ve.ctx_.context, ve.enumeration_, &o, &os);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(std::memcmp(var_expect, o, sizeof(uint64_t) * 5) == 0);
    REQUIRE(os == sizeof(uint64_t) * 5);
  }

  SECTION("failure - invalid context") {
    auto rc = tiledb_enumeration_get_offsets(nullptr, fe.enumeration_, &o, &os);
    REQUIRE(rc == TILEDB_INVALID_CONTEXT);
  }

  SECTION("failure - invalid enumeration") {
    auto rc = tiledb_enumeration_get_offsets(fe.ctx_.context, nullptr, &o, &os);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("failure - invalid offsets pointer") {
    auto rc = tiledb_enumeration_get_offsets(
        fe.ctx_.context, fe.enumeration_, nullptr, &os);
    REQUIRE(rc == TILEDB_ERR);
  }

  SECTION("failure - invalid offsets size pointer") {
    auto rc = tiledb_enumeration_get_offsets(
        fe.ctx_.context, fe.enumeration_, &o, nullptr);
    REQUIRE(rc == TILEDB_ERR);
  }
}
