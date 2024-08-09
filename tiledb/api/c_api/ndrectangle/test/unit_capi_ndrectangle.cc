/**
 * @file tiledb/api/c_api/ndrectangle/test/unit_capi_ndrectangle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB Inc.
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
 * Tests the NDRectangle C API.
 */

#include <catch2/catch_test_macros.hpp>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_external.h"
#include "tiledb/api/c_api/ndrectangle/ndrectangle_api_external_experimental.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"

using namespace tiledb::test;

struct CAPINDRectangleFx : TemporaryDirectoryFixture {
  CAPINDRectangleFx() {
    create_domain();
  }
  ~CAPINDRectangleFx() {
    free_domain();
  }
  void create_domain();
  void free_domain();

  tiledb_dimension_t *d1_, *d2_;
  tiledb_domain_t* domain_;
};

void CAPINDRectangleFx::create_domain() {
  // Create dimensions
  uint64_t tile_extents[] = {2, 2};
  uint64_t dim_domain[] = {1, 10, 1, 10};

  int rc = tiledb_dimension_alloc(
      ctx, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_alloc(
      ctx, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2_);
  CHECK(rc == TILEDB_OK);

  // Create domain
  rc = tiledb_domain_alloc(ctx, &domain_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain_, d1_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain_, d2_);
  CHECK(rc == TILEDB_OK);
}

void CAPINDRectangleFx::free_domain() {
  tiledb_dimension_free(&d1_);
  tiledb_dimension_free(&d2_);
  tiledb_domain_free(&domain_);
}

TEST_CASE_METHOD(
    CAPINDRectangleFx,
    "C API: argument validation",
    "[capi][ndrectangle][args]") {
  CHECK(
      tiledb_ndrectangle_alloc(nullptr, nullptr, nullptr) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(tiledb_ndrectangle_alloc(ctx, nullptr, nullptr) == TILEDB_ERR);
  CHECK(tiledb_ndrectangle_alloc(ctx, domain_, nullptr) == TILEDB_ERR);

  CHECK(tiledb_ndrectangle_free(nullptr) == TILEDB_ERR);
  tiledb_ndrectangle_t* ndr = nullptr;
  CHECK(tiledb_ndrectangle_free(&ndr) == TILEDB_ERR);

  CHECK(
      tiledb_ndrectangle_get_range_from_name(
          nullptr, nullptr, nullptr, nullptr) == TILEDB_INVALID_CONTEXT);
  CHECK(
      tiledb_ndrectangle_get_range_from_name(ctx, nullptr, nullptr, nullptr) ==
      TILEDB_ERR);

  REQUIRE(tiledb_ndrectangle_alloc(ctx, domain_, &ndr) == TILEDB_OK);
  CHECK(
      tiledb_ndrectangle_get_range_from_name(ctx, ndr, nullptr, nullptr) ==
      TILEDB_ERR);
  CHECK(
      tiledb_ndrectangle_get_range_from_name(ctx, ndr, "dim1", nullptr) ==
      TILEDB_ERR);
  tiledb_range_t range;
  CHECK(
      tiledb_ndrectangle_get_range_from_name(ctx, ndr, "doesntexist", &range) ==
      TILEDB_ERR);

  CHECK(
      tiledb_ndrectangle_get_range(nullptr, nullptr, 0, nullptr) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(tiledb_ndrectangle_get_range(ctx, nullptr, 0, nullptr) == TILEDB_ERR);
  CHECK(tiledb_ndrectangle_get_range(ctx, ndr, 0, nullptr) == TILEDB_ERR);
  CHECK(tiledb_ndrectangle_get_range(ctx, ndr, 2, &range) == TILEDB_ERR);

  CHECK(
      tiledb_ndrectangle_set_range(nullptr, nullptr, 0, nullptr) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(tiledb_ndrectangle_set_range(ctx, nullptr, 0, nullptr) == TILEDB_ERR);
  CHECK(tiledb_ndrectangle_set_range(ctx, ndr, 0, nullptr) == TILEDB_ERR);
  CHECK(tiledb_ndrectangle_set_range(ctx, ndr, 2, &range) == TILEDB_ERR);

  CHECK(
      tiledb_ndrectangle_set_range_for_name(
          nullptr, nullptr, nullptr, nullptr) == TILEDB_INVALID_CONTEXT);
  CHECK(
      tiledb_ndrectangle_set_range_for_name(ctx, nullptr, nullptr, nullptr) ==
      TILEDB_ERR);
  CHECK(
      tiledb_ndrectangle_set_range_for_name(ctx, ndr, nullptr, nullptr) ==
      TILEDB_ERR);
  CHECK(
      tiledb_ndrectangle_set_range_for_name(ctx, ndr, "dim1", nullptr) ==
      TILEDB_ERR);
  CHECK(
      tiledb_ndrectangle_set_range_for_name(ctx, ndr, "doesntexist", &range) ==
      TILEDB_ERR);

  CHECK(
      tiledb_ndrectangle_get_dtype(nullptr, nullptr, 0, nullptr) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(tiledb_ndrectangle_get_dtype(ctx, nullptr, 0, nullptr) == TILEDB_ERR);
  CHECK(tiledb_ndrectangle_get_dtype(ctx, ndr, 0, nullptr) == TILEDB_ERR);
  tiledb_datatype_t dtype;
  CHECK(tiledb_ndrectangle_get_dtype(ctx, ndr, 2, &dtype) == TILEDB_ERR);

  CHECK(
      tiledb_ndrectangle_get_dtype_from_name(
          nullptr, nullptr, "dim1", nullptr) == TILEDB_INVALID_CONTEXT);
  CHECK(
      tiledb_ndrectangle_get_dtype_from_name(ctx, nullptr, "dim1", nullptr) ==
      TILEDB_ERR);
  CHECK(
      tiledb_ndrectangle_get_dtype_from_name(ctx, ndr, "dim1", nullptr) ==
      TILEDB_ERR);
  CHECK(
      tiledb_ndrectangle_get_dtype_from_name(ctx, ndr, "doesntexist", &dtype) ==
      TILEDB_ERR);

  CHECK(
      tiledb_ndrectangle_get_dim_num(nullptr, nullptr, nullptr) ==
      TILEDB_INVALID_CONTEXT);
  CHECK(tiledb_ndrectangle_get_dim_num(ctx, nullptr, nullptr) == TILEDB_ERR);
  CHECK(tiledb_ndrectangle_get_dim_num(ctx, ndr, nullptr) == TILEDB_ERR);

  REQUIRE(tiledb_ndrectangle_free(&ndr) == TILEDB_OK);
}

TEST_CASE_METHOD(
    CAPINDRectangleFx,
    "C API: setting and getting ranges works",
    "[capi][ndrectangle][range]") {
  tiledb_ndrectangle_t* ndr = nullptr;
  REQUIRE(tiledb_ndrectangle_alloc(ctx, domain_, &ndr) == TILEDB_OK);

  tiledb_range_t range;
  uint64_t min = 2;
  uint64_t max = 5;
  range.min = &min;
  range.min_size = sizeof(uint64_t);
  range.max = &max;
  range.max_size = sizeof(uint64_t);

  CHECK(
      tiledb_ndrectangle_set_range_for_name(ctx, ndr, "d1", &range) ==
      TILEDB_OK);

  CHECK(tiledb_ndrectangle_set_range(ctx, ndr, 1, &range) == TILEDB_OK);

  tiledb_range_t out_range_d1;

  REQUIRE(
      tiledb_ndrectangle_get_range_from_name(ctx, ndr, "d1", &out_range_d1) ==
      TILEDB_OK);
  CHECK(range.min_size == out_range_d1.min_size);
  CHECK(range.min_size == out_range_d1.min_size);
  CHECK(std::memcmp(range.min, out_range_d1.min, range.min_size) == 0);
  CHECK(std::memcmp(range.max, out_range_d1.max, range.max_size) == 0);

  tiledb_range_t out_range_d2;
  REQUIRE(
      tiledb_ndrectangle_get_range(ctx, ndr, 1, &out_range_d2) == TILEDB_OK);
  CHECK(range.min_size == out_range_d2.min_size);
  CHECK(range.min_size == out_range_d2.min_size);
  CHECK(std::memcmp(range.min, out_range_d2.min, range.min_size) == 0);
  CHECK(std::memcmp(range.max, out_range_d2.max, range.max_size) == 0);

  tiledb_datatype_t dtype;
  REQUIRE(tiledb_ndrectangle_get_dtype(ctx, ndr, 0, &dtype) == TILEDB_OK);
  CHECK(dtype == TILEDB_UINT64);
  REQUIRE(
      tiledb_ndrectangle_get_dtype_from_name(ctx, ndr, "d1", &dtype) ==
      TILEDB_OK);
  CHECK(dtype == TILEDB_UINT64);

  uint32_t ndim;
  REQUIRE(tiledb_ndrectangle_get_dim_num(ctx, ndr, &ndim) == TILEDB_OK);
  CHECK(ndim == 2);

  REQUIRE(tiledb_ndrectangle_free(&ndr) == TILEDB_OK);
}
