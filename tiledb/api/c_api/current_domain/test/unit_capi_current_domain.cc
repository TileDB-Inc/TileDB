/**
 * @file tiledb/api/c_api/current_domain/test/unit_capi_current_domain.cc
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
 * Tests the current_domain C API.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/current_domain/current_domain_api_external_experimental.h"
#include "tiledb/api/c_api/current_domain/current_domain_api_internal.h"
#include "tiledb/api/c_api/ndrectangle/ndrectangle_api_internal.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"

using namespace tiledb::test;

struct CAPICurrentDomainFx : TemporaryDirectoryFixture {
  CAPICurrentDomainFx()
      : array_name_(temp_dir_ + "curent_domain_array") {
    create_domain();
  }
  ~CAPICurrentDomainFx() {
    free_domain();
  }
  void create_domain();
  void free_domain();

  std::string array_name_;
  tiledb_dimension_t *d1_, *d2_;
  tiledb_domain_t* domain_;
};

void CAPICurrentDomainFx::create_domain() {
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

void CAPICurrentDomainFx::free_domain() {
  tiledb_dimension_free(&d1_);
  tiledb_dimension_free(&d2_);
  tiledb_domain_free(&domain_);
}

TEST_CASE_METHOD(
    CAPICurrentDomainFx,
    "C API: argument validation",
    "[capi][current_domain][args]") {
  CHECK(
      tiledb_current_domain_create(nullptr, nullptr) == TILEDB_INVALID_CONTEXT);
  CHECK(tiledb_current_domain_create(ctx, nullptr) == TILEDB_ERR);

  CHECK(tiledb_current_domain_free(nullptr) == TILEDB_ERR);
  tiledb_current_domain_t* crd = nullptr;
  CHECK(tiledb_current_domain_free(&crd) == TILEDB_ERR);

  crd = nullptr;
  CHECK(tiledb_current_domain_create(ctx, &crd) == TILEDB_OK);

  CHECK(tiledb_current_domain_set_ndrectangle(nullptr, nullptr) == TILEDB_ERR);
  CHECK(tiledb_current_domain_set_ndrectangle(crd, nullptr) == TILEDB_ERR);

  CHECK(tiledb_current_domain_get_ndrectangle(nullptr, nullptr) == TILEDB_ERR);
  CHECK(tiledb_current_domain_get_ndrectangle(crd, nullptr) == TILEDB_ERR);

  CHECK(tiledb_current_domain_get_is_empty(nullptr, nullptr) == TILEDB_ERR);
  CHECK(tiledb_current_domain_get_is_empty(crd, nullptr) == TILEDB_ERR);

  CHECK(tiledb_current_domain_get_type(nullptr, nullptr) == TILEDB_ERR);
  CHECK(tiledb_current_domain_get_type(crd, nullptr) == TILEDB_ERR);

  tiledb_current_domain_free(&crd);
}

TEST_CASE_METHOD(
    CAPICurrentDomainFx,
    "C API: Setting ND rectangles works",
    "[capi][current_domain][ndr]") {
  tiledb_current_domain_t* crd = nullptr;
  REQUIRE(tiledb_current_domain_create(ctx, &crd) == TILEDB_OK);

  tiledb_ndrectangle_t* ndr = nullptr;
  REQUIRE(tiledb_ndrectangle_alloc(ctx, domain_, &ndr) == TILEDB_OK);

  uint32_t is_empty = 0;
  REQUIRE(tiledb_current_domain_get_is_empty(crd, &is_empty) == TILEDB_OK);
  CHECK(is_empty == 1);

  tiledb_current_domain_type_t type;
  CHECK(tiledb_current_domain_get_type(crd, &type) == TILEDB_ERR);

  REQUIRE(tiledb_current_domain_set_ndrectangle(crd, ndr) == TILEDB_OK);

  REQUIRE(tiledb_current_domain_get_is_empty(crd, &is_empty) == TILEDB_OK);
  CHECK(is_empty == 0);

  REQUIRE(tiledb_current_domain_get_type(crd, &type) == TILEDB_OK);
  CHECK(type == TILEDB_NDRECTANGLE);

  tiledb_ndrectangle_t* out_ndr = nullptr;
  REQUIRE(tiledb_current_domain_get_ndrectangle(crd, &out_ndr) == TILEDB_OK);
  CHECK(out_ndr != nullptr);

  // Verify that they point to the same tiledb::sm::NDRectangle instance.
  CHECK(ndr->ndrectangle().get() == out_ndr->ndrectangle().get());

  tiledb_ndrectangle_free(&out_ndr);
  tiledb_ndrectangle_free(&ndr);
  tiledb_current_domain_free(&crd);
}
