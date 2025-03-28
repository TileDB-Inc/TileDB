/**
 * @file unit-current-domain-rest.cc
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
 * Fuctional test for Current Domain locally and via REST.
 */

#include "test/support/src/vfs_helpers.h"
#include "test/support/tdb_catch.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb::test;

struct RestCurrentDomainFx {
  RestCurrentDomainFx();

  void create_sparse_array(const std::string& array_name);
  void create_sparse_array_at_timestamp(
      const std::string& array_name, uint64_t timestamp);

  VFSTestSetup vfs_test_setup_;

  // TileDB context
  tiledb_ctx_t* ctx_c_;
  std::string uri_;
};

RestCurrentDomainFx::RestCurrentDomainFx()
    : ctx_c_(vfs_test_setup_.ctx_c) {
}

void RestCurrentDomainFx::create_sparse_array(const std::string& array_name) {
  uri_ = vfs_test_setup_.array_uri(array_name);

  // Create dimensions
  uint64_t tile_extents[] = {2};
  uint64_t dim_domain[] = {1, 10};

  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_c_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);

  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(ctx_c_, "d2", TILEDB_STRING_ASCII, 0, 0, &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_c_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_c_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_c_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_c_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_c_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(
      ctx_c_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(
      ctx_c_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_c_, array_schema, 4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_c_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_c_, array_schema, a);
  CHECK(rc == TILEDB_OK);

  tiledb_current_domain_t* crd = nullptr;
  REQUIRE(tiledb_current_domain_create(ctx_c_, &crd) == TILEDB_OK);
  tiledb_ndrectangle_t* ndr = nullptr;
  REQUIRE(tiledb_ndrectangle_alloc(ctx_c_, domain, &ndr) == TILEDB_OK);

  tiledb_range_t range;
  uint64_t min = 2;
  uint64_t max = 5;
  range.min = &min;
  range.min_size = sizeof(uint64_t);
  range.max = &max;
  range.max_size = sizeof(uint64_t);
  REQUIRE(
      tiledb_ndrectangle_set_range_for_name(ctx_c_, ndr, "d1", &range) ==
      TILEDB_OK);

  tiledb_range_t range_var;
  std::string min_var("ab");
  std::string max_var("cd");
  range_var.min = min_var.data();
  range_var.min_size = 2;
  range_var.max = max_var.data();
  range_var.max_size = 2;
  REQUIRE(
      tiledb_ndrectangle_set_range_for_name(ctx_c_, ndr, "d2", &range_var) ==
      TILEDB_OK);

  REQUIRE(tiledb_current_domain_set_ndrectangle(ctx_c_, crd, ndr) == TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_set_current_domain(ctx_c_, array_schema, crd) ==
      TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_c_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_c_, uri_.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  REQUIRE(tiledb_ndrectangle_free(&ndr) == TILEDB_OK);
  REQUIRE(tiledb_current_domain_free(&crd) == TILEDB_OK);
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void RestCurrentDomainFx::create_sparse_array_at_timestamp(
    const std::string& array_name, uint64_t timestamp) {
  uri_ = vfs_test_setup_.array_uri(array_name);

  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc_at_timestamp(
      ctx_c_, TILEDB_SPARSE, timestamp, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  int64_t dim_domain[2] = {0, 99};
  int64_t tile_extent = 10;
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_c_, "", TILEDB_INT64, &dim_domain[0], &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_c_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_c_, domain, dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_c_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set current domain to [0,10]
  tiledb_current_domain_t* current_domain;
  rc = tiledb_current_domain_create(ctx_c_, &current_domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_ndrectangle_t* ndr;
  rc = tiledb_ndrectangle_alloc(ctx_c_, domain, &ndr);
  REQUIRE(rc == TILEDB_OK);

  tiledb_range_t orignal_range;
  int64_t min = 0;
  orignal_range.min = &min;
  orignal_range.min_size = sizeof(int64_t);
  int64_t max = 10;
  orignal_range.max = &max;
  orignal_range.max_size = sizeof(int64_t);
  rc = tiledb_ndrectangle_set_range_for_name(ctx_c_, ndr, "", &orignal_range);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_current_domain_set_ndrectangle(ctx_c_, current_domain, ndr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_current_domain(
      ctx_c_, array_schema, current_domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_c_, "attr", TILEDB_INT32, &attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_c_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  uint64_t capacity = 500;
  rc = tiledb_array_schema_set_capacity(ctx_c_, array_schema, capacity);
  REQUIRE(rc == TILEDB_OK);
  tiledb_layout_t cell_order = TILEDB_COL_MAJOR;
  rc = tiledb_array_schema_set_cell_order(ctx_c_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  rc = tiledb_array_schema_set_tile_order(ctx_c_, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array schema
  rc = tiledb_array_schema_check(ctx_c_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_c_, uri_.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_current_domain_free(&current_domain);
  tiledb_ndrectangle_free(&ndr);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    RestCurrentDomainFx,
    "C API: Current Domain basic behavior",
    "[current_domain][rest]") {
  create_sparse_array("currentdomain_array");

  // Open array, read back current domain from schema and check
  tiledb_array_schema_t* schema;
  tiledb_array_t* array;
  tiledb_current_domain_t* crd;
  tiledb_ndrectangle_t* ndr;

  REQUIRE(tiledb_array_alloc(ctx_c_, uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_c_, array, TILEDB_READ) == TILEDB_OK);
  REQUIRE(tiledb_array_get_schema(ctx_c_, array, &schema) == TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_get_current_domain(ctx_c_, schema, &crd) ==
      TILEDB_OK);

  REQUIRE(
      tiledb_current_domain_get_ndrectangle(ctx_c_, crd, &ndr) == TILEDB_OK);
  tiledb_range_t outrange;
  tiledb_range_t outrange_var;
  REQUIRE(
      tiledb_ndrectangle_get_range_from_name(ctx_c_, ndr, "d1", &outrange) ==
      TILEDB_OK);
  CHECK(*(uint64_t*)outrange.min == 2);
  CHECK(*(uint64_t*)outrange.max == 5);
  CHECK(outrange.min_size == sizeof(uint64_t));
  CHECK(outrange.max_size == sizeof(uint64_t));

  REQUIRE(
      tiledb_ndrectangle_get_range_from_name(
          ctx_c_, ndr, "d2", &outrange_var) == TILEDB_OK);
  CHECK(std::memcmp(outrange_var.min, "ab", 2) == 0);
  CHECK(std::memcmp(outrange_var.max, "cd", 2) == 0);
  CHECK(outrange_var.min_size == 2);
  CHECK(outrange_var.max_size == 2);

  REQUIRE(tiledb_ndrectangle_free(&ndr) == TILEDB_OK);
  REQUIRE(tiledb_current_domain_free(&crd) == TILEDB_OK);
  tiledb_array_schema_free(&schema);
  REQUIRE(tiledb_array_close(ctx_c_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    RestCurrentDomainFx,
    "C API: Current Domain basic schema evolution",
    "[current_domain][evolution][rest]") {
  create_sparse_array("currentdomain_array");
  tiledb_array_t* array;
  tiledb_array_schema_t* schema;
  tiledb_domain_t* domain;
  tiledb_current_domain_t* crd;
  tiledb_ndrectangle_t* ndr;

  REQUIRE(tiledb_array_alloc(ctx_c_, uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_c_, array, TILEDB_READ) == TILEDB_OK);
  REQUIRE(tiledb_array_get_schema(ctx_c_, array, &schema) == TILEDB_OK);
  REQUIRE(tiledb_array_schema_get_domain(ctx_c_, schema, &domain) == TILEDB_OK);

  tiledb_array_schema_evolution_t* evo;
  REQUIRE(tiledb_array_schema_evolution_alloc(ctx_c_, &evo) == TILEDB_OK);
  REQUIRE(tiledb_current_domain_create(ctx_c_, &crd) == TILEDB_OK);
  REQUIRE(tiledb_ndrectangle_alloc(ctx_c_, domain, &ndr) == TILEDB_OK);

  tiledb_range_t range;
  uint64_t min = 2;
  uint64_t max = 7;
  range.min = &min;
  range.min_size = sizeof(uint64_t);
  range.max = &max;
  range.max_size = sizeof(uint64_t);

  REQUIRE(
      tiledb_ndrectangle_set_range_for_name(ctx_c_, ndr, "d1", &range) ==
      TILEDB_OK);

  tiledb_range_t range_var;
  std::string min_var("aa");
  std::string max_var("ce");
  range_var.min = min_var.data();
  range_var.min_size = 2;
  range_var.max = max_var.data();
  range_var.max_size = 2;
  REQUIRE(
      tiledb_ndrectangle_set_range_for_name(ctx_c_, ndr, "d2", &range_var) ==
      TILEDB_OK);

  REQUIRE(tiledb_current_domain_set_ndrectangle(ctx_c_, crd, ndr) == TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_evolution_expand_current_domain(ctx_c_, evo, crd) ==
      TILEDB_OK);

  REQUIRE(tiledb_array_evolve(ctx_c_, uri_.c_str(), evo) == TILEDB_OK);

  REQUIRE(tiledb_ndrectangle_free(&ndr) == TILEDB_OK);
  REQUIRE(tiledb_current_domain_free(&crd) == TILEDB_OK);
  tiledb_array_schema_evolution_free(&evo);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&schema);
  REQUIRE(tiledb_array_close(ctx_c_, array) == TILEDB_OK);
  tiledb_array_free(&array);

  // Open array, read back current domain from schema and check
  REQUIRE(tiledb_array_alloc(ctx_c_, uri_.c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx_c_, array, TILEDB_READ) == TILEDB_OK);
  REQUIRE(tiledb_array_get_schema(ctx_c_, array, &schema) == TILEDB_OK);
  REQUIRE(
      tiledb_array_schema_get_current_domain(ctx_c_, schema, &crd) ==
      TILEDB_OK);

  REQUIRE(
      tiledb_current_domain_get_ndrectangle(ctx_c_, crd, &ndr) == TILEDB_OK);
  tiledb_range_t outrange;
  REQUIRE(
      tiledb_ndrectangle_get_range_from_name(ctx_c_, ndr, "d1", &outrange) ==
      TILEDB_OK);
  CHECK(*(uint64_t*)outrange.min == 2);
  CHECK(*(uint64_t*)outrange.max == 7);
  CHECK(outrange.min_size == sizeof(uint64_t));
  CHECK(outrange.max_size == sizeof(uint64_t));

  tiledb_range_t outrange_var;
  REQUIRE(
      tiledb_ndrectangle_get_range_from_name(
          ctx_c_, ndr, "d2", &outrange_var) == TILEDB_OK);
  CHECK(std::memcmp(outrange_var.min, "aa", 2) == 0);
  CHECK(std::memcmp(outrange_var.max, "ce", 2) == 0);
  CHECK(outrange_var.min_size == 2);
  CHECK(outrange_var.max_size == 2);

  REQUIRE(tiledb_ndrectangle_free(&ndr) == TILEDB_OK);
  REQUIRE(tiledb_current_domain_free(&crd) == TILEDB_OK);
  tiledb_array_schema_free(&schema);
  REQUIRE(tiledb_array_close(ctx_c_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    RestCurrentDomainFx,
    "C API: Current Domain basic schema evolution at timestamp",
    "[current_domain][evolution][rest]") {
  // Create array schema at ts=1
  create_sparse_array_at_timestamp("currentdomain_array", 1);

  // Create an array schema evolution
  tiledb_array_schema_evolution_t* array_schema_evolution;
  int rc = tiledb_array_schema_evolution_alloc(ctx_c_, &array_schema_evolution);
  REQUIRE(rc == TILEDB_OK);

  // Extend current domain to [-10,20]
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_load(ctx_c_, uri_.c_str(), &array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_t* read_dom;
  rc = tiledb_array_schema_get_domain(ctx_c_, array_schema, &read_dom);
  REQUIRE(rc == TILEDB_OK);
  tiledb_current_domain_t* extended_current_domain;
  rc = tiledb_current_domain_create(ctx_c_, &extended_current_domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_ndrectangle_t* extended_ndr;
  rc = tiledb_ndrectangle_alloc(ctx_c_, read_dom, &extended_ndr);
  REQUIRE(rc == TILEDB_OK);

  tiledb_range_t extended_range;
  int64_t min = 0;
  extended_range.min = &min;
  extended_range.min_size = sizeof(int64_t);
  int64_t max = 20;
  extended_range.max = &max;
  extended_range.max_size = sizeof(int64_t);
  rc = tiledb_ndrectangle_set_range_for_name(
      ctx_c_, extended_ndr, "", &extended_range);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_current_domain_set_ndrectangle(
      ctx_c_, extended_current_domain, extended_ndr);
  REQUIRE(rc == TILEDB_OK);

  // Set timestamp at ts=2
  rc = tiledb_array_schema_evolution_expand_current_domain(
      ctx_c_, array_schema_evolution, extended_current_domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_evolution_set_timestamp_range(
      ctx_c_, array_schema_evolution, 2, 2);
  REQUIRE(rc == TILEDB_OK);

  // Evolve schema
  rc = tiledb_array_evolve(ctx_c_, uri_.c_str(), array_schema_evolution);
  REQUIRE(rc == TILEDB_OK);

  // Clean up array schema evolution
  tiledb_array_schema_evolution_free(&array_schema_evolution);
  tiledb_domain_free(&read_dom);
  tiledb_current_domain_free(&extended_current_domain);
  tiledb_ndrectangle_free(&extended_ndr);
  tiledb_array_schema_free(&array_schema);

  // Check current domain at ts=1 is [0,10]
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_c_, uri_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_c_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_c_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_schema_t* read_schema;
  rc = tiledb_array_get_schema(ctx_c_, array, &read_schema);
  REQUIRE(rc == TILEDB_OK);

  tiledb_current_domain_t* read_current_domain;
  rc = tiledb_array_schema_get_current_domain(
      ctx_c_, read_schema, &read_current_domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_ndrectangle_t* read_ndr;
  rc = tiledb_current_domain_get_ndrectangle(
      ctx_c_, read_current_domain, &read_ndr);
  REQUIRE(rc == TILEDB_OK);
  tiledb_range_t read_range;
  rc =
      tiledb_ndrectangle_get_range_from_name(ctx_c_, read_ndr, "", &read_range);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(*(static_cast<const int64_t*>(read_range.min)) == 0);
  REQUIRE(*(static_cast<const int64_t*>(read_range.max)) == 10);

  // Check current domain at ts=2 is extended to [0,20]
  rc = tiledb_array_alloc(ctx_c_, uri_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_c_, array, 2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_c_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_get_schema(ctx_c_, array, &read_schema);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_get_current_domain(
      ctx_c_, read_schema, &read_current_domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_current_domain_get_ndrectangle(
      ctx_c_, read_current_domain, &read_ndr);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_ndrectangle_get_range_from_name(ctx_c_, read_ndr, "", &read_range);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(*(static_cast<const int64_t*>(read_range.min)) == 0);
  REQUIRE(*(static_cast<const int64_t*>(read_range.max)) == 20);

  // Close array
  rc = tiledb_array_close(ctx_c_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_schema_free(&read_schema);
  tiledb_array_free(&array);
  tiledb_current_domain_free(&read_current_domain);
  tiledb_ndrectangle_free(&read_ndr);
}
