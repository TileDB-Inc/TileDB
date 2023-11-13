/**
 * @file tiledb/api/c_api/query_field/test/unit_capi_query_field.cc
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
 * Tests the query field C API.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/query_field/query_field_api_external_experimental.h"

using namespace tiledb::test;

struct QueryFieldFx : TemporaryDirectoryFixture {
  QueryFieldFx() {
    create_sparse_array(array_name());
    write_sparse_array(array_name());
  }

  std::string array_name() {
    return temp_dir_ + "queryfield_array";
  }

  inline void throw_if_setup_failed(const int32_t rc) {
    if (rc != TILEDB_OK) {
      throw std::runtime_error("test setup failed");
    }
  }

  void write_sparse_array(const std::string& path);
  void create_sparse_array(const std::string& path);
};

void QueryFieldFx::write_sparse_array(const std::string& array_name) {
  tiledb_array_t* array;
  throw_if_setup_failed(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  throw_if_setup_failed(tiledb_array_open(ctx, array, TILEDB_WRITE));

  tiledb_query_t* query;
  throw_if_setup_failed(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));

  throw_if_setup_failed(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));

  int32_t a[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int32_t b[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t a_size = 10 * sizeof(int32_t);
  uint64_t b_size = 10 * sizeof(int32_t);

  int64_t d1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int64_t d2[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  uint64_t d1_size = 10 * sizeof(int64_t);
  uint64_t d2_size = 10 * sizeof(int64_t);
  char c_data[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  uint64_t c_size = strlen(c_data);
  uint64_t c_data_offsets[] = {0, 5, 8, 13, 17, 21, 26, 31, 36, 40};
  uint64_t c_offsets_size = sizeof(c_data_offsets);

  throw_if_setup_failed(
      tiledb_query_set_data_buffer(ctx, query, "a", a, &a_size));

  throw_if_setup_failed(
      tiledb_query_set_data_buffer(ctx, query, "b", b, &b_size));

  throw_if_setup_failed(
      tiledb_query_set_data_buffer(ctx, query, "d1", d1, &d1_size));

  throw_if_setup_failed(
      tiledb_query_set_data_buffer(ctx, query, "d2", d2, &d2_size));

  throw_if_setup_failed(
      tiledb_query_set_data_buffer(ctx, query, "c", c_data, &c_size));
  throw_if_setup_failed(tiledb_query_set_offsets_buffer(
      ctx, query, "c", c_data_offsets, &c_offsets_size));

  throw_if_setup_failed(
      tiledb_query_set_data_buffer(ctx, query, "d", c_data, &c_size));
  throw_if_setup_failed(tiledb_query_set_offsets_buffer(
      ctx, query, "d", c_data_offsets, &c_offsets_size));

  throw_if_setup_failed(tiledb_query_submit(ctx, query));

  // Clean up
  throw_if_setup_failed(tiledb_array_close(ctx, array));
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void QueryFieldFx::create_sparse_array(const std::string& array_name) {
  // Create dimensions
  uint64_t tile_extents[] = {2, 2};
  uint64_t dim_domain[] = {1, 10, 1, 10};

  tiledb_dimension_t* d1 = nullptr;
  throw_if_setup_failed(tiledb_dimension_alloc(
      ctx, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1));
  tiledb_dimension_t* d2 = nullptr;
  throw_if_setup_failed(tiledb_dimension_alloc(
      ctx, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2));

  // Create domain
  tiledb_domain_t* domain;
  throw_if_setup_failed(tiledb_domain_alloc(ctx, &domain));
  throw_if_setup_failed(tiledb_domain_add_dimension(ctx, domain, d1));
  throw_if_setup_failed(tiledb_domain_add_dimension(ctx, domain, d2));

  // Create attributes
  tiledb_attribute_t* a = nullptr;
  throw_if_setup_failed(tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a));
  tiledb_attribute_t* b = nullptr;
  throw_if_setup_failed(tiledb_attribute_alloc(ctx, "b", TILEDB_INT32, &b));
  tiledb_attribute_t* c = nullptr;
  throw_if_setup_failed(
      tiledb_attribute_alloc(ctx, "c", TILEDB_STRING_ASCII, &c));
  throw_if_setup_failed(
      tiledb_attribute_set_cell_val_num(ctx, c, TILEDB_VAR_NUM));
  tiledb_attribute_t* d = nullptr;
  throw_if_setup_failed(
      tiledb_attribute_alloc(ctx, "d", TILEDB_STRING_UTF8, &d));
  throw_if_setup_failed(
      tiledb_attribute_set_cell_val_num(ctx, d, TILEDB_VAR_NUM));

  // Create array schema
  tiledb_array_schema_t* array_schema = nullptr;
  throw_if_setup_failed(
      tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema));
  throw_if_setup_failed(
      tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR));
  throw_if_setup_failed(
      tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR));
  throw_if_setup_failed(tiledb_array_schema_set_capacity(ctx, array_schema, 4));
  throw_if_setup_failed(
      tiledb_array_schema_set_domain(ctx, array_schema, domain));
  throw_if_setup_failed(
      tiledb_array_schema_add_attribute(ctx, array_schema, a));
  throw_if_setup_failed(
      tiledb_array_schema_add_attribute(ctx, array_schema, b));
  throw_if_setup_failed(
      tiledb_array_schema_add_attribute(ctx, array_schema, c));
  throw_if_setup_failed(
      tiledb_array_schema_add_attribute(ctx, array_schema, d));

  // check array schema
  throw_if_setup_failed(tiledb_array_schema_check(ctx, array_schema));

  // Create array
  throw_if_setup_failed(
      tiledb_array_create(ctx, array_name.c_str(), array_schema));

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_attribute_free(&b);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    QueryFieldFx,
    "C API: argument validation",
    "[capi][query_field][get][args]") {
  tiledb_array_t* array = nullptr;
  ;
  REQUIRE(tiledb_array_alloc(ctx, array_name().c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query = nullptr;
  ;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  tiledb_query_field_t* field = nullptr;

  SECTION("nullptr context") {
    CHECK(
        tiledb_query_get_field(nullptr, query, "", &field) ==
        TILEDB_INVALID_CONTEXT);
  }

  SECTION("nullptr query") {
    CHECK(tiledb_query_get_field(ctx, nullptr, "", &field) == TILEDB_ERR);
  }

  SECTION("nullptr field name") {
    CHECK(tiledb_query_get_field(ctx, query, nullptr, &field) == TILEDB_ERR);
  }

  SECTION("nullptr output field") {
    CHECK(tiledb_query_get_field(ctx, query, "", nullptr) == TILEDB_ERR);
    CHECK(tiledb_query_field_free(ctx, nullptr) == TILEDB_ERR);
    CHECK(tiledb_query_field_free(ctx, &field) == TILEDB_ERR);
  }

  // Clean up
  tiledb_query_free(&query);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    QueryFieldFx,
    "C API: argument validation",
    "[capi][query_field][access][args]") {
  tiledb_array_t* array = nullptr;
  ;
  REQUIRE(tiledb_array_alloc(ctx, array_name().c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query = nullptr;
  ;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  tiledb_query_field_t* field = nullptr;
  tiledb_datatype_t type;
  tiledb_field_origin_t origin;
  uint32_t cell_val_num;
  tiledb_query_channel_t* channel = nullptr;

  REQUIRE(tiledb_query_get_field(ctx, query, "d1", &field) == TILEDB_OK);

  SECTION("nullptr context") {
    CHECK(
        tiledb_field_datatype(nullptr, field, &type) == TILEDB_INVALID_CONTEXT);
    CHECK(
        tiledb_field_origin(nullptr, field, &origin) == TILEDB_INVALID_CONTEXT);
    CHECK(
        tiledb_field_cell_val_num(nullptr, field, &cell_val_num) ==
        TILEDB_INVALID_CONTEXT);
    CHECK(
        tiledb_field_channel(nullptr, field, &channel) ==
        TILEDB_INVALID_CONTEXT);
  }

  SECTION("nullptr field") {
    CHECK(tiledb_field_datatype(ctx, nullptr, &type) == TILEDB_ERR);
    CHECK(tiledb_field_origin(ctx, nullptr, &origin) == TILEDB_ERR);
    CHECK(tiledb_field_cell_val_num(ctx, nullptr, &cell_val_num) == TILEDB_ERR);
    CHECK(tiledb_field_channel(ctx, nullptr, &channel) == TILEDB_ERR);
  }

  SECTION("nullptr output ptr") {
    CHECK(tiledb_field_datatype(ctx, field, nullptr) == TILEDB_ERR);
    CHECK(tiledb_field_origin(ctx, field, nullptr) == TILEDB_ERR);
    CHECK(tiledb_field_cell_val_num(ctx, field, nullptr) == TILEDB_ERR);
    CHECK(tiledb_field_channel(ctx, field, nullptr) == TILEDB_ERR);
  }

  // Clean up
  CHECK(tiledb_query_field_free(ctx, &field) == TILEDB_OK);
  tiledb_query_free(&query);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    QueryFieldFx, "C API: argument validation", "[capi][query_field]") {
  tiledb_array_t* array = nullptr;
  REQUIRE(tiledb_array_alloc(ctx, array_name().c_str(), &array) == TILEDB_OK);
  REQUIRE(tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_OK);

  tiledb_query_t* query = nullptr;
  REQUIRE(tiledb_query_alloc(ctx, array, TILEDB_READ, &query) == TILEDB_OK);

  REQUIRE(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED) == TILEDB_OK);
  int64_t dom[] = {1, 9, 1, 2};
  REQUIRE(tiledb_query_set_subarray(ctx, query, &dom) == TILEDB_OK);

  tiledb_query_field_t* field = nullptr;
  tiledb_datatype_t type;
  tiledb_field_origin_t origin;
  uint32_t cell_val_num = 0;
  tiledb_query_channel_t* channel = nullptr;

  // Errors out when the field doesn't exist
  CHECK(
      tiledb_query_get_field(ctx, query, "non_existent", &field) == TILEDB_ERR);

  // Check field api works on dimension field
  REQUIRE(tiledb_query_get_field(ctx, query, "d1", &field) == TILEDB_OK);

  REQUIRE(tiledb_field_datatype(ctx, field, &type) == TILEDB_OK);
  CHECK(type == TILEDB_UINT64);
  REQUIRE(tiledb_field_origin(ctx, field, &origin) == TILEDB_OK);
  CHECK(origin == TILEDB_DIMENSION_FIELD);
  REQUIRE(tiledb_field_cell_val_num(ctx, field, &cell_val_num) == TILEDB_OK);
  CHECK(cell_val_num == 1);

  // Check field api works on aggregate field
  REQUIRE(tiledb_field_channel(ctx, field, &channel) == TILEDB_OK);
  REQUIRE(
      tiledb_channel_apply_aggregate(
          ctx, channel, "Count", tiledb_aggregate_count) == TILEDB_OK);
  uint64_t count = 0;
  uint64_t size = 8;
  REQUIRE(
      tiledb_query_set_data_buffer(ctx, query, "Count", &count, &size) ==
      TILEDB_OK);
  REQUIRE(tiledb_query_submit(ctx, query) == TILEDB_OK);
  CHECK(count == 9);
  CHECK(tiledb_query_channel_free(ctx, &channel) == TILEDB_OK);
  CHECK(tiledb_query_field_free(ctx, &field) == TILEDB_OK);

  // Check field api works on timestamp field
  REQUIRE(
      tiledb_query_get_field(ctx, query, "__timestamps", &field) == TILEDB_OK);
  REQUIRE(tiledb_field_datatype(ctx, field, &type) == TILEDB_OK);
  CHECK(type == TILEDB_UINT64);
  REQUIRE(tiledb_field_origin(ctx, field, &origin) == TILEDB_OK);
  CHECK(origin == TILEDB_ATTRIBUTE_FIELD);
  REQUIRE(tiledb_field_cell_val_num(ctx, field, &cell_val_num) == TILEDB_OK);
  CHECK(cell_val_num == 1);
  CHECK(tiledb_query_field_free(ctx, &field) == TILEDB_OK);

  // Check field api works on coords field
  REQUIRE(tiledb_query_get_field(ctx, query, "__coords", &field) == TILEDB_OK);
  REQUIRE(tiledb_field_datatype(ctx, field, &type) == TILEDB_OK);
  CHECK(type == TILEDB_UINT64);
  REQUIRE(tiledb_field_origin(ctx, field, &origin) == TILEDB_OK);
  CHECK(origin == TILEDB_DIMENSION_FIELD);
  REQUIRE(tiledb_field_cell_val_num(ctx, field, &cell_val_num) == TILEDB_OK);
  CHECK(cell_val_num == 1);
  CHECK(tiledb_query_field_free(ctx, &field) == TILEDB_OK);

  // Check field api works on attribute field
  REQUIRE(tiledb_query_get_field(ctx, query, "c", &field) == TILEDB_OK);
  REQUIRE(tiledb_field_datatype(ctx, field, &type) == TILEDB_OK);
  CHECK(type == TILEDB_STRING_ASCII);
  REQUIRE(tiledb_field_origin(ctx, field, &origin) == TILEDB_OK);
  CHECK(origin == TILEDB_ATTRIBUTE_FIELD);
  REQUIRE(tiledb_field_cell_val_num(ctx, field, &cell_val_num) == TILEDB_OK);
  CHECK(cell_val_num == TILEDB_VAR_NUM);
  CHECK(tiledb_query_field_free(ctx, &field) == TILEDB_OK);

  // Check field api works on aggregate field
  REQUIRE(tiledb_query_get_field(ctx, query, "Count", &field) == TILEDB_OK);
  REQUIRE(tiledb_field_datatype(ctx, field, &type) == TILEDB_OK);
  CHECK(type == TILEDB_UINT64);
  REQUIRE(tiledb_field_origin(ctx, field, &origin) == TILEDB_OK);
  CHECK(origin == TILEDB_AGGREGATE_FIELD);
  REQUIRE(tiledb_field_cell_val_num(ctx, field, &cell_val_num) == TILEDB_OK);
  CHECK(cell_val_num == 1);
  CHECK(tiledb_query_field_free(ctx, &field) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
}
