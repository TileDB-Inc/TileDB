/**
 * @file windows-commits-path-separator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB Inc.
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
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/c_api/tiledb.h"

#include <chrono>
#include <thread>

TEST_CASE(
    "Regression: Windows commits consolidation path separator mismatch",
    "[regression][windows][commits][consolidation]") {
  tiledb_ctx_t* ctx;
  tiledb_vfs_t* vfs;
  int rc;

  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  rc = tiledb_ctx_alloc(config, &ctx);
  REQUIRE(rc == TILEDB_OK);
  tiledb_config_free(&config);

  rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
  REQUIRE(rc == TILEDB_OK);

  std::string array_name = "test_windows_commits_regression";

  int is_dir = 0;
  rc = tiledb_vfs_is_dir(ctx, vfs, array_name.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (is_dir) {
    rc = tiledb_vfs_remove_dir(ctx, vfs, array_name.c_str());
    REQUIRE(rc == TILEDB_OK);
  }

  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* d;
  uint64_t dim_domain[] = {1, 1000};
  uint64_t tile_extent = 10;
  rc = tiledb_dimension_alloc(
      ctx, "d", TILEDB_UINT64, &dim_domain[0], &tile_extent, &d);
  REQUIRE(rc == TILEDB_OK);

  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_create(ctx, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  {
    tiledb_array_t* array;
    rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
    REQUIRE(rc == TILEDB_OK);

    tiledb_query_t* query;
    rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
    REQUIRE(rc == TILEDB_OK);

    uint64_t coords[] = {1, 2, 3};
    int32_t data[] = {10, 20, 30};
    uint64_t coords_size = sizeof(coords);
    uint64_t data_size = sizeof(data);

    rc = tiledb_query_set_data_buffer(ctx, query, "d", coords, &coords_size);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_query_submit(ctx, query);
    REQUIRE(rc == TILEDB_OK);

    tiledb_query_free(&query);
    rc = tiledb_array_close(ctx, array);
    REQUIRE(rc == TILEDB_OK);
    tiledb_array_free(&array);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  {
    tiledb_array_t* array;
    rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
    REQUIRE(rc == TILEDB_OK);

    tiledb_query_t* query;
    rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
    REQUIRE(rc == TILEDB_OK);

    uint64_t coords[] = {4, 5};
    int32_t data[] = {40, 50};
    uint64_t coords_size = sizeof(coords);
    uint64_t data_size = sizeof(data);

    rc = tiledb_query_set_data_buffer(ctx, query, "d", coords, &coords_size);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_query_submit(ctx, query);
    REQUIRE(rc == TILEDB_OK);

    tiledb_query_free(&query);
    rc = tiledb_array_close(ctx, array);
    REQUIRE(rc == TILEDB_OK);
    tiledb_array_free(&array);
  }

  {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);

    rc = tiledb_config_set(cfg, "sm.consolidation.mode", "commits", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);

    rc = tiledb_array_consolidate(ctx, array_name.c_str(), cfg);
    REQUIRE(rc == TILEDB_OK);

    tiledb_config_free(&cfg);
  }

  {
    tiledb_array_t* array;
    rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_array_open(ctx, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);

    tiledb_query_t* query;
    rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
    REQUIRE(rc == TILEDB_OK);

    uint64_t coords[5];
    int32_t data[5];
    uint64_t coords_size = sizeof(coords);
    uint64_t data_size = sizeof(data);

    rc = tiledb_query_set_data_buffer(ctx, query, "d", coords, &coords_size);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_query_submit(ctx, query);
    REQUIRE(rc == TILEDB_OK);

    REQUIRE(coords_size == 5 * sizeof(uint64_t));
    REQUIRE(data_size == 5 * sizeof(int32_t));
    REQUIRE(coords[0] == 1);
    REQUIRE(coords[1] == 2);
    REQUIRE(coords[2] == 3);
    REQUIRE(coords[3] == 4);
    REQUIRE(coords[4] == 5);
    REQUIRE(data[0] == 10);
    REQUIRE(data[1] == 20);
    REQUIRE(data[2] == 30);
    REQUIRE(data[3] == 40);
    REQUIRE(data[4] == 50);

    tiledb_query_free(&query);
    rc = tiledb_array_close(ctx, array);
    REQUIRE(rc == TILEDB_OK);
    tiledb_array_free(&array);
  }

  rc = tiledb_vfs_remove_dir(ctx, vfs, array_name.c_str());
  REQUIRE(rc == TILEDB_OK);

  tiledb_vfs_free(&vfs);
  tiledb_ctx_free(&ctx);
}
