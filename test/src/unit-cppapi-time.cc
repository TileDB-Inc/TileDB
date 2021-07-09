/**
 * @file   unit-cppapi-time.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB Inc.
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
 * Tests the C++ API for time attributes.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

TEST_CASE("C++ API: Time attribute", "[cppapi][Time]") {
  const std::string array_name = "cpp_unit_time_array";
  for (const auto& time_type : {TILEDB_TIME_HR,
                                TILEDB_TIME_MIN,
                                TILEDB_TIME_SEC,
                                TILEDB_TIME_MS,
                                TILEDB_TIME_US,
                                TILEDB_TIME_NS,
                                TILEDB_TIME_PS,
                                TILEDB_TIME_FS,
                                TILEDB_TIME_AS}) {
    Context ctx;
    VFS vfs(ctx);

    if (vfs.is_dir(array_name))
      vfs.remove_dir(array_name);

    auto attr = Attribute(ctx, "a", time_type)
                    .set_filter_list(FilterList(ctx).add_filter(
                        Filter(ctx, TILEDB_FILTER_BZIP2)));
    Array::create(
        array_name,
        ArraySchema(ctx, TILEDB_SPARSE)
            .set_domain(Domain(ctx).add_dimension(
                Dimension::create<uint32_t>(ctx, "d0", {{0, 9}}, 5)))
            .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
            .add_attribute(attr));

    std::vector<int64_t> data_w(10);
    std::vector<uint32_t> coords_w = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++)
      data_w[i] = 2 * i;

    Array array_w(ctx, array_name, TILEDB_WRITE);
    Query query_w(ctx, array_w);
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", data_w)
        .set_coordinates(coords_w)
        .submit();
    query_w.finalize();
    array_w.close();

    // Read and check results
    std::vector<int64_t> data_r(10, -1);
    std::vector<uint32_t> coords_r = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r);
    query_r.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_r)
        .set_coordinates(coords_r);
    REQUIRE(query_r.submit() == Query::Status::COMPLETE);

    auto result_num = query_r.result_buffer_elements()["a"].second;
    REQUIRE(result_num == 10);
    for (int i = 0; i < 10; i++)
      REQUIRE(data_r[i] == 2 * i);

    array_r.close();

    if (vfs.is_dir(array_name))
      vfs.remove_dir(array_name);
  }
}

TEST_CASE("C++ API: Time dimension", "[cppapi][time]") {
  const std::string array_name = "cpp_unit_time_array";
  for (const auto& time_type : {TILEDB_TIME_HR,
                                TILEDB_TIME_MIN,
                                TILEDB_TIME_SEC,
                                TILEDB_TIME_MS,
                                TILEDB_TIME_US,
                                TILEDB_TIME_NS,
                                TILEDB_TIME_PS,
                                TILEDB_TIME_FS,
                                TILEDB_TIME_AS}) {
    Context ctx;
    VFS vfs(ctx);

    if (vfs.is_dir(array_name))
      vfs.remove_dir(array_name);

    int64_t tile_extent = int64_t(1e6);
    int64_t domain[] = {0, std::numeric_limits<int64_t>::max() - tile_extent};
    auto dim = Dimension::create(ctx, "d0", time_type, domain, &tile_extent);
    Array::create(
        array_name,
        ArraySchema(ctx, TILEDB_SPARSE)
            .set_domain(Domain(ctx).add_dimension(dim))
            .set_order({{TILEDB_COL_MAJOR, TILEDB_COL_MAJOR}})
            .add_attribute(Attribute(ctx, "a", TILEDB_INT32)));

    std::vector<int32_t> data_w(10);
    std::vector<int64_t> coords_w = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    for (int i = 0; i < 10; i++)
      data_w[i] = 2 * i;

    Array array_w(ctx, array_name, TILEDB_WRITE);
    Query query_w(ctx, array_w);
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", data_w)
        .set_coordinates(coords_w)
        .submit();
    query_w.finalize();
    array_w.close();

    // Read and check results
    std::vector<int32_t> data_r(10, -1);
    std::vector<int64_t> subarray_r = {0, 9};
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r);
    query_r.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_r)
        .set_subarray(subarray_r);
    REQUIRE(query_r.submit() == Query::Status::COMPLETE);

    auto result_num = query_r.result_buffer_elements()["a"].second;
    REQUIRE(result_num == 10);
    for (int i = 0; i < 10; i++)
      REQUIRE(data_r[i] == 2 * i);

    array_r.close();

    if (vfs.is_dir(array_name))
      vfs.remove_dir(array_name);
  }
}