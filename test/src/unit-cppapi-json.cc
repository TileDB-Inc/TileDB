/**
 * @file   unit-cppapi-schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
 * Tests the C++ API for schema related functions.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/filesystem/posix.h"

// Filesystem related
#ifdef _WIN32
const std::string FILE_URI_PREFIX = "";
const std::string FILE_TEMP_DIR =
    tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
const std::string FILE_URI_PREFIX = "file://";
const std::string FILE_TEMP_DIR =
    tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
TEST_CASE("C++ API: JSON", "[cppapi]") {
  using namespace tiledb;
  Context ctx;

  Domain dense_domain(ctx);
  auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
  dense_domain.add_dimension(id1).add_dimension(id2);

  Domain sparse_domain(ctx);
  auto fd1 = Dimension::create<double>(ctx, "d1", {{-100.0, 100.0}}, 10.0);
  auto fd2 = Dimension::create<double>(ctx, "d2", {{-100.0, 100.0}}, 10.0);
  sparse_domain.add_dimensions(fd1, fd2);

  Domain sparse_domain_json(ctx);
  auto fdj1 = Dimension::create<double>(ctx, "d1", {{-100.0, 100.0}}, 10.0);
  auto fdj2 = Dimension::create<double>(ctx, "d2", {{-100.0, 100.0}}, 10.0);
  sparse_domain_json.add_dimensions(fdj1, fdj2);

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<std::string>(ctx, "a2");
  auto a3 = Attribute::create<std::array<double, 2>>(ctx, "a3");
  auto a4 = Attribute::create<std::vector<uint32_t>>(ctx, "a4");
  tiledb::Filter a1Filter(ctx, TILEDB_FILTER_LZ4);
  tiledb::FilterList a1FilterList(ctx);
  a1FilterList.add_filter(a1Filter);
  a1.set_filter_list(a1FilterList);
  /*
    SECTION("ArraySchema Serialization") {
      ArraySchema schema(ctx, TILEDB_SPARSE);
      schema.set_domain(sparse_domain_json);
      schema.add_attribute(a1);
      schema.add_attribute(a2);
      schema.add_attribute(a3);
      schema.add_attribute(a4);
      schema.set_cell_order(TILEDB_ROW_MAJOR);
      schema.set_tile_order(TILEDB_COL_MAJOR);
      schema.set_offsets_compressor({TILEDB_DOUBLE_DELTA, -1});
      schema.set_coords_compressor({TILEDB_ZSTD, -1});

      std::string json = schema.to_json();
      CHECK_THAT(
          json,
          Catch::Equals(
              "{\"array_type\":\"sparse\",\"attributes\":[{\"cell_val_num\":1,"
              "\"compressor\":"
              "\"BLOSC_LZ\",\"compressor_level\":-1,\"name\":\"a1\",\"type\":"
              "\"INT32\"},{\"cell_val_num\":"
              "4294967295,\"compressor\":\"NO_COMPRESSION\",\"compressor_level\":"
              "-1,\"name\":\"a2\","
              "\"type\":\"CHAR\"},{\"cell_val_num\":16,\"compressor\":\"NO_"
              "COMPRESSION\","
              "\"compressor_level\":-1,\"name\":\"a3\",\"type\":\"CHAR\"},{"
              "\"cell_val_num\":4294967295,"
              "\"compressor\":\"NO_COMPRESSION\",\"compressor_level\":-1,"
              "\"name\":\"a4\",\"type\":"
              "\"UINT32\"}],\"capacity\":10000,\"cell_order\":\"row-major\","
              "\"coords_compression\":"
              "\"ZSTD\",\"coords_compression_level\":-1,\"domain\":{\"cell_"
              "order\":\"row-major\","
              "\"dimensions\":[{\"domain\":[-100.0,100.0],\"name\":\"d1\",\"null_"
              "tile_extent\":false,"
              "\"tile_extent\":10.0,\"tile_extent_type\":\"FLOAT64\",\"type\":"
              "\"FLOAT64\"},{\"domain\":"
              "[-100.0,100.0],\"name\":\"d2\",\"null_tile_extent\":false,\"tile_"
              "extent\":10.0,"
              "\"tile_extent_type\":\"FLOAT64\",\"type\":\"FLOAT64\"}],\"tile_"
              "order\":\"row-major\","
              "\"type\":\"FLOAT64\"},\"offset_compression\":\"DOUBLE_DELTA\","
              "\"offset_compression_level\":-1,\"tile_order\":\"col-major\","
              "\"uri\":\"\",\"version\":[1,"
              "3,0]}"));

      ArraySchema* schemaParsed = ArraySchema::from_json(ctx, json);

      REQUIRE(schemaParsed != nullptr);
      REQUIRE(schemaParsed->attribute_num() == 4);
      CHECK(schemaParsed->attribute(0).name() == "a1");
      CHECK(schemaParsed->attribute(1).name() == "a2");
      CHECK(schemaParsed->attribute(2).name() == "a3");
      CHECK(
          schemaParsed->attribute("a1").compressor().compressor() ==
          TILEDB_BLOSC_LZ);
      CHECK(schemaParsed->attribute("a2").cell_val_num() == TILEDB_VAR_NUM);
      CHECK(schemaParsed->attribute("a3").cell_val_num() == 16);
      CHECK(schemaParsed->attribute("a4").cell_val_num() == TILEDB_VAR_NUM);
      CHECK(schemaParsed->attribute("a4").type() == TILEDB_UINT32);

      auto dims = schemaParsed->domain().dimensions();
      REQUIRE(dims.size() == 2);
      CHECK(dims[0].name() == "d1");
      CHECK(dims[1].name() == "d2");
      CHECK_THROWS(dims[0].domain<float>());
      CHECK(dims[0].domain<double>().first == -100.0);
      CHECK(dims[0].domain<double>().second == 100.0);
      CHECK_THROWS(dims[0].tile_extent<unsigned>());
      CHECK(dims[0].tile_extent<double>() == 10.0);
    }*/

  /*SECTION("Query Serialization") {
    std::string temp_dir = FILE_TEMP_DIR + "query_serialization_test";
    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(sparse_domain_json);
    schema.add_attribute(a1);
    schema.add_attribute(a2);
    schema.add_attribute(a3);
    schema.add_attribute(a4);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);
    schema.set_offsets_compressor({TILEDB_DOUBLE_DELTA, -1});
    schema.set_coords_compressor({TILEDB_ZSTD, -1});

    Array::create(temp_dir, schema);
    Array array(ctx, FILE_TEMP_DIR, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);

    std::string json = schema.to_json();
    CHECK_THAT(
        json,
        Catch::Equals(
            "{\"array_schema\":{\"array_type\":\"dense\","
            "\"attributes\":[{\"cell_val_num\":1,\"compressor\":\"NO_"
            "COMPRESSION\",\"compressor_level\":-1,\"name\":\"a1\","
            "\"type\":\"INT32\"}],\"capacity\":10000,"
            "\"cell_order\":\"row-major\",\"coords_compression\":"
            "\"ZSTD\",\"coords_compression_"
            "level\":-1,\"domain\":{\"cell_order\":\"row-major\","
            "\"dimensions\":[{\"domain\":"
            "[0,99],\"name\":\"d1\",\"null_tile_extent\":false,\"tile_"
            "extent\":10,\"tile_extent_type\":\"INT64\",\"type\":\"INT64\"}],"
            "\"tile_"
            "order\":\"row-major\",\"type\":"
            "\"INT64\"},\"offset_compression\":\"ZSTD\",\"offset_"
            "compression_level\":-1,\"tile_order\":"
            "\"row-major\",\"uri\":\"" +
            temp_dir +
            "query_test\",\"version\":[1,3,0]},\"buffers\":{\"a1\":{"
            "\"buffer\":[1,2,3,4]}},"
            "\"subarray\":[1,4],\"type\":\"WRITE\"}"));

    Query* queryParsed = Query::from_json(ctx, array, json);

    REQUIRE(queryParsed != nullptr);
    REQUIRE(queryParsed->query_type() == TILEDB_WRITE);
    auto resultBuffersMap = queryParsed->result_buffer_elements();
    CHECK(resultBuffersMap.size() == 1);
    auto a1Result = resultBuffersMap.find("a1");
    REQUIRE(a1Result != resultBuffersMap.end());
    CHECK(a1Result->second.second == 4);
  }*/
}
