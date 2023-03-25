/**
 * @file unit-cppapi-dictionary.cc
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
 * Tests the C++ API for dictionary related functions.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

struct DirectoryCreator {
  DirectoryCreator(tiledb::VFS& vfs, std::string uri) : vfs_(vfs), uri_(uri){
    rm_dir();
  }

  ~DirectoryCreator() {
    rm_dir();
  }

  void rm_dir() {
    if (vfs_.is_dir(uri_)) {
      vfs_.remove_dir(uri_);
    }
  }

  tiledb::VFS& vfs_;
  std::string uri_;
};

TEST_CASE("C++ API: Dictionary basics", "[cppapi][dictionary]") {
  Context ctx;
  std::vector<std::string> dict_values = {
    "red",
    "green",
    "blue"
  };
  CHECK_NOTHROW(Dictionary::create(ctx, dict_values));
}

TEST_CASE("C++ API: Dictionary storage", "[cppapi][dictionary]") {
  std::string uri = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  DirectoryCreator(vfs, uri);

  Domain dom(ctx);
  auto dim = Dimension::create<int>(ctx, "dim", {{0, 100}}, 10);
  dom.add_dimensions(dim);

  std::vector<std::string> dict_values = {
    "red", "orange", "yellow", "green", "blue", "indigo", "violet"};
  auto dict = Dictionary::create(ctx, dict_values);
  auto attr = Attribute::create<int>(ctx, "attr");
  attr.set_dictionary(dict);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(dom);
  schema.add_attributes(attr);

  CHECK_NOTHROW(Array::create(uri, schema));
}

TEST_CASE("C++ API: Dictionary read after creation", "[cppapi][dictionary]") {
  std::string uri = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  DirectoryCreator(vfs, uri);

  Domain dom(ctx);
  auto dim = Dimension::create<int>(ctx, "dim", {{0, 100}}, 10);
  dom.add_dimensions(dim);

  std::vector<std::string> dict_values = {
    "red", "orange", "yellow", "green", "blue", "indigo", "violet"};
  auto dict = Dictionary::create(ctx, dict_values);
  auto attr = Attribute::create<int>(ctx, "attr");
  attr.set_dictionary(dict);

  ArraySchema schema_w(ctx, TILEDB_SPARSE);
  schema_w.set_domain(dom);
  schema_w.add_attributes(attr);

  CHECK_NOTHROW(Array::create(uri, schema_w));

  std::vector<int> w_dim_data = {0, 1, 2, 3, 4, 5};
  std::vector<int> w_attr_data = {0, 0, 2, 3, 6, 1};

  Array array_w(ctx, uri, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_layout(TILEDB_UNORDERED);
  query_w.set_data_buffer("dim", w_dim_data);
  query_w.set_data_buffer("attr", w_attr_data);
  CHECK_NOTHROW(query_w.submit());
  query_w.finalize();
  array_w.close();

  std::vector<int> r_dim_data(6);
  std::vector<int> r_attr_data(6);

  Array array(ctx, uri, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_data_buffer("dim", r_dim_data.data(), r_dim_data.size());
  query.set_data_buffer("attr", r_attr_data.data(), r_attr_data.size());
  CHECK_NOTHROW(query.submit());
  query.finalize();

  CHECK(std::memcmp(w_dim_data.data(), r_dim_data.data(), r_dim_data.size()) == 0);
  CHECK(std::memcmp(w_attr_data.data(), r_attr_data.data(), r_attr_data.size()) == 0);

  auto schema = array.schema();
  auto r_attr = schema.attribute("attr");
  auto r_dict = r_attr.get_dictionary();
  REQUIRE(r_dict.has_value());

  auto values = r_dict->get_values<std::string>();
  REQUIRE(values.size() == dict_values.size());

  for(size_t i = 0; i < values.size(); i++) {
    REQUIRE(values[i] == dict_values[i]);
  }
}
