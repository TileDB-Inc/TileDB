/**
 * @file unit-cppapi-aggregates.cc
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
 * Tests the aggregates C++ API.
 */

#include <fstream>

#include <test/support/tdb_catch.h>
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;

struct CPPAggregatesFx {
  CPPAggregatesFx();
  ~CPPAggregatesFx();

  void create_array();
  void rm_array();

  std::string uri_;
  Context ctx_;
  VFS vfs_;
};

CPPAggregatesFx::CPPAggregatesFx()
    : uri_("aggregates_test_array")
    , vfs_(ctx_) {
  rm_array();
  Config cfg;
  cfg["sm.allow_aggregates_experimental"] = "true";
  ctx_ = Context(cfg);
  create_array();
}

CPPAggregatesFx::~CPPAggregatesFx() {
  rm_array();
}

void CPPAggregatesFx::rm_array() {
  if (vfs_.is_dir(uri_)) {
    vfs_.remove_dir(uri_);
  }
}

void CPPAggregatesFx::create_array() {
  // dim = {1, 2, 3, 4, 5}
  // attr1 = {"fred", "wilma", "barney", "wilma", "fred"}
  // attr2 = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}
  ArraySchema schema(ctx_, TILEDB_DENSE);

  auto dim = Dimension::create<int>(ctx_, "dim", {{-100, 100}});
  auto dom = Domain(ctx_);
  dom.add_dimension(dim);
  schema.set_domain(dom);

  auto attr2 = Attribute::create<float>(ctx_, "attr2");
  schema.add_attribute(attr2);

  Array::create(uri_, schema);

  // Attribute data
  std::vector<float> attr2_values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};

  Array array(ctx_, uri_, TILEDB_WRITE);
  Subarray subarray(ctx_, array);
  subarray.set_subarray({1, 5});
  Query query(ctx_, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("attr2", attr2_values);
  CHECK_NOTHROW(query.submit());
  query.finalize();
  array.close();
}

TEST_CASE_METHOD(
    CPPAggregatesFx,
    "CPP: Aggregates Query - Basic",
    "[aggregates][query][sum][basic]") {
  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  query.add_range("dim", 1, 5).set_layout(TILEDB_ROW_MAJOR);

  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  ChannelOperation operation =
      QueryExperimental::create_unary_aggregate<SumOperator>(query, "attr2");
  default_channel.apply_aggregate("Sum", operation);

  double sum = 0;
  uint64_t size = 8;

  // TODO: use proper set_data_buffer c++ API
  REQUIRE(query.ptr()->query_->set_data_buffer("Sum", &sum, &size).ok());

  REQUIRE(query.submit() == Query::Status::COMPLETE);

  CHECK(sum == 15);
}

TEST_CASE_METHOD(
    CPPAggregatesFx,
    "CPP: Aggregates Query - Basic",
    "[aggregates][query][count][basic]") {
  auto array = Array(ctx_, uri_, TILEDB_READ);
  Query query(ctx_, array);
  query.add_range("dim", 1, 3).set_layout(TILEDB_ROW_MAJOR);

  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  default_channel.apply_aggregate("Count", CountOperation{});

  uint64_t count = 0;
  uint64_t size = 8;
  // TODO: use proper set_data_buffer c++ API
  REQUIRE(query.ptr()->query_->set_data_buffer("Count", &count, &size).ok());

  REQUIRE(query.submit() == Query::Status::COMPLETE);

  CHECK(count == 3);
}
