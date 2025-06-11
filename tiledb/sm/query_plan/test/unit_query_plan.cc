/**
 * @file tiledb/sm/query_plan/test/unit_query_plan.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB Inc.
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
 * This file tests the QueryPlan class
 */

#include <test/support/src/mem_helpers.h>
#include <test/support/src/temporary_local_directory.h>
#include <test/support/tdb_catch.h>
#include "../query_plan.h"
#include "external/include/nlohmann/json.hpp"
#include "test/support/src/mem_helpers.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/storage_format/uri/parse_uri.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

struct QueryPlanFx {
 public:
  QueryPlanFx();

  tdb_unique_ptr<Array> create_array(const URI uri);

  void destroy_array(const std::shared_ptr<Array>& array);

  URI array_uri(const std::string& uri);

  shared_ptr<MemoryTracker> memory_tracker_;

  TemporaryLocalDirectory temp_dir_;
  Config cfg_;
  shared_ptr<Logger> logger_;
  ContextResources resources_;
  shared_ptr<StorageManager> sm_;
};

tdb_unique_ptr<Array> QueryPlanFx::create_array(const URI uri) {
  // Create Domain
  uint64_t dim_dom[2]{0, 1};
  uint64_t tile_extent = 1;
  shared_ptr<Dimension> dim = make_shared<Dimension>(
      HERE(), std::string("dim"), Datatype::UINT64, memory_tracker_);
  dim->set_domain(&dim_dom);
  dim->set_tile_extent(&tile_extent);

  std::vector<shared_ptr<Dimension>> dims = {dim};
  shared_ptr<Domain> domain = make_shared<Domain>(
      HERE(), Layout::ROW_MAJOR, dims, Layout::ROW_MAJOR, memory_tracker_);

  // Create the ArraySchema
  shared_ptr<ArraySchema> schema = make_shared<ArraySchema>(
      HERE(), ArrayType::DENSE, tiledb::test::create_test_memory_tracker());
  schema->set_domain(domain);
  schema->add_attribute(
      make_shared<Attribute>(
          HERE(), std::string("attr"), Datatype::UINT64, false),
      false);
  EncryptionKey key;
  throw_if_not_ok(key.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));

  // Create the (empty) array on disk.
  Array::create(resources_, uri, schema, key);
  tdb_unique_ptr<Array> array(new Array{resources_, uri});

  return array;
}

void QueryPlanFx::destroy_array(const std::shared_ptr<Array>& array) {
  REQUIRE(array->close().ok());
}

URI QueryPlanFx::array_uri(const std::string& array_name) {
  return URI(temp_dir_.path() + array_name);
}

QueryPlanFx::QueryPlanFx()
    : memory_tracker_(tiledb::test::create_test_memory_tracker())
    , logger_(make_shared<Logger>(HERE(), "foo"))
    , resources_(cfg_, logger_, 1, 1, "")
    , sm_(make_shared<StorageManager>(resources_, logger_, cfg_)) {
}

TEST_CASE_METHOD(QueryPlanFx, "Query plan dump_json", "[query_plan][dump]") {
  const URI uri = array_uri("query_plan_array");

  auto array = create_array(uri);

  auto st =
      array->open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);
  REQUIRE(st.ok());

  shared_ptr<Array> array_shared = std::move(array);
  Query query(
      resources_, CancellationSource(sm_.get()), sm_.get(), array_shared);
  REQUIRE(query.set_layout(Layout::ROW_MAJOR).ok());

  stats::Stats stats("foo");
  Subarray subarray(array_shared.get(), &stats, logger_);
  uint64_t r[2]{0, 1};
  subarray.add_range(0, r, r + 1);
  query.set_subarray(subarray);

  std::vector<uint64_t> data(2);
  uint64_t size = 2;
  REQUIRE(query.set_data_buffer("attr", data.data(), &size).ok());

  QueryPlan plan(query);

  nlohmann::json json_plan = nlohmann::json::parse(plan.dump_json());

  CHECK(json_plan["TileDB Query Plan"]["Array.URI"] == uri.to_string());
  CHECK(json_plan["TileDB Query Plan"]["Array.Type"] == "dense");
  CHECK(json_plan["TileDB Query Plan"]["VFS.Backend"] == uri.backend_name());
  CHECK(json_plan["TileDB Query Plan"]["Query.Layout"] == "row-major");
  CHECK(json_plan["TileDB Query Plan"]["Query.Strategy.Name"] == "DenseReader");
  CHECK(
      json_plan["TileDB Query Plan"]["Query.Attributes"] ==
      std::vector({"attr"}));
  CHECK(
      json_plan["TileDB Query Plan"]["Query.Dimensions"] ==
      std::vector<std::string>({"dim"}));

  destroy_array(array_shared);
}
