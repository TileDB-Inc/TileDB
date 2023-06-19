/**
 * @file tiledb/sm/query_aggregate/test/unit_query_aggregate.cc
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
 * This file tests the QueryAggregate class
 */

#include <test/support/tdb_catch.h>
#include "../query_aggregate.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/storage_format/uri/parse_uri.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

struct QueryAggregateFx {
 public:
  QueryAggregateFx();

  tdb_unique_ptr<Array> create_array(const URI uri);

  void destroy_array(const std::shared_ptr<Array>& array);

  Config cfg_;
  shared_ptr<Logger> logger_;
  ContextResources resources_;
  shared_ptr<StorageManager> sm_;
};

tdb_unique_ptr<Array> QueryAggregateFx::create_array(const URI uri) {
  // Create Domain
  uint64_t dim_dom[2]{0, 1};
  uint64_t tile_extent = 1;
  shared_ptr<Dimension> dim =
      make_shared<Dimension>(HERE(), std::string("dim"), Datatype::UINT64);
  throw_if_not_ok(dim->set_domain(&dim_dom));
  throw_if_not_ok(dim->set_tile_extent(&tile_extent));

  std::vector<shared_ptr<Dimension>> dims = {dim};
  shared_ptr<Domain> domain =
      make_shared<Domain>(HERE(), Layout::ROW_MAJOR, dims, Layout::ROW_MAJOR);

  // Create the ArraySchema
  shared_ptr<ArraySchema> schema =
      make_shared<ArraySchema>(HERE(), ArrayType::DENSE);
  throw_if_not_ok(schema->set_domain(domain));
  throw_if_not_ok(schema->add_attribute(
      make_shared<Attribute>(
          HERE(), std::string("attr"), Datatype::UINT64, false),
      false));
  EncryptionKey key;
  throw_if_not_ok(key.set_key(EncryptionType::NO_ENCRYPTION, nullptr, 0));

  // Create the (empty) array on disk.
  Status st = sm_->array_create(uri, schema, key);
  if (!st.ok()) {
    throw std::runtime_error("Could not create array.");
  }
  tdb_unique_ptr<Array> array(new Array{uri, sm_.get()});

  return array;
}

void QueryAggregateFx::destroy_array(const std::shared_ptr<Array>& array) {
  REQUIRE(array->close().ok());
  REQUIRE(sm_->vfs()->remove_dir(array->array_uri()).ok());
}

QueryAggregateFx::QueryAggregateFx()
    : logger_(make_shared<Logger>(HERE(), "foo"))
    , resources_(cfg_, logger_, 1, 1, "")
    , sm_(make_shared<StorageManager>(resources_, logger_, cfg_)) {
}

TEST_CASE_METHOD(QueryAggregateFx, "TODO", "[query_aggregate][todo]") {
  // TODO
}
