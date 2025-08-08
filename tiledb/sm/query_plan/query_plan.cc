/**
 * @file   query_plan.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 This file implements class QueryPlan.
 */

#include "tiledb/sm/query_plan/query_plan.h"

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/rest/rest_client.h"

#include <nlohmann/json.hpp>

using namespace tiledb::common;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */
QueryPlan::QueryPlan(Query& query) {
  if (query.array()->is_remote()) {
    auto rest_client = query.rest_client();
    if (!rest_client) {
      throw std::runtime_error(
          "Failed to create a query plan; Remote query"
          "with no REST client.");
    }

    rest_client->post_query_plan_from_rest(
        query.array()->array_uri(), query, *this);

    // We need to transition the query status to INITIALIZED to mimic the
    // behavior of getting a query plan locally
    query.set_status(QueryStatus::INITIALIZED);

    return;
  }

  array_uri_ = query.array()->array_uri().to_string();
  vfs_backend_ = URI(array_uri_).backend_name();
  query_layout_ = query.layout();

  // This most probably ends up creating the strategy on the query
  auto strategy_ptr = query.strategy();
  strategy_name_ = strategy_ptr->name();

  array_type_ = query.array()->array_schema_latest().array_type();

  for (auto& buf : query.buffer_names()) {
    if (query.array()->array_schema_latest().is_dim(buf)) {
      dimensions_.push_back(buf);
    } else {
      attributes_.push_back(buf);
    }
  }
  if (query.is_dense()) {
    dimensions_ = query.array()->array_schema_latest().dim_names();
  }

  std::sort(attributes_.begin(), attributes_.end());
  std::sort(dimensions_.begin(), dimensions_.end());
}

QueryPlan::QueryPlan(
    Query& query,
    Layout layout,
    const std::string& strategy_name,
    ArrayType array_type,
    const std::vector<std::string>& attributes,
    const std::vector<std::string>& dimensions)
    : array_uri_{query.array()->array_uri().to_string()}
    , vfs_backend_{URI(array_uri_).backend_name()}
    , query_layout_{layout}
    , strategy_name_{strategy_name}
    , array_type_{array_type}
    , attributes_{attributes}
    , dimensions_{dimensions} {
}

/* ********************************* */
/*                API                */
/* ********************************* */
std::string QueryPlan::dump_json(uint32_t indent) {
  nlohmann::json rv;
  if (!URI(array_uri_).is_tiledb()) {
    rv = {
        {"TileDB Query Plan",
         {{"Array.URI", array_uri_},
          {"Array.Type", array_type_str(array_type_)},
          {"VFS.Backend", vfs_backend_},
          {"Query.Layout", layout_str(query_layout_)},
          {"Query.Strategy.Name", strategy_name_},
          {"Query.Attributes", attributes_},
          {"Query.Dimensions", dimensions_}}}};
  } else {
    rv = {
        {"TileDB Query Plan",
         {{"Array.URI", array_uri_},
          {"Array.Type", array_type_str(array_type_)},
          {"Query.Layout", layout_str(query_layout_)},
          {"Query.Strategy.Name", strategy_name_},
          {"Query.Attributes", attributes_},
          {"Query.Dimensions", dimensions_}}}};
  }

  return rv.dump(indent);
}

}  // namespace tiledb::sm
