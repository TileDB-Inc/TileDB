/**
 * @file   query_plan.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/query/query.h"

#include "external/include/nlohmann/json.hpp"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */
QueryPlan::QueryPlan(Query& query) {
  if (query.array()->is_remote()) {
    throw std::logic_error(
        "Failed to create a query plan; Remote arrays"
        "are not currently supported.");
  }

  set_array_uri(query.array()->array_uri().to_string());
  set_vfs_backend(URI(array_uri_).backend_name());
  set_query_layout(query.layout());

  // This most probably ends up creating the strategy on the query
  auto strategy_ptr = query.strategy();
  set_strategy_name(strategy_ptr->name());

  set_array_type(query.array()->array_schema_latest().array_type());

  std::vector<std::string> attrs;
  std::vector<std::string> dims;
  for (auto& buf : query.buffer_names()) {
    if (query.array()->array_schema_latest().is_dim(buf)) {
      dims.push_back(buf);
    } else {
      attrs.push_back(buf);
    }
  }
  if (query.is_dense()) {
    dims = query.array()->array_schema_latest().dim_names();
  }
  set_attributes(attrs);
  set_dimensions(dims);
}

/* ********************************* */
/*                API                */
/* ********************************* */
std::string QueryPlan::dump_json(uint32_t indent) {
  nlohmann::json rv = {
      {"TileDB Query Plan",
       {{"Array.URI", array_uri_},
        {"Array.Type", array_type_str(array_type_)},
        {"VFS.Backend", vfs_backend_},
        {"Query.Layout", layout_str(query_layout_)},
        {"Query.Strategy.Name", strategy_name_},
        {"Query.Attributes", attributes_},
        {"Query.Dimensions", dimensions_}}}};

  return rv.dump(indent);
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */
void QueryPlan::set_array_uri(const std::string& uri) {
  array_uri_ = uri;
}

void QueryPlan::set_vfs_backend(const std::string& backend) {
  vfs_backend_ = backend;
}

void QueryPlan::set_query_layout(Layout layout) {
  query_layout_ = layout;
}

void QueryPlan::set_strategy_name(const std::string& strategy) {
  strategy_name_ = strategy;
}

void QueryPlan::set_array_type(ArrayType type) {
  array_type_ = type;
}

void QueryPlan::set_attributes(const std::vector<std::string>& attrs) {
  attributes_ = attrs;
}

void QueryPlan::set_dimensions(const std::vector<std::string>& dims) {
  dimensions_ = dims;
}

}  // namespace sm
}  // namespace tiledb
