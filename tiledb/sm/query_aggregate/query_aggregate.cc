/**
 * @file   query_aggregate.cc
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
 This file implements class QueryAggregate.
 */

#include "tiledb/sm/query_aggregate/query_aggregate.h"

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/query/query.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */
QueryAggregate::QueryAggregate(Query& query) {
  if (query.array()->is_remote()) {
    throw std::logic_error("TODO");
  }
}

/* ********************************* */
/*                API                */
/* ********************************* */
// TODO

}  // namespace sm
}  // namespace tiledb
