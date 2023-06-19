/**
 * @file   query_aggregate.h
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
 This file defines class QueryPlan.
 */

#ifndef TILEDB_query_aggregate_H
#define TILEDB_query_aggregate_H

#include <string>
#include <vector>
#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Query;

/**
 * TODO
 */
class QueryAggregate {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /**
   * Default Constructor
   * Deleted, a QueryAggregate object doesn't make sense without a Query
   */
  QueryAggregate() = delete;

  /**
   * Constructor
   *
   * @param query TODO
   */
  QueryAggregate(Query& query);

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  // TODO

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  // TODO
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_AGGREGATE_H
