/**
 * @file   query_plan.h
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

#ifndef TILEDB_QUERY_PLAN_H
#define TILEDB_QUERY_PLAN_H

#include <string>
#include <vector>
#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Query;
enum class Layout : uint8_t;
enum class ArrayType : uint8_t;

/**
 * Query plan information
 */
class QueryPlan {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /**
   * Default Constructor
   * Deleted, a QueryPlan object doesn't make sense without a Query
   */
  QueryPlan() = delete;

  /**
   * Constructor
   *
   * @param query A query object for which we want to calculate the plan
   */
  QueryPlan(Query& query);

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /**
   * Dump a valid json string representation of the query plan information.
   * This output is designed for human consumption and there is currently
   * no schema that defines the structure of the returned json string.
   *
   * @return a json representation of the query plan
   */
  std::string dump_json(uint32_t indent = 4);

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** The uri of the queried array */
  std::string array_uri_;

  /** The storage backend as seen by VFS */
  std::string vfs_backend_;

  /** Query layout */
  Layout query_layout_;

  /** The strategy name used by the query */
  std::string strategy_name_;

  /** The dense/sparse array type */
  ArrayType array_type_;

  /** A list of queried attributes */
  std::vector<std::string> attributes_;

  /** A list of queried dimensions */
  std::vector<std::string> dimensions_;

  /**
   * Populate query plan from a valid json representation.
   * Only meant to be used during construction when a remote query
   * plan comes as a json string from rest serialization.
   *
   * @param json a json representation of the query plan
   */
  void from_json(const std::string& json);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_PLAN_H
