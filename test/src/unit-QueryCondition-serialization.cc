/**
 * @file unit-QueryCondition-serialization.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Tests the `QueryCondition` serialization paths.
 */

#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "test/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/query_condition.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::sm;

extern "C++" Status condition_from_capnp(const serialization::capnp::Condition::Reader& condition_reader,
    QueryCondition* const condition);

TEST_CASE(
    "QueryCondition serialization: Test basic construction serialization",
    "[QueryCondition][serialization]") {
  std::string field_name = "foo";
  int value = 5;

  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name),
                  &value,
                  sizeof(value),
                  QueryConditionOp::LT)
              .ok());
    tiledb::sm::serialization::capnp::Condition::Builder builder;
    REQUIRE(tiledb::sm::serialization::condition_to_capnp(query_condition1, &builder).ok());
    QueryCondition query_condition1_clone;
    REQUIRE(condition_from_capnp(builder, &query_condition1_clone).ok());
    REQUIRE(tiledb::test::ast_equal(query_condition1.ast(), query_condition1_clone.ast()));
}

TEST_CASE(
    "QueryCondition serialization: Test basic combine construction "
    "serialization",
    "[QueryCondition][serialization]") {
}

TEST_CASE(
    "QueryCondition serialization: Test complex combine construction "
    "serialization",
    "[QueryCondition][serialization]") {
}