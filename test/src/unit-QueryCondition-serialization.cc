/**
 * @file unit-QueryCondition-serialization.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
#include <capnp/message.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#include "tiledb/sm/serialization/query.h"
#endif

#include "test/support/src/ast_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/enums/query_condition_combination_op.h"
#include "tiledb/sm/enums/query_condition_op.h"
#include "tiledb/sm/query/query_condition.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE(
    "QueryCondition serialization: Test serialization",
    "[QueryCondition][serialization]") {
  QueryCondition query_condition;

  SECTION("Test serialization, basic") {
    std::string field_name = "x";
    int val = 5;
    REQUIRE(query_condition
                .init(
                    std::string(field_name),
                    &val,
                    sizeof(int),
                    QueryConditionOp::LT)
                .ok());
  }

  SECTION("Test serialization, basic AND combine") {
    std::string field_name1 = "x";
    int val1 = 5;
    QueryCondition query_condition1;
    REQUIRE(query_condition1
                .init(
                    std::string(field_name1),
                    &val1,
                    sizeof(int),
                    QueryConditionOp::LT)
                .ok());

    std::string field_name2 = "y";
    int val2 = 3;
    QueryCondition query_condition2;
    REQUIRE(query_condition2
                .init(
                    std::string(field_name2),
                    &val2,
                    sizeof(int),
                    QueryConditionOp::GT)
                .ok());

    REQUIRE(query_condition1
                .combine(
                    query_condition2,
                    QueryConditionCombinationOp::AND,
                    &query_condition)
                .ok());
  }

  SECTION("Test serialization, basic OR combine") {
    std::string field_name1 = "x";
    int val1 = 5;
    QueryCondition query_condition1;
    REQUIRE(query_condition1
                .init(
                    std::string(field_name1),
                    &val1,
                    sizeof(int),
                    QueryConditionOp::LT)
                .ok());

    std::string field_name2 = "y";
    int val2 = 3;
    QueryCondition query_condition2;
    REQUIRE(query_condition2
                .init(
                    std::string(field_name2),
                    &val2,
                    sizeof(int),
                    QueryConditionOp::GT)
                .ok());

    REQUIRE(query_condition1
                .combine(
                    query_condition2,
                    QueryConditionCombinationOp::OR,
                    &query_condition)
                .ok());
  }

  SECTION("Test serialization, OR of 2 AND ASTs") {
    std::string field_name1 = "x";
    int val1 = 5;
    QueryCondition query_condition1;
    REQUIRE(query_condition1
                .init(
                    std::string(field_name1),
                    &val1,
                    sizeof(int),
                    QueryConditionOp::LT)
                .ok());

    std::string field_name2 = "y";
    int val2 = 3;
    QueryCondition query_condition2;
    REQUIRE(query_condition2
                .init(
                    std::string(field_name2),
                    &val2,
                    sizeof(int),
                    QueryConditionOp::GT)
                .ok());

    QueryCondition combined_and1;
    REQUIRE(query_condition1
                .combine(
                    query_condition2,
                    QueryConditionCombinationOp::AND,
                    &combined_and1)
                .ok());

    std::string field_name3 = "a";
    int val3 = 1;
    QueryCondition query_condition3;
    REQUIRE(query_condition3
                .init(
                    std::string(field_name3),
                    &val3,
                    sizeof(int),
                    QueryConditionOp::EQ)
                .ok());

    std::string field_name4 = "b";
    int val4 = 2;
    QueryCondition query_condition4;
    REQUIRE(query_condition4
                .init(
                    std::string(field_name4),
                    &val4,
                    sizeof(int),
                    QueryConditionOp::NE)
                .ok());

    QueryCondition combined_and2;
    REQUIRE(query_condition3
                .combine(
                    query_condition4,
                    QueryConditionCombinationOp::AND,
                    &combined_and2)
                .ok());

    REQUIRE(combined_and1
                .combine(
                    combined_and2,
                    QueryConditionCombinationOp::OR,
                    &query_condition)
                .ok());
  }

  SECTION("Test serialization, AND of 2 OR ASTs") {
    std::string field_name1 = "x";
    int val1 = 5;
    QueryCondition query_condition1;
    REQUIRE(query_condition1
                .init(
                    std::string(field_name1),
                    &val1,
                    sizeof(int),
                    QueryConditionOp::LT)
                .ok());

    std::string field_name2 = "y";
    int val2 = 3;
    QueryCondition query_condition2;
    REQUIRE(query_condition2
                .init(
                    std::string(field_name2),
                    &val2,
                    sizeof(int),
                    QueryConditionOp::GT)
                .ok());

    QueryCondition combined_or1;
    REQUIRE(query_condition1
                .combine(
                    query_condition2,
                    QueryConditionCombinationOp::OR,
                    &combined_or1)
                .ok());

    std::string field_name3 = "a";
    int val3 = 1;
    QueryCondition query_condition3;
    REQUIRE(query_condition3
                .init(
                    std::string(field_name3),
                    &val3,
                    sizeof(int),
                    QueryConditionOp::EQ)
                .ok());

    std::string field_name4 = "b";
    int val4 = 2;
    QueryCondition query_condition4;
    REQUIRE(query_condition4
                .init(
                    std::string(field_name4),
                    &val4,
                    sizeof(int),
                    QueryConditionOp::NE)
                .ok());

    QueryCondition combined_or2;
    REQUIRE(query_condition3
                .combine(
                    query_condition4,
                    QueryConditionCombinationOp::OR,
                    &combined_or2)
                .ok());

    REQUIRE(combined_or1
                .combine(
                    combined_or2,
                    QueryConditionCombinationOp::AND,
                    &query_condition)
                .ok());
  }

  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::Condition::Builder condition_builder =
      message.initRoot<tiledb::sm::serialization::capnp::Condition>();
  REQUIRE(tiledb::sm::serialization::condition_to_capnp(
              query_condition, &condition_builder)
              .ok());
  auto query_condition_clone =
      tiledb::sm::serialization::condition_from_capnp(condition_builder);
  REQUIRE(tiledb::test::ast_equal(
      query_condition.ast(), query_condition_clone.ast()));
}
