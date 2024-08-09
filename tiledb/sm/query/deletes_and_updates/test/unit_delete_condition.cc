/**
 * @file unit_delete_condition.cc
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
 * Tests the delete condition serialization.
 */

#include "test/support/src/ast_helpers.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/query/deletes_and_updates/serialization.h"
#include "tiledb/sm/storage_manager/context.h"

#include <catch2/catch_test_macros.hpp>

using namespace tiledb::sm;
using namespace tiledb::sm::deletes_and_updates::serialization;

/**
 * Make sure a condition is the same after going through
 * serialization/deserialization.
 *
 * @param query_condition Condition to check.
 */
void serialize_deserialize_check(QueryCondition& query_condition) {
  auto tracker = tiledb::test::create_test_memory_tracker();
  auto serialized = serialize_condition(query_condition, tracker);
  auto deserialized =
      deserialize_condition(0, "", serialized->data(), serialized->size());

  CHECK(tiledb::test::ast_equal(query_condition.ast(), deserialized.ast()));
}

TEST_CASE("DeleteCondition: Test char", "[deletecondition][char_value]") {
  std::string field_name = "foo";
  char value[] = "bar";

  QueryCondition query_condition;
  REQUIRE(query_condition
              .init(
                  std::string(field_name),
                  &value,
                  strlen(value),
                  QueryConditionOp::LT)
              .ok());
  serialize_deserialize_check(query_condition);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, basic", "[deletecondition][ast]") {
  std::string field_name = "x";
  int val = 0x12345678;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  serialize_deserialize_check(query_condition);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, basic AND combine",
    "[deletecondition][ast]") {
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  serialize_deserialize_check(query_condition);

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  serialize_deserialize_check(combined_and);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, basic OR combine",
    "[deletecondition][ast]") {
  // OR combine.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  serialize_deserialize_check(query_condition);

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  serialize_deserialize_check(combined_or);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, basic AND combine, string",
    "[deletecondition][ast]") {
  char e[] = "eve";
  QueryCondition query_condition;
  REQUIRE(query_condition.init("x", &e, strlen(e), QueryConditionOp::LT).ok());
  serialize_deserialize_check(query_condition);

  char b[] = "bob";
  QueryCondition query_condition1;
  REQUIRE(query_condition1.init("x", &b, strlen(b), QueryConditionOp::GT).ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  serialize_deserialize_check(combined_and);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, basic OR combine, string",
    "[deletecondition][ast]") {
  char e[] = "eve";
  QueryCondition query_condition;
  REQUIRE(query_condition.init("x", &e, strlen(e), QueryConditionOp::LT).ok());
  serialize_deserialize_check(query_condition);

  char b[] = "bob";
  QueryCondition query_condition1;
  REQUIRE(query_condition1.init("x", &b, strlen(b), QueryConditionOp::GT).ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  serialize_deserialize_check(combined_or);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, tree structure, AND of 2 OR ASTs",
    "[deletecondition][ast]") {
  // First OR compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  serialize_deserialize_check(query_condition);

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  serialize_deserialize_check(combined_or);

  // Second OR compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  serialize_deserialize_check(query_condition2);

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition3);

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  serialize_deserialize_check(combined_or1);

  QueryCondition combined_and;
  REQUIRE(combined_or
              .combine(
                  combined_or1, QueryConditionCombinationOp::AND, &combined_and)
              .ok());
  serialize_deserialize_check(combined_and);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, tree structure, OR of 2 AND ASTs",
    "[deletecondition][ast]") {
  // First AND compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  serialize_deserialize_check(query_condition);

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  serialize_deserialize_check(combined_and);

  // Second AND compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  serialize_deserialize_check(query_condition2);

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition3);

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  serialize_deserialize_check(combined_and1);

  QueryCondition combined_or;
  REQUIRE(
      combined_and
          .combine(combined_and1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  serialize_deserialize_check(combined_or);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, tree structure with same "
    "combining operator, OR of 2 OR ASTs",
    "[deletecondition][ast]") {
  // First OR compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  serialize_deserialize_check(query_condition);

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_or;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::OR, &combined_or)
          .ok());
  serialize_deserialize_check(combined_or);

  // Second OR compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  serialize_deserialize_check(query_condition2);

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition3);

  QueryCondition combined_or1;
  REQUIRE(
      query_condition2
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  serialize_deserialize_check(combined_or1);

  QueryCondition combined_or2;
  REQUIRE(
      combined_or
          .combine(combined_or1, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  serialize_deserialize_check(combined_or2);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, tree structure with same "
    "combining operator, AND of 2 AND ASTs",
    "[deletecondition][ast]") {
  // First AND compound AST.
  std::string field_name = "x";
  int val = 0xabcdef12;
  QueryCondition query_condition;
  REQUIRE(
      query_condition
          .init(
              std::string(field_name), &val, sizeof(int), QueryConditionOp::LT)
          .ok());
  serialize_deserialize_check(query_condition);

  std::string field_name1 = "y";
  int val1 = 0x33333333;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::GT)
              .ok());
  serialize_deserialize_check(query_condition1);

  QueryCondition combined_and;
  REQUIRE(
      query_condition
          .combine(
              query_condition1, QueryConditionCombinationOp::AND, &combined_and)
          .ok());
  serialize_deserialize_check(combined_and);

  // Second AND compound AST.
  std::string field_name2 = "a";
  int val2 = 0x12121212;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::EQ)
              .ok());
  serialize_deserialize_check(query_condition2);

  std::string field_name3 = "b";
  int val3 = 0x34343434;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition3);

  QueryCondition combined_and1;
  REQUIRE(query_condition2
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  serialize_deserialize_check(combined_and1);

  QueryCondition combined_and2;
  REQUIRE(
      combined_and
          .combine(
              combined_and1, QueryConditionCombinationOp::AND, &combined_and2)
          .ok());
  serialize_deserialize_check(combined_and2);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, adding simple clauses to AND tree",
    "[deletecondition][ast]") {
  // foo != 0xaaaaaaaa && foo != 0xbbbbbbbb && foo != 0xcccccccc && foo !=
  // 0xdddddddd && foo != 0xeeeeeeee
  std::string field_name1 = "foo";
  int val1 = 0xaaaaaaaa;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition1);

  std::string field_name2 = "foo";
  int val2 = 0xbbbbbbbb;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition2);

  std::string field_name3 = "foo";
  int val3 = 0xcccccccc;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition3);

  std::string field_name4 = "foo";
  int val4 = 0xdddddddd;
  QueryCondition query_condition4;
  REQUIRE(query_condition4
              .init(
                  std::string(field_name4),
                  &val4,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition4);

  std::string field_name5 = "foo";
  int val5 = 0xeeeeeeee;
  QueryCondition query_condition5;
  REQUIRE(query_condition5
              .init(
                  std::string(field_name5),
                  &val5,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition5);

  QueryCondition combined_and1;
  REQUIRE(query_condition1
              .combine(
                  query_condition2,
                  QueryConditionCombinationOp::AND,
                  &combined_and1)
              .ok());
  serialize_deserialize_check(combined_and1);

  QueryCondition combined_and2;
  REQUIRE(combined_and1
              .combine(
                  query_condition3,
                  QueryConditionCombinationOp::AND,
                  &combined_and2)
              .ok());
  serialize_deserialize_check(combined_and2);

  QueryCondition combined_and3;
  REQUIRE(combined_and2
              .combine(
                  query_condition4,
                  QueryConditionCombinationOp::AND,
                  &combined_and3)
              .ok());
  serialize_deserialize_check(combined_and3);

  QueryCondition combined_and4;
  REQUIRE(combined_and3
              .combine(
                  query_condition5,
                  QueryConditionCombinationOp::AND,
                  &combined_and4)
              .ok());
  serialize_deserialize_check(combined_and4);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, adding simple clauses to OR tree",
    "[deletecondition][ast]") {
  // foo != 0xaaaaaaaa OR foo != 0xbbbbbbbb OR foo != 0xcccccccc OR foo !=
  // 0xdddddddd OR foo != 0xeeeeeeee
  std::string field_name1 = "foo";
  int val1 = 0xaaaaaaaa;
  QueryCondition query_condition1;
  REQUIRE(query_condition1
              .init(
                  std::string(field_name1),
                  &val1,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition1);

  std::string field_name2 = "foo";
  int val2 = 0xbbbbbbbb;
  QueryCondition query_condition2;
  REQUIRE(query_condition2
              .init(
                  std::string(field_name2),
                  &val2,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition2);

  std::string field_name3 = "foo";
  int val3 = 0xcccccccc;
  QueryCondition query_condition3;
  REQUIRE(query_condition3
              .init(
                  std::string(field_name3),
                  &val3,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition3);

  std::string field_name4 = "foo";
  int val4 = 0xdddddddd;
  QueryCondition query_condition4;
  REQUIRE(query_condition4
              .init(
                  std::string(field_name4),
                  &val4,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition4);

  std::string field_name5 = "foo";
  int val5 = 0xeeeeeeee;
  QueryCondition query_condition5;
  REQUIRE(query_condition5
              .init(
                  std::string(field_name5),
                  &val5,
                  sizeof(int),
                  QueryConditionOp::NE)
              .ok());
  serialize_deserialize_check(query_condition5);

  QueryCondition combined_or1;
  REQUIRE(
      query_condition1
          .combine(
              query_condition2, QueryConditionCombinationOp::OR, &combined_or1)
          .ok());
  serialize_deserialize_check(combined_or1);

  QueryCondition combined_or2;
  REQUIRE(
      combined_or1
          .combine(
              query_condition3, QueryConditionCombinationOp::OR, &combined_or2)
          .ok());
  serialize_deserialize_check(combined_or2);

  QueryCondition combined_or3;
  REQUIRE(
      combined_or2
          .combine(
              query_condition4, QueryConditionCombinationOp::OR, &combined_or3)
          .ok());
  serialize_deserialize_check(combined_or3);

  QueryCondition combined_or4;
  REQUIRE(
      combined_or3
          .combine(
              query_condition5, QueryConditionCombinationOp::OR, &combined_or4)
          .ok());
  serialize_deserialize_check(combined_or4);
}

TEST_CASE(
    "DeleteCondition: Test AST construction, complex tree with depth > 2",
    "[deletecondition][ast]") {
  std::vector<int> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<QueryCondition> qc_value_vector;
  for (int i = 0; i < 7; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::EQ).ok());
    serialize_deserialize_check(qc);
    qc_value_vector.push_back(qc);
  }

  for (int i = 7; i < 9; ++i) {
    QueryCondition qc;
    REQUIRE(qc.init("x", &vals[i], sizeof(vals[i]), QueryConditionOp::NE).ok());
    serialize_deserialize_check(qc);
    qc_value_vector.push_back(qc);
  }

  int x = 6;
  QueryCondition x_neq_six;
  REQUIRE(x_neq_six.init("x", &x, sizeof(x), QueryConditionOp::NE).ok());
  CHECK(tiledb::test::ast_node_to_str(x_neq_six.ast()) == "x NE 06 00 00 00");
  serialize_deserialize_check(x_neq_six);

  QueryCondition one_or_two;
  REQUIRE(
      qc_value_vector[0]
          .combine(
              qc_value_vector[1], QueryConditionCombinationOp::OR, &one_or_two)
          .ok());
  serialize_deserialize_check(one_or_two);

  QueryCondition three_or_four;
  REQUIRE(qc_value_vector[2]
              .combine(
                  qc_value_vector[3],
                  QueryConditionCombinationOp::OR,
                  &three_or_four)
              .ok());
  serialize_deserialize_check(three_or_four);

  QueryCondition six_or_seven;
  REQUIRE(qc_value_vector[5]
              .combine(
                  qc_value_vector[6],
                  QueryConditionCombinationOp::OR,
                  &six_or_seven)
              .ok());
  serialize_deserialize_check(six_or_seven);

  QueryCondition eight_and_nine;
  REQUIRE(qc_value_vector[7]
              .combine(
                  qc_value_vector[8],
                  QueryConditionCombinationOp::AND,
                  &eight_and_nine)
              .ok());
  serialize_deserialize_check(eight_and_nine);

  QueryCondition subtree_a;
  REQUIRE(
      one_or_two
          .combine(three_or_four, QueryConditionCombinationOp::AND, &subtree_a)
          .ok());
  serialize_deserialize_check(subtree_a);

  QueryCondition subtree_d;
  REQUIRE(
      eight_and_nine
          .combine(six_or_seven, QueryConditionCombinationOp::AND, &subtree_d)
          .ok());
  serialize_deserialize_check(subtree_d);

  QueryCondition subtree_c;
  REQUIRE(
      subtree_d
          .combine(
              qc_value_vector[4], QueryConditionCombinationOp::OR, &subtree_c)
          .ok());
  serialize_deserialize_check(subtree_c);

  QueryCondition subtree_b;
  REQUIRE(
      subtree_c.combine(x_neq_six, QueryConditionCombinationOp::AND, &subtree_b)
          .ok());
  serialize_deserialize_check(subtree_b);

  QueryCondition qc;
  REQUIRE(
      subtree_a.combine(subtree_b, QueryConditionCombinationOp::OR, &qc).ok());
  serialize_deserialize_check(qc);
}
