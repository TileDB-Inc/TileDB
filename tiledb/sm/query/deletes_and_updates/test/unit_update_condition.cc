/**
 * @file unit_update_condition.cc
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
 * Tests the update condition serialization.
 */

#include "test/support/src/ast_helpers.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/query/deletes_and_updates/serialization.h"
#include "tiledb/sm/storage_manager/context.h"

#include <catch2/catch_test_macros.hpp>

using namespace tiledb::sm;
using namespace tiledb::sm::deletes_and_updates::serialization;

/**
 * Make sure a condition and update values are the same after going through
 * serialization/deserialization.
 *
 * @param query_condition Condition to check.
 * @param update_values Update values to check.
 */
void serialize_deserialize_check(
    QueryCondition& query_condition, std::vector<UpdateValue>& update_values) {
  auto tracker = tiledb::test::create_test_memory_tracker();
  auto serialized = serialize_update_condition_and_values(
      query_condition, update_values, tracker);
  auto&& [deserialized_condition, deserialized_update_values] =
      deserialize_update_condition_and_values(
          0, "", serialized->data(), serialized->size());

  CHECK(tiledb::test::ast_equal(
      query_condition.ast(), deserialized_condition.ast()));

  CHECK(update_values.size() == deserialized_update_values.size());

  for (uint64_t i = 0; i < update_values.size(); i++) {
    CHECK(
        update_values[i].field_name() ==
        deserialized_update_values[i].field_name());
    CHECK(
        update_values[i].view().size() ==
        deserialized_update_values[i].view().size());
    CHECK(
        0 == memcmp(
                 update_values[i].view().content(),
                 deserialized_update_values[i].view().content(),
                 update_values[i].view().size()));
  }
}

TEST_CASE(
    "UpdateCondition: Test single value", "[updatecondition][single-value]") {
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

  std::vector<UpdateValue> values;
  std::vector<uint8_t> val = {1, 2, 3};
  values.emplace_back(field_name, val.data(), val.size());
  serialize_deserialize_check(query_condition, values);
}

TEST_CASE(
    "UpdateCondition: Test multiple values",
    "[updatecondition][multiple-value]") {
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

  std::vector<UpdateValue> values;
  std::string field_name1 = "foo";
  std::vector<uint8_t> val1 = {1, 2, 3};

  std::string field_name2 = "X";
  std::vector<uint8_t> val2 = {3, 2, 1, 0};

  std::string field_name3 = "contig";
  std::string val3 = "CONTIGXYZ";

  values.emplace_back(field_name1, val1.data(), val1.size());
  values.emplace_back(field_name2, val2.data(), val2.size());
  values.emplace_back(field_name3, val3.data(), val3.length());

  serialize_deserialize_check(query_condition, values);
}
