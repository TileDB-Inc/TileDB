/**
 * @file unit_aggregation_policies.cc
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
 * Tests the aggregation policy classes.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/query/readers/aggregators/min_max.h"
#include "tiledb/sm/query/readers/aggregators/no_op.h"
#include "tiledb/sm/query/readers/aggregators/safe_sum.h"
#include "tiledb/sm/query/readers/aggregators/sum_type.h"
#include "tiledb/sm/query/readers/aggregators/validity_policies.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers.hpp>

using namespace tiledb::sm;

typedef tuple<
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double,
    std::string_view>
    TypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Aggregate policies: no op",
    "[aggregate-policies][no-op]",
    TypesUnderTest) {
  typedef TestType T;
  auto op = NoOp();
  op.op<T>(T(), 0, 0);
}

typedef tuple<
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t>
    FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "Aggregate policies: safe sum",
    "[aggregate-policies][safe-sum]",
    FixedTypesUnderTest) {
  typedef TestType T;
  typedef typename sum_type_data<T>::sum_type SUM_T;

  SUM_T sum;
  std::atomic<SUM_T> sum_atomic;
  T val;
  auto op = SafeSum();

  // Regular sum.
  sum = 10;
  sum_atomic = sum;
  val = 10;
  op.op<SUM_T>(val, sum, 0);
  op.safe_sum<SUM_T>(val, sum_atomic);
  CHECK(sum == 20);
  CHECK(sum_atomic == 20);

  // Overflow.
  sum = std::numeric_limits<SUM_T>::max();
  sum_atomic = sum;
  val = 1;
  CHECK_THROWS_WITH(op.op<SUM_T>(val, sum, 0), "overflow on sum");
  CHECK_THROWS_WITH(op.safe_sum<SUM_T>(val, sum_atomic), "overflow on sum");
  CHECK(sum == std::numeric_limits<SUM_T>::max());
  CHECK(sum_atomic == sum);

  // Underflow
  if constexpr (std::is_signed<SUM_T>::value) {
    sum = std::numeric_limits<SUM_T>::lowest();
    sum_atomic = sum;
    val = -1;
    CHECK_THROWS_WITH(op.op<SUM_T>(val, sum, 0), "overflow on sum");
    CHECK_THROWS_WITH(op.safe_sum<SUM_T>(val, sum_atomic), "overflow on sum");
    CHECK(sum == std::numeric_limits<SUM_T>::lowest());
    CHECK(sum_atomic == sum);
  }
}

template <typename T>
T value(uint64_t v) {
  return v;
}

const std::string values = "0123456789";
template <>
std::string_view value<std::string_view>(uint64_t v) {
  return std::string_view(values.data() + v, 1);
}

template <typename T, typename Op>
void min_max_test() {
  auto op = MinMax<Op>();

  T val;
  T minmax;
  Op operation;
  bool res;
  for (uint64_t left = 0; left < 10; left++) {
    for (uint64_t right = 0; right < 10; right++) {
      // First value always replaces the min/max.
      val = value<T>(left);
      minmax = value<T>(right);
      op.template op<T>(val, minmax, 0);
      CHECK(minmax == val);

      val = value<T>(right);
      minmax = value<T>(left);
      op.template op<T>(val, minmax, 0);
      CHECK(minmax == val);

      // Other value replaces the min/max depending on Op.
      for (uint64_t c = 1; c < 10; c++) {
        val = value<T>(left);
        minmax = value<T>(right);
        res = operation(val, minmax);
        op.template op<T>(val, minmax, 1);
        CHECK(minmax == (res ? val : minmax));

        val = value<T>(right);
        minmax = value<T>(left);
        op.template op<T>(val, minmax, 1);
        CHECK(minmax == (res ? minmax : val));
      }
    }
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Aggregate policies: min max",
    "[aggregate-policies][min-max]",
    TypesUnderTest) {
  typedef TestType T;
  min_max_test<T, std::less<T>>();
  min_max_test<T, std::greater<T>>();
}

TEST_CASE("Aggregate policies: no op", "[aggregate-policies][validities]") {
  auto n = Null();
  auto nn = NonNull();
  CHECK(n.op(0));
  CHECK(!n.op(1));
  CHECK(!nn.op(0));
  CHECK(nn.op(1));
}
