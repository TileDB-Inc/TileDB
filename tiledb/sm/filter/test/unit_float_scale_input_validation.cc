/**
 * @file tiledb/sm/filter/test/unit_float_scale_input_validation.cc
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
 * This file tests the float scaling filter input validation.
 *
 */
#include <cmath>
#include <iostream>

#include <catch2/catch_test_macros.hpp>
#include "../../../common/common.h"
#include "../../enums/filter_option.h"
#include "../float_scaling_filter.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb::sm;

double nan_example = nan("");
double inf_example = std::numeric_limits<double>::infinity();
double zero_example = 0.0;
double normal_example = 100.0f;
double subnormal_example = std::numeric_limits<double>::denorm_min();

void check_input_values() {
  REQUIRE(std::fpclassify(nan_example) == FP_NAN);
  REQUIRE(std::fpclassify(inf_example) == FP_INFINITE);
  REQUIRE(std::fpclassify(zero_example) == FP_ZERO);
  REQUIRE(std::fpclassify(normal_example) == FP_NORMAL);
  REQUIRE(std::fpclassify(subnormal_example) == FP_SUBNORMAL);
}

void check_throw_message(
    FilterOption option, double* value, const std::string& message) {
  FloatScalingFilter filter(Datatype::FLOAT32);
  Status status = filter.set_option_impl(option, value);
  CHECK(!status.ok());
  CHECK(status.message() == message);
}

void check_ok(FilterOption option, double* value) {
  FloatScalingFilter filter(Datatype::FLOAT32);
  Status status = filter.set_option_impl(option, value);
  CHECK(status.ok());
}

TEST_CASE(
    "Float scaling filter input validation, scale",
    "[filter][float-scaling][scale-validation]") {
  check_input_values();
  const std::string& err_msg =
      "Float scaling filter error; invalid scale value.";
  check_throw_message(FilterOption::SCALE_FLOAT_FACTOR, &nan_example, err_msg);
  check_throw_message(FilterOption::SCALE_FLOAT_FACTOR, &inf_example, err_msg);
  check_throw_message(FilterOption::SCALE_FLOAT_FACTOR, &zero_example, err_msg);
  check_throw_message(
      FilterOption::SCALE_FLOAT_FACTOR, &subnormal_example, err_msg);
  check_ok(FilterOption::SCALE_FLOAT_FACTOR, &normal_example);
}

TEST_CASE(
    "Float scaling filter input validation, offset",
    "[filter][float-scaling][offset-validation]") {
  check_input_values();
  const std::string& err_msg =
      "Float scaling filter error; invalid offset value.";
  check_throw_message(FilterOption::SCALE_FLOAT_OFFSET, &nan_example, err_msg);
  check_throw_message(FilterOption::SCALE_FLOAT_OFFSET, &inf_example, err_msg);
  check_ok(FilterOption::SCALE_FLOAT_OFFSET, &zero_example);
  check_ok(FilterOption::SCALE_FLOAT_OFFSET, &subnormal_example);
  check_ok(FilterOption::SCALE_FLOAT_OFFSET, &normal_example);
}
