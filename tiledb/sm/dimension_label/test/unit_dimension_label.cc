/**
 * @file tiledb/sm/array_schema/unit_dimension_label.cc
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
 * This file tests the DimensionLabel class
 */

#include <array>
#include <catch.hpp>
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <typename T>
void require_range_is_equal(const Range& result, const T start, const T end) {
  if (result.empty()) {
    REQUIRE(false);
    return;
  }
  auto result_data = static_cast<const T*>(result.data());
  CHECK(start == result_data[0]);
  REQUIRE(end == result_data[1]);
}

template <typename T>
Range create_range(const T start, const T stop) {
  std::array<T, 2> array{start, stop};
  return {array.data(), 2 * sizeof(T)};
}

TEMPLATE_TEST_CASE_SIG(
    "DimensionLabel for type LABEL_UNIFORM on domain [10, 70] -> [0, 5]",
    "[dimension_label][uniform_label]",
    ((typename TLABEL, Datatype DLABEL), TLABEL, DLABEL),
    (int8_t, Datatype::INT8),
    (uint8_t, Datatype::UINT8),
    (int16_t, Datatype::INT16),
    (uint16_t, Datatype::UINT16),
    (int32_t, Datatype::INT32),
    (uint32_t, Datatype::UINT32),
    (int64_t, Datatype::INT64),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (int64_t, Datatype::DATETIME_MONTH),
    (int64_t, Datatype::DATETIME_WEEK),
    (int64_t, Datatype::DATETIME_DAY),
    (int64_t, Datatype::DATETIME_HR),
    (int64_t, Datatype::DATETIME_MIN),
    (int64_t, Datatype::DATETIME_SEC),
    (int64_t, Datatype::DATETIME_MS),
    (int64_t, Datatype::DATETIME_US),
    (int64_t, Datatype::DATETIME_NS),
    (int64_t, Datatype::DATETIME_PS),
    (int64_t, Datatype::DATETIME_FS),
    (int64_t, Datatype::DATETIME_AS),
    (int64_t, Datatype::TIME_HR),
    (int64_t, Datatype::TIME_MIN),
    (int64_t, Datatype::TIME_SEC),
    (int64_t, Datatype::TIME_MS),
    (int64_t, Datatype::TIME_US),
    (int64_t, Datatype::TIME_NS),
    (int64_t, Datatype::TIME_PS),
    (int64_t, Datatype::TIME_FS),
    (int64_t, Datatype::TIME_AS),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  const uint64_t n_min{0};
  const uint64_t n_max{5};
  const TLABEL x_min{10};
  const TLABEL x_max{60};
  const auto index_domain = create_range<uint64_t>(n_min, n_max);
  const auto label_domain = create_range<TLABEL>(x_min, x_max);
  auto&& [status, dim_label] =
      DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
          "label", DLABEL, 1, label_domain, Datatype::UINT64, 1, index_domain));
  REQUIRE(status.ok());
  REQUIRE(dim_label != nullptr);
  SECTION("Convert full data range") {
    auto&& [status, result] = dim_label->index_range(label_domain);
    INFO(status.to_string());
    REQUIRE(status.ok());
    require_range_is_equal<uint64_t>(result, 0, 5);
  }
  SECTION("Convert [1, 30] -> [0, 2]") {
    const auto label_range = create_range<TLABEL>(1, 30);
    auto&& [status, result] = dim_label->index_range(label_range);
    INFO(status.to_string());
    REQUIRE(status.ok());
    require_range_is_equal<uint64_t>(result, 0, 2);
  }
  SECTION("Convert [40, 40] -> [3, 3]") {
    const auto label_range = create_range<TLABEL>(40, 40);
    auto&& [status, result] = dim_label->index_range(label_range);
    INFO(status.to_string());
    REQUIRE(status.ok());
    require_range_is_equal<uint64_t>(result, 3, 3);
  }
  SECTION("Convert [45, 80] -> [4, 5]") {
    const auto label_range = create_range<TLABEL>(45, 80);
    auto&& [status, result] = dim_label->index_range(label_range);
    INFO(status.to_string());
    REQUIRE(status.ok());
    require_range_is_equal<uint64_t>(result, 4, 5);
  }
  SECTION("Exception [0, 5] is out of bounds") {
    const auto label_range = create_range<TLABEL>(0, 5);
    auto&& [status, result] = dim_label->index_range(label_range);
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }
  SECTION("Exception [70, 75] is out of bounds") {
    const auto label_range = create_range<TLABEL>(70, 75);
    auto&& [status, result] = dim_label->index_range(label_range);
    INFO(status.to_string());
    REQUIRE(!status.ok());
  }
}

TEST_CASE(
    "DimensionLabel::create_uniform - verify status not ok for "
    "cell value number not equal to 1",
    "[dimension_label][uniform_label]") {
  uint64_t index_domain_data[2] = {0, 10};
  Range index_domain{index_domain_data, 2 * sizeof(uint64_t)};
  float label_domain_data[2] = {-1.0, 1.0};
  Range label_domain{label_domain_data, 2 * sizeof(float)};
  SECTION("label_cell_val_num!=1") {
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label",
            Datatype::FLOAT32,
            2,
            label_domain,
            Datatype::UINT64,
            1,
            index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
  SECTION("index_cell_val_num!=1") {
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label",
            Datatype::FLOAT32,
            1,
            label_domain,
            Datatype::UINT64,
            2,
            index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "DimensionLabel::create_uniform - verify status not ok for "
    "invalid label domain for floating-point label",
    "[dimension_label][uniform_label]",
    ((typename TLABEL, Datatype DLABEL), TLABEL, DLABEL),
    (float, Datatype::FLOAT32),
    (double, Datatype::FLOAT64)) {
  uint64_t index_domain_data[2] = {0, 10};
  Range index_domain{index_domain_data, 2 * sizeof(uint64_t)};
  SECTION("empty label domain") {
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label", DLABEL, 1, Range(), Datatype::UINT64, 1, index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
  SECTION("label domain with nan") {
    TLABEL label_domain_data[2] = {-std::numeric_limits<TLABEL>::quiet_NaN(),
                                   std::numeric_limits<TLABEL>::quiet_NaN()};
    Range label_domain{label_domain_data, 2 * sizeof(TLABEL)};
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label",
            DLABEL,
            1,
            label_domain,
            Datatype::UINT64,
            1,
            index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
  SECTION("label domain with inf") {
    TLABEL label_domain_data[2] = {-std::numeric_limits<TLABEL>::infinity(),
                                   std::numeric_limits<TLABEL>::infinity()};
    Range label_domain{label_domain_data, 2 * sizeof(TLABEL)};
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label",
            DLABEL,
            1,
            label_domain,
            Datatype::UINT64,
            1,
            index_domain));
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
  SECTION("lower bound greater than upper bound") {
    TLABEL label_domain_data[2] = {1.0, -1.0};
    Range label_domain{label_domain_data, 2 * sizeof(TLABEL)};
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label",
            DLABEL,
            1,
            label_domain,
            Datatype::UINT64,
            1,
            index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
}

TEMPLATE_TEST_CASE_SIG(
    "DimensionLabel::create_uniform - verify status not ok for "
    "invalid label domain for integer labels",
    "[dimension_label][uniform_label]",
    ((typename TLABEL, Datatype DLABEL), TLABEL, DLABEL),
    (int8_t, Datatype::INT8),
    (uint8_t, Datatype::UINT8),
    (int16_t, Datatype::INT16),
    (uint16_t, Datatype::UINT16),
    (int32_t, Datatype::INT32),
    (uint32_t, Datatype::UINT32),
    (int64_t, Datatype::INT64),
    (uint64_t, Datatype::UINT64),
    (int64_t, Datatype::DATETIME_YEAR),
    (int64_t, Datatype::DATETIME_MONTH),
    (int64_t, Datatype::DATETIME_WEEK),
    (int64_t, Datatype::DATETIME_DAY),
    (int64_t, Datatype::DATETIME_HR),
    (int64_t, Datatype::DATETIME_MIN),
    (int64_t, Datatype::DATETIME_SEC),
    (int64_t, Datatype::DATETIME_MS),
    (int64_t, Datatype::DATETIME_US),
    (int64_t, Datatype::DATETIME_NS),
    (int64_t, Datatype::DATETIME_PS),
    (int64_t, Datatype::DATETIME_FS),
    (int64_t, Datatype::DATETIME_AS),
    (int64_t, Datatype::TIME_HR),
    (int64_t, Datatype::TIME_MIN),
    (int64_t, Datatype::TIME_SEC),
    (int64_t, Datatype::TIME_MS),
    (int64_t, Datatype::TIME_US),
    (int64_t, Datatype::TIME_NS),
    (int64_t, Datatype::TIME_PS),
    (int64_t, Datatype::TIME_FS),
    (int64_t, Datatype::TIME_AS)) {
  const auto index_domain = create_range<uint64_t>(0, 10);
  SECTION("empty label domain") {
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label", DLABEL, 1, Range(), Datatype::UINT64, 1, index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
  SECTION("lower bound greater than upper bound") {
    const auto label_domain = create_range<TLABEL>(10, 0);
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label",
            DLABEL,
            1,
            label_domain,
            Datatype::UINT64,
            1,
            index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
  SECTION("bad label alignment") {
    const auto label_domain = create_range<TLABEL>(0, 12);
    auto&& [status, dim_label] =
        DimensionLabel::create_uniform(DimensionLabel::BaseSchema(
            "label",
            DLABEL,
            1,
            label_domain,
            Datatype::UINT64,
            1,
            index_domain));
    INFO(status.to_string());
    CHECK(!status.ok());
    REQUIRE(dim_label == nullptr);
  }
}
