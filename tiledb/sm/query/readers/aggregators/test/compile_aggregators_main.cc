/**
 * @file compile_aggregators_main.cc
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
 */

#include "../count_aggregator.h"
#include "../field_info.h"
#include "../min_max_aggregator.h"
#include "../sum_aggregator.h"

int main() {
  tiledb::sm::CountAggregator();

  tiledb::sm::MeanAggregator<uint8_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<uint16_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<uint32_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<uint64_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<int8_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<int16_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<int32_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<int64_t>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<float>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MeanAggregator<double>(tiledb::sm::FieldInfo(
      "Mean", false, false, 1, tiledb::sm::Datatype::UINT8));

  tiledb::sm::MinAggregator<uint8_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<uint16_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<uint32_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<uint64_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<int8_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<int16_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<int32_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<int64_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<float>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<double>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MinAggregator<std::string>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));

  tiledb::sm::MaxAggregator<uint8_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<uint16_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<uint32_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<uint64_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<int8_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<int16_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<int32_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<int64_t>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<float>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<double>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::MaxAggregator<std::string>(tiledb::sm::FieldInfo(
      "MinMax", false, false, 1, tiledb::sm::Datatype::UINT8));

  tiledb::sm::NullCountAggregator(tiledb::sm::FieldInfo(
      "NullCount", false, false, 1, tiledb::sm::Datatype::UINT8));

  tiledb::sm::SumAggregator<uint8_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<uint16_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<uint32_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<uint64_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<int8_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<int16_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<int32_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<int64_t>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<float>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));
  tiledb::sm::SumAggregator<double>(tiledb::sm::FieldInfo(
      "Sum", false, false, 1, tiledb::sm::Datatype::UINT8));

  return 0;
}
