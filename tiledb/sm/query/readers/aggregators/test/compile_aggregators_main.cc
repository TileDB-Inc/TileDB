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
#include "../sum_aggregator.h"
#include "tiledb/sm/array_schema/array_schema.h"

int main() {
  tiledb::sm::CountAggregator("");

  tiledb::sm::ArraySchema schema;
  tiledb::sm::SumAggregator<uint8_t>("Sum", schema);
  tiledb::sm::SumAggregator<uint16_t>("Sum", schema);
  tiledb::sm::SumAggregator<uint32_t>("Sum", schema);
  tiledb::sm::SumAggregator<uint64_t>("Sum", schema);
  tiledb::sm::SumAggregator<int8_t>("Sum", schema);
  tiledb::sm::SumAggregator<int16_t>("Sum", schema);
  tiledb::sm::SumAggregator<int32_t>("Sum", schema);
  tiledb::sm::SumAggregator<int64_t>("Sum", schema);
  tiledb::sm::SumAggregator<float>("Sum", schema);
  tiledb::sm::SumAggregator<double>("Sum", schema);
  return 0;
}