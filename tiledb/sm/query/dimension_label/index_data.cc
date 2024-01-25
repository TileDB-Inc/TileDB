/**
 * @file index_data.cc
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
 * Class for internally managed index data for dimension labels.
 */

#include "tiledb/sm/query/dimension_label/index_data.h"
#include "tiledb/common/common.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

IndexData* IndexDataCreate::make_index_data(
    const Datatype type, const Range& input_range) {
  switch (type) {
    case Datatype::INT8:
      return tdb_new(TypedIndexData<int8_t>, input_range);
    case Datatype::UINT8:
      return tdb_new(TypedIndexData<uint8_t>, input_range);
    case Datatype::INT16:
      return tdb_new(TypedIndexData<int16_t>, input_range);
    case Datatype::UINT16:
      return tdb_new(TypedIndexData<uint16_t>, input_range);
    case Datatype::INT32:
      return tdb_new(TypedIndexData<int32_t>, input_range);
    case Datatype::UINT32:
      return tdb_new(TypedIndexData<uint32_t>, input_range);
    case Datatype::INT64:
      return tdb_new(TypedIndexData<int64_t>, input_range);
    case Datatype::UINT64:
      return tdb_new(TypedIndexData<uint64_t>, input_range);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return tdb_new(TypedIndexData<int64_t>, input_range);
    default:
      stdx::unreachable();
  }
}

IndexData* IndexDataCreate::make_index_data(
    const Datatype type,
    const uint64_t num_values,
    const bool ranges_are_points) {
  switch (type) {
    case Datatype::INT8:
      return tdb_new(TypedIndexData<int8_t>, num_values, ranges_are_points);
    case Datatype::UINT8:
      return tdb_new(TypedIndexData<uint8_t>, num_values, ranges_are_points);
    case Datatype::INT16:
      return tdb_new(TypedIndexData<int16_t>, num_values, ranges_are_points);
    case Datatype::UINT16:
      return tdb_new(TypedIndexData<uint16_t>, num_values, ranges_are_points);
    case Datatype::INT32:
      return tdb_new(TypedIndexData<int32_t>, num_values, ranges_are_points);
    case Datatype::UINT32:
      return tdb_new(TypedIndexData<uint32_t>, num_values, ranges_are_points);
    case Datatype::INT64:
      return tdb_new(TypedIndexData<int64_t>, num_values, ranges_are_points);
    case Datatype::UINT64:
      return tdb_new(TypedIndexData<uint64_t>, num_values, ranges_are_points);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return tdb_new(TypedIndexData<int64_t>, num_values, ranges_are_points);
    default:
      stdx::unreachable();
  }
}

}  // namespace tiledb::sm
