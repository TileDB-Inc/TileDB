/*
 * @file uniform_mapping.cc
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
 * This file defines the mapping from the label to index for a dimension label
 * that is a virtual uniform or evenly-spaced dimension.
 */

#include "tiledb/sm/dimension_label/uniform_mapping.h"

namespace tiledb::sm {

shared_ptr<DimensionLabelMapping> create_uniform_mapping(
    const Datatype label_datatype,
    const Range& label_domain,
    const Datatype index_datatype,
    const Range& index_domain) {
  if (index_datatype != Datatype::UINT64)
    throw std::invalid_argument(
        "The uniform dimension label is only supported on UINT64 dimensions");
  switch (label_datatype) {
    case Datatype::INT8:
      return UniformMapping<int8_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::UINT8:
      return UniformMapping<uint8_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::INT16:
      return UniformMapping<int16_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::UINT16:
      return UniformMapping<uint16_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::INT32:
      return UniformMapping<int32_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::UINT32:
      return UniformMapping<uint32_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::INT64:
      return UniformMapping<int64_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::UINT64:
      return UniformMapping<uint64_t, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::FLOAT32:
      return UniformMapping<float, uint64_t>::create(
          label_domain, index_domain);
    case Datatype::FLOAT64:
      return UniformMapping<double, uint64_t>::create(
          label_domain, index_domain);
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
      return UniformMapping<int64_t, uint64_t>::create(
          label_domain, index_domain);
    default:
      throw std::invalid_argument(
          "The uniform dimension label does not support label datatype " +
          datatype_str(label_datatype));
  }
}

}  // namespace tiledb::sm
