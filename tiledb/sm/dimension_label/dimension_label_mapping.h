/**
 * @file dimension_label_mapping.h
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
 * This file contains classes that define the mapping from the label in a
 * dimension label to the index, or underlying dimension.
 */

#ifndef TILEDB_DIMENSION_LABEL_MAPPING_H
#define TILEDB_DIMENSION_LABEL_MAPPING_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb::sm {

/**
 * Interface for the mapping from label-to-index for TileDB dimension labels.
 */
class DimensionLabelMapping {
 public:
  virtual ~DimensionLabelMapping() = default;

  /**
   * Returns the index range that covers the same region of the domain as the
   * input label range.
   *
   * This may raise a logic error for label ranges that are out-of-bounds.
   *
   * @param label_range A range of a label coordinates.
   * @returns The output index range that covers the same region of the array as
   * in the input label.
   **/
  virtual Range index_range(const Range& labels) const = 0;
};

template <
    typename TLABEL,
    typename TINDEX,
    typename ENABLE_LABEL = TLABEL,
    typename ENABLE_INDEX = TINDEX>
class VirtualLabelMapping : public DimensionLabelMapping {
 public:
  virtual ~VirtualLabelMapping() = default;

  /**
   * Returns the index range that convers the same region of the domain as the
   * input label range.
   *
   * This will raise a logic error for label ranges that are out-of-bounds.
   *
   * @param label_range A range of a label coordinates. The label must be a
   * valid non-empty range with ordered data of the expected datatype.
   * @returns The output index range that covers the same region of the array as
   * in the input label.
   **/
  Range index_range(const Range& labels) const override {
    auto label_data = static_cast<const TLABEL*>(labels.data());
    std::array<TINDEX, 2> index_data{index_lower_bound(label_data[0]),
                                     index_upper_bound(label_data[1])};
    return Range(index_data.data(), 2 * sizeof(TINDEX));
  };

 protected:
  /**
   * Returns the index value matching the requested label.
   *
   * If the label is between indices, it will round up. This is used for the
   * lower bound of a region.
   *
   * A logic error is thrown if the label is larger than the maximum label
   * value.
   *
   * @param label Input label coordinate value.
   * @returns The cooresponding coordinate value of the original dimension.
   */
  virtual TINDEX index_lower_bound(const TLABEL label) const = 0;

  /**
   * Returns the index value matching the requested label.
   *
   * If the label is between indices, it will round down. This is used for the
   * upper bound of a region.
   *
   * A logic error is thrown if the label is smaller than the minimum label
   * value.
   *
   * @param label Input label coordinate value.
   * @returns The cooresponding coordinate value of the original dimension.
   */
  virtual TINDEX index_upper_bound(const TLABEL label) const = 0;
};

}  // namespace tiledb::sm

#endif
