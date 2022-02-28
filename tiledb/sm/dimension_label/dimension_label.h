/**
 * @file   dimension_label.h
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
 * This file defines the DimensionLabel class.
 */

#ifndef TILEDB_DIMENSION_LABEL_H
#define TILEDB_DIMENSION_LABEL_H

#include "tiledb/common/common.h"
#include "tiledb/sm/dimension_label/dimension_label_mapping.h"
#include "tiledb/sm/dimension_label/uniform_mapping.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/dimension_label_type.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * A TileDB dimension label.
 *
 * A dimension label is an additional set of coordinates that can be used to
 * indirectly query an array in place of the standard dimension. The concept is
 * analoguous to adding labels to the axis of a plot.
 *
 * Definitions:
 *  * label: The new coordinates that are used to access data from the array.
 *  * index: A coordinate from the original dimension. The index domain,
 * index datataype, and index cell value number refer to these properties for
 * the original dimension.
 *
 * There are two categories of dimension labels: virtual dimension labels and
 * actualized dimension labels. For virtual dimension labels, the mapping from
 * the label to the index is managed by a function that can be resolved without
 * storing additional data on disk. For actualized dimension labels, the mapping
 * from the label to the index is defined by a direct label-to-index map stored
 * on-disk.
 *
 * Current label types:
 *
 * * Uniform label (virtual): A uniformly spaced grid from an numeric label type
 * to an ``TILEDB_UINT64`` dimension.
 *
 */
class DimensionLabel {
 public:
  /* No default constructor: not C.41 compliant. */
  DimensionLabel() = delete;

  /* Private constructor.
   *
   * @param label_type The dimension label type.
   * @param name Name of the dimension label.
   * @param label_datatype The TileDB datatype of the label.
   * @param label_cell_val_num The number of values per cell for the label.
   * @param label_domain The label domain. A pair of [lower, upper] bounds.
   * @param index_datatype The TileDB datatype of the original dimension.
   * @param index_cell_val_num The number of values per cell for the original
   * dimension.
   * @param index_domain The domain of the original domain. A pair of [lower,
   * upper] bounds.
   * @parame label_map Internal mapping from labels to indices.
   * */
  explicit DimensionLabel(
      LabelType label_type,
      const std::string& name,
      Datatype label_datatype,
      uint32_t label_cell_val_num,
      const Range& label_domain,
      Datatype index_datatype,
      uint32_t index_cell_val_num,
      const Range& index_domain,
      shared_ptr<DimensionLabelMapping> label_map);

  /**
   * A factory for creating an uniform (evenly-spaced) virtual dimension label.
   *
   * @param name Name of the dimension label.
   * @param label_datatype The TileDB datatype of the label.
   * @param label_cell_val_num The number of values per cell for the label.
   * @param label_domain The label domain. A pair of [lower, upper] bounds.
   * @param index_datatype The TileDB datatype of the original dimension.
   * @param index_cell_val_num The number of values per cell for the original
   * dimension.
   * @param index_domain The domain of the original domain. A pair of [lower,
   * upper] bounds.
   **/
  static tuple<Status, shared_ptr<DimensionLabel>> create_uniform(
      const std::string& name,
      Datatype label_datatype,
      uint32_t label_cell_val_num,
      const Range& label_domain,
      Datatype index_datatype,
      uint32_t index_cell_val_num,
      const Range& index_domain);

  /**
   * Returns success status and a range on the dimension coordinates that
   * matches the label range.
   *
   * This function will be used to convert from a labelled Subarray to an
   * un-labelled subarray.
   *
   * The index range that is returned from this function will map to the same
   * region of the array as the input label range. The lower bound of the
   * returned index range may round up to the nearest valid value. Similarly,
   * the upper bound may round down to the nearest valid valid.
   *
   * If the input label range is out-of-bounds of the array, the status will
   * return an error.
   *
   * @param label_range A range of a label coordinates. The label must be a
   * valid non-empty range with ordered data of the label datatype.
   * @returns {status, range} The status of the conversion and the output index
   * range that covers the same region of the array as in the input label.
   **/
  tuple<Status, Range> index_range(const Range& label_range) const;

  /** The core data required for all dimension labels. */
  struct BaseSchema {
    /** No default constructor: not C.41 compliant. */
    BaseSchema() = delete;

    /** Constructor. */
    BaseSchema(
        LabelType label_type,
        const std::string& name,
        Datatype label_datatype,
        uint32_t label_cell_val_num,
        const Range& label_domain,
        Datatype index_datatype,
        uint32_t index_cell_val_num,
        const Range& index_domain);

    /** The type of the dimension
     *
     * Possible types include:
     *
     * * LABEL_UNIFORM: A uniform (evenly space) virtual dimension label.
     **/
    LabelType label_type;

    /** The dimension label name. */
    std::string name;

    /** The TileDB datatype of the label coordinates. */
    Datatype label_datatype;

    /** The number of values in a cell for the label coordinates. */
    uint32_t label_cell_val_num;

    /** The domain of the label. A pair of [lower, upper] bounds. */
    Range label_domain;

    /** The TileDB datatype of the original dimension. */
    Datatype index_datatype;

    /** The number of values in a cell for the original dimension. */
    uint32_t index_cell_val_num;

    /**
     * The domain of the original dimension. A pair of [lower, upper] bounds.
     */
    Range index_domain;
  };

 private:
  /** Core data needed for all dimension label types. */
  BaseSchema schema_;

  /** Label-to-index map for the dimension label.*/
  shared_ptr<DimensionLabelMapping> label_index_map_{nullptr};
};

}  // namespace tiledb::sm
#endif
