/**
 * @file tiledb/sm/array_schema/dimension_label_schema.h
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
 * Defines the array schema class
 */

#ifndef TILEDB_DIMENSION_LABEL_SCHEMA_H
#define TILEDB_DIMENSION_LABEL_SCHEMA_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb::type {
class Range;
}

using namespace tiledb::type;

namespace tiledb::sm {

class ArraySchema;
class Attribute;
class Dimension;
class ByteVecValue;
class FilterPipeline;

/**
 * Return a Status_DimensionLabelSchema error class Status with a given message
 **/
inline Status Status_DimensionLabelSchemaError(const std::string& msg) {
  return {"[TileDB::DimensionLabelSchema] Error", msg};
}

/**
 * Schema for an dimension label. An dimension label consists of two
 * one-dimensional arrays used to define a dimension label.
 */
class DimensionLabelSchema {
 public:
  /**
   * Size type for the number of labels in an dimension label and for label
   * indices.
   *
   * This must be the same as ArraySchema::attribute_size_type
   */
  using attribute_size_type = unsigned int;

  /** Default constructor is not C.41 compliant. */
  DimensionLabelSchema() = delete;

  /**
   * Constructor.
   *
   * @param label_type The label type.
   * @param index_type The datatype for the original dimension data. Must be the
   * same as the dimension the dimension label is applied to.
   * @param index_domain The range the original dimension is defined on. Must be
   * the same as the dimension the dimension label is applied to.
   * @param index_tile_extent The tile extent for the original dimension data on
   * the dimension label.
   * @param label_type The datatype for the new label dimension data.
   * @param label_dim_domain The range the label data is defined on domain.
   * @param label_tile_extent The tile extent for the label data.
   */
  DimensionLabelSchema(
      LabelOrder label_order,
      Datatype index_type,
      const void* index_domain,
      const void* index_tile_extent,
      Datatype label_type,
      const void* label_domain,
      const void* label_tile_extent);

  /**
   * Constructor.
   *
   * @param label_order Order of the labels relative to the index for the
   * dimension label.
   * @param indexed_array_schema Array schema for the array with indices defined
   * on the dimension.
   * @param labelled_array_schema Array schema for the array with labels defined
   * on the dimension.
   */
  DimensionLabelSchema(
      LabelOrder label_order,
      shared_ptr<ArraySchema> indexed_array_schema,
      shared_ptr<ArraySchema> labelled_array_schema);

  /**
   * Constructor.
   *
   * Performs a deep copy of an existing dimension label.
   *
   * @param dim_label The dimension label schema to clone.
   */
  explicit DimensionLabelSchema(const DimensionLabelSchema* dim_label);

  /** Returns the index attribute from the labelled array. */
  const Attribute* index_attribute() const;

  /** Returns the index dimension from the indexed array. */
  const Dimension* index_dimension() const;

  /** Returns the indexed array schema. */
  inline const shared_ptr<ArraySchema> indexed_array_schema() const {
    return indexed_array_schema_;
  }

  /**
   * Checks if this dimension label is compatible as a dimension label for a
   * given dimension.
   *
   * @param dim Dimension to check compatibility against
   * @returns If the dimension label is compatible as a dimension label
   */
  bool is_compatible_label(const Dimension* dim) const;

  /** Returns the label attribute from the indexed array. */
  const Attribute* label_attribute() const;

  /** Returns the label dimension from the labelled array. */
  const Dimension* label_dimension() const;

  /** Returns the label order type of this dimension label. */
  inline LabelOrder label_order() const {
    return label_order_;
  }

  /** Returns the indexed array schema. */
  inline const shared_ptr<ArraySchema> labelled_array_schema() const {
    return labelled_array_schema_;
  }

 private:
  /** Order of the labels relative to the indices. */
  LabelOrder label_order_;

  /** Schema for the array with indices defined on the dimension. */
  shared_ptr<ArraySchema> indexed_array_schema_;

  /** Schema for the array with labels defined on the dimension. */
  shared_ptr<ArraySchema> labelled_array_schema_;
};

}  // namespace tiledb::sm

#endif
