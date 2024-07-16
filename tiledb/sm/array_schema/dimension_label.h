/**
 * @file tiledb/sm/array_schema/dimension_label.h
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
 * Defines the dimension label class
 */

#ifndef TILEDB_DIMENSION_LABEL_H
#define TILEDB_DIMENSION_LABEL_H

#include "tiledb/common/common.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/storage_format/serialization/serializers.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

class ArraySchema;
class Buffer;
class ConstBuffer;
class Dimension;
class MemoryTracker;
enum class Datatype : uint8_t;
enum class DataOrder : uint8_t;

/**
 * Class containing dimension label information required for usage
 * in a TileDB array schema.
 *
 * A dimension label reference can be to an external dimension label or to a
 * dimension label that is contained inside the array. For dimension labels
 * internal to the array, the dimension label schema must be set before writing.
 * By default, the dimension label schema is not loaded when the array schema is
 * loaded.
 */
class DimensionLabel {
 public:
  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   *
   * Note: This should be the same as `Domain::dimension_size_type`. We're
   * not including `domain.h`, otherwise we'd use that definition here.
   */
  using dimension_size_type = uint32_t;

  /** Default constructor is not C.41. */
  DimensionLabel() = delete;

  /**
   * Constructor for accessing an existing dimension label.
   *
   * @param dim_id The index of the dimension the label is attached to.
   * @param dim_label_name The name of the dimension label.
   * @param uri The URI of an external dimension label.
   * @param label_attr_name The name of the attribute in the array that stores
   *     the label data.
   * @param label_order The order of the dimension label.
   * @param label_type The datatype of the label data.
   * @param label_cell_val_num The number of values stored in dimension label
   *     cell.
   * @param schema The schema of the dimension label.
   * @param is_external If ``true``, the dimension label exits outside of the
   * array.
   * @param relative_uri If ``true``, the URI is relative.
   */
  DimensionLabel(
      dimension_size_type dim_id,
      const std::string& dim_label_name,
      const URI& uri,
      const std::string& label_attr_name,
      DataOrder label_order,
      Datatype label_type,
      uint32_t label_cell_val_num,
      shared_ptr<ArraySchema> schema,
      bool is_external,
      bool relative_uri);

  /**
   * Constructor for an internally generated dimension label.
   *
   * @param memory_tracker Memory tracker for the dimension label.
   * @param dim_id The index of the dimension the label is attached to.
   * @param dim_label_name The name of the dimension label.
   * @param uri The URI of an external dimension label.
   * @param dim The dimension the label is being added to.
   * @param label_order The order of the dimension label.
   * @param label_type The datatype of the label data.
   */
  DimensionLabel(
      dimension_size_type dim_id,
      const std::string& dim_label_name,
      const URI& uri,
      const Dimension* dim,
      DataOrder label_order,
      Datatype label_type,
      shared_ptr<MemoryTracker> memory_tracker);

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param version The array schema version.
   * @return DimensionLabel
   */
  static shared_ptr<DimensionLabel> deserialize(
      Deserializer& deserializer, uint32_t version);

  /** Index of the dimension the label is attached to. */
  inline dimension_size_type dimension_index() const {
    return dim_id_;
  }

  /**
   * Returns ``true`` if the dimension label is not contained inside the array.
   */
  inline bool is_external() const {
    return is_external_;
  }

  /**
   * Returns ``true`` if the label cells are variable length.
   */
  inline bool is_var() const {
    return label_cell_val_num_ == constants::var_num;
  }

  /**
   * Returns ``true`` if the dimension label schema if set and ``false``
   * otherwise.
   */
  inline bool has_schema() const {
    return schema_ != nullptr;
  }

  /** The name of the label attribute in the dimension label schema. */
  inline const std::string& label_attr_name() const {
    return label_attr_name_;
  }

  /** The number of values per label cell. */
  inline uint32_t label_cell_val_num() const {
    return label_cell_val_num_;
  }

  /** The label order of the dimension label. */
  inline DataOrder label_order() const {
    return label_order_;
  }

  /** The interval label data is valid on. */
  inline Datatype label_type() const {
    return label_type_;
  }

  /** The name of the dimension label. */
  inline const std::string& name() const {
    return dim_label_name_;
  }

  const shared_ptr<ArraySchema> schema() const;

  /**
   * Serializes the dimension label object into a buffer.
   *
   * @param serializer The object the dimension is serialized into.
   * @param version The array schema version.
   */
  void serialize(Serializer& serializer, uint32_t version) const;

  /** Returns the URI of the dimension label. */
  inline const URI& uri() const {
    return uri_;
  }

  /**
   * Returns a copy of the dimension label URI.
   *
   * If the dimension label is relative to the array URI, it will append the
   * dimension label URI to the array URI.
   *
   * @param array_uri URI of the parent array for this dimension label
   *     reference.
   * @returns URI of the dimension label.
   */
  inline URI uri(const URI& array_uri) const {
    return relative_uri_ ? array_uri.join_path(uri_) : uri_;
  }

  /** Returns ``true`` if the URI is relative to the array URI. */
  inline bool uri_is_relative() const {
    return relative_uri_;
  }

 private:
  /** The index of the dimension the labels are attached to. */
  dimension_size_type dim_id_;

  /** The name of the dimension label. */
  std::string dim_label_name_;

  /** The URI of the existing dimension label. */
  URI uri_;

  /** The name of the attribute that stores the label data. */
  std::string label_attr_name_;

  /** The label order of the dimension label. */
  DataOrder label_order_;

  /** The datatype of the label data. */
  Datatype label_type_;

  /** The number of cells per label value. */
  uint32_t label_cell_val_num_;

  /**
   * The dimension label schema.
   *
   * The schema is used for creating the dimension label and is not included in
   * the dimension label schema serialization and deserialization from disk.
   */
  shared_ptr<ArraySchema> schema_;

  /**
   * If ``true`` the dimension label exists outside the array, otherwise
   * it is stored in the array's label directory.
   */
  bool is_external_;

  /**
   * If ``true`` the URI is relative. If ``false``, it is absolute.
   *
   * If the dimension label is not external, the URI should always be relative.
   */
  bool relative_uri_;
};

}  // namespace tiledb::sm

/** Converts the filter into a string representation. */
std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::DimensionLabel& dl);

#endif
