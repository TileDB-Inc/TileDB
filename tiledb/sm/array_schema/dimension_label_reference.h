/**
 * @file tiledb/sm/array_schema/dimension_label_reference.h
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
 * Defines the dimension label reference class
 */

#ifndef TILEDB_DIMENSION_LABEL_REFERENCE_H
#define TILEDB_DIMENSION_LABEL_REFERENCE_H

#include "tiledb/common/common.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/storage_format/serialization/serializers.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::sm {

class Buffer;
class ConstBuffer;
class DimensionLabelSchema;
enum class Datatype : uint8_t;
enum class LabelOrder : uint8_t;

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
class DimensionLabelReference {
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
  DimensionLabelReference() = delete;

  /**
   * Constructor for accessing an existing dimension label.
   *
   * @param dim_id The index of the dimension the label is attached to.
   * @param name The name of the dimension label.
   * @param uri The URI of an external dimension label.
   * @param label_order The order of the dimension label.
   * @param label_type The datatype of the label data.
   * @param cell_val_num The number of values stored in dimension label cell.
   * @param domain The domain the dimension label is defined on.
   * @param is_external If ``true``, the dimension label exits outside of the
   * array.
   * @param relative_uri If ``true``, the URI is relative.
   */
  DimensionLabelReference(
      dimension_size_type dim_id,
      const std::string& name,
      const URI& uri,
      LabelOrder label_order,
      Datatype label_type,
      uint32_t label_cell_val_num,
      const Range& label_domain,
      shared_ptr<const DimensionLabelSchema> schema,
      bool is_external,
      bool relative_uri);

  /**
   * Constructor for an internally generated dimension label.
   *
   * @param dim_id The index of the dimension the label is attached to.
   * @param name The name of the dimension label.
   * @param uri The URI of an external dimension label.
   * @param label_order The order of the dimension label.
   * @param label_type The datatype of the label data.
   * @param label_cell_val_num The number of values stored in dimension cell.
   * @param label_domain The domain the dimension label is defined on.
   * @param schema The array schema
   */
  DimensionLabelReference(
      dimension_size_type dim_id,
      const std::string& name,
      const URI& uri,
      LabelOrder label_order,
      Datatype label_type,
      uint32_t label_cell_val_num,
      const Range& label_domain,
      shared_ptr<const DimensionLabelSchema> schema);

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param version The array schema version.
   * @return DimensionLabelReference
   */
  static shared_ptr<DimensionLabelReference> deserialize(
      Deserializer& deserializer, uint32_t version);

  /** Index of the dimension the label is attached to. */
  inline dimension_size_type dimension_id() const {
    return dim_id_;
  }

  /**
   * Dumps the dimension label contents in ASCII form in the selected output.
   */
  void dump(FILE* out) const;

  /**
   * Returns ``true`` if the dimension label is not contained inside the array.
   */
  inline bool is_external() const {
    return is_external_;
  }

  /**
   * Returns ``true`` is the label cells are variable length.
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

  /** The number of values per label cell. */
  inline uint32_t label_cell_val_num() const {
    return label_cell_val_num_;
  }

  /** The interval label data is valid on. */
  inline const Range& label_domain() const {
    return label_domain_;
  }

  /** The label order of the dimension label. */
  inline LabelOrder label_order() const {
    return label_order_;
  }

  /** The interval label data is valid on. */
  inline Datatype label_type() const {
    return label_type_;
  }

  /** The name of the dimension label. */
  inline const std::string& name() const {
    return name_;
  }

  /** The schema of the dimension label. */
  inline const DimensionLabelSchema& schema() const {
    if (!schema_)
      throw StatusException(
          "DimensionLabelReference",
          "Cannot return dimension label schema; No schema is set.");
    return *schema_;
  }

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

 private:
  /** The index of the dimension the labels are attached to. */
  dimension_size_type dim_id_;

  /** The name of the dimension label. */
  std::string name_;

  /** The URI of the existing dimension label. */
  URI uri_;

  /** The label order of the dimension label. */
  LabelOrder label_order_;

  /** The datatype of the label data. */
  Datatype label_type_;

  /** The number of cells per label value. */
  uint32_t label_cell_val_num_;

  /** The interval the labels are defined on. */
  Range label_domain_;

  /** The dimension label schema */
  shared_ptr<const DimensionLabelSchema> schema_;

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

#endif
