/**
 * @file   array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class ArraySchema.
 */

#ifndef TILEDB_ARRAY_METADATA_H
#define TILEDB_ARRAY_METADATA_H

#include <string>
#include <typeinfo>
#include <vector>

#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/uri.h"

namespace tiledb {
namespace sm {

/** Specifies the array schema. */
class ArraySchema {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ArraySchema();

  /** Constructor. */
  ArraySchema(ArrayType array_type);

  /**
   * Constructor. Clones the input.
   *
   * @param array_schema The array schema to copy.
   */
  explicit ArraySchema(const ArraySchema* array_schema);

  /** Destructor. */
  ~ArraySchema();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /** Returns the array type. */
  ArrayType array_type() const;

  /** Returns the array uri. */
  const URI& array_uri() const;

  /** Returns a constant pointer to the selected attribute (NULL if error). */
  const Attribute* attribute(unsigned int id) const;

  /** Returns a constant pointer to the selected attribute (NULL if error). */
  const Attribute* attribute(std::string name) const;

  /** Returns the name of the attribute with the input id. */
  const std::string& attribute_name(unsigned id) const;

  /**
   * Returns the id of the input attribute.
   *
   * @param attribute The attribute name whose id will be retrieved.
   * @param The attribute id if the input attribute exists.
   * @return Status
   *
   */
  Status attribute_id(const std::string& attribute, unsigned int* id) const;

  /** Returns the attribute names. */
  std::vector<std::string> attribute_names() const;

  /** Returns the attribute types. */
  std::vector<Datatype> attribute_types() const;

  /** Returns the number of attributes. */
  unsigned int attribute_num() const;

  /** Returns the attributes. */
  const std::vector<Attribute*>& attributes() const;

  /**
   * Retrieves the number of buffers that correspond to the input attributes.
   * The function counts one buffer per fixed-sized attribute and two buffers
   * per variable-sized attribute.
   *
   * @param attributes The input attributes.
   * @param attribute_num The number of attributes.
   * @param buffer_num The number of buffers to be retrieved.
   * @return Status
   */
  Status buffer_num(
      const char** attributes,
      unsigned int attribute_num,
      unsigned int* buffer_num) const;

  /**
   * Retrieves the number of buffers that correspond to the input attribute
   * ids.The function counts one buffer per fixed-sized attribute and two
   * buffersper variable-sized attribute.
   *
   * @param attribute_ids The input attribute ids.
   * @param buffer_num The number of buffers to be retrieved.
   * @return Status
   */
  Status buffer_num(
      const std::vector<unsigned>& attribute_ids,
      unsigned int* buffer_num) const;

  /** Returns the capacity. */
  uint64_t capacity() const;

  /** Returns the cell order. */
  Layout cell_order() const;

  /** Returns the size of cell on the input attribute. */
  uint64_t cell_size(unsigned int attribute_id) const;

  /** Returns the number of values per cell of the input attribute. */
  unsigned int cell_val_num(unsigned int attribute_id) const;

  /** Returns the number of values per cell for all attributes. */
  std::vector<unsigned int> cell_val_nums() const;

  /** Returns the compression type used for offsets of variable-sized cells. */
  Compressor cell_var_offsets_compression() const;

  /** Returns the compression level used for offsets of variable-sized cells. */
  int cell_var_offsets_compression_level() const;

  /**
   * Checks the correctness of the array schema.
   *
   * @return Status
   */
  Status check() const;

  /** Returns the compression type of the attribute with the input id. */
  Compressor compression(unsigned int attribute_id) const;

  /** Return the compression level of the attribute with the input id. */
  int compression_level(unsigned int attribute_id) const;

  /** Returns the compressor of the coordinates. */
  Compressor coords_compression() const;

  /** Returns the compression level of the coordinates. */
  int coords_compression_level() const;

  /** Returns the coordinates size. */
  uint64_t coords_size() const;

  /** Returns the type of the coordinates. */
  Datatype coords_type() const;

  /** True if the array is dense. */
  bool dense() const;

  /** Returns the i-th dimension. */
  const Dimension* dimension(unsigned int i) const;

  /** Returns the number of dimensions. */
  unsigned int dim_num() const;

  /** Dumps the array schema in ASCII format in the selected output. */
  void dump(FILE* out) const;

  /**
   * Gets the ids of the input attributes.
   *
   * @param attributes The name of the attributes whose ids will be retrieved.
   * @param attribute_ids The ids that are retrieved by the function.
   * @return Status
   */
  Status get_attribute_ids(
      const std::vector<std::string>& attributes,
      std::vector<unsigned int>& attribute_ids) const;

  /** Checks if the array is defined as a key-value store. */
  bool is_kv() const;

  /**
   * Serializes the array schema object into a buffer.
   *
   * @param buff The buffer the array schema is serialized into.
   * @return Status
   */
  Status serialize(Buffer* buff) const;

  /** Returns the tile order. */
  Layout tile_order() const;

  /** Returns the type of the i-th attribute, or NULL if 'i' is invalid. */
  Datatype type(unsigned int i) const;

  /** Returns *true* if the indicated attribute has variable-sized values. */
  bool var_size(unsigned int attribute_id) const;

  /** Adds an attribute, copying the input. */
  Status add_attribute(const Attribute* attr);

  /**
   * It assigns values to the members of the object from the input buffer.
   *
   * @param buff The binary representation of the object to read from.
   * @param is_kv `true` if the array schema to be deserialized is a key-value
   *     store. Note that a key-value store is indicated by a special file
   *     stored in the array directory. The storage manager can know this
   *     and, as such, pass `is_kv` to `deserialize` during the call.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff, bool is_kv);

  /** Returns the array domain. */
  const Domain* domain() const;

  /**
   * Initializes the ArraySchema object. It also performs a check to see if
   * all the member attributes have been properly set.
   *
   * @return Status
   */
  Status init();

  /** Defines the array as a key-value store. */
  Status set_as_kv();

  /** Sets an array URI. */
  void set_array_uri(const URI& array_uri);

  /**
   * Sets the array type. The function returns an error if the array has been
   * defined as a key-value store (which by default is always sparse).
   */
  Status set_array_type(ArrayType array_type);

  /** Sets the variable cell offsets compressor. */
  void set_cell_var_offsets_compressor(Compressor compressor);

  /** Sets the variable cell offsets compression level. */
  void set_cell_var_offsets_compression_level(int compression_level);

  /** Sets the coordinates compressor. */
  void set_coords_compressor(Compressor compressor);

  /** Sets the coordinates compression level. */
  void set_coords_compression_level(int compression_level);

  /** Sets the tile capacity. */
  void set_capacity(uint64_t capacity);

  /**
   * Sets the cell order.
   *
   * @note For 1D arrays (vectors), the cell order will always be **row-major**,
   *     (col-major is equivalent).
   */
  void set_cell_order(Layout cell_order);

  /**
   * Sets the domain. The function returns an error if the array has been
   * previously set to be a key-value store.
   */
  Status set_domain(Domain* domain);

  /**
   * Sets the tile order.
   *
   * @note For 1D arrays (vectors), the cell order will always be **row-major**,
   *     (col-major is equivalent).
   */
  void set_tile_order(Layout tile_order);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** An array name attached to the schema object. */
  URI array_uri_;

  /** The array type. */
  ArrayType array_type_;

  /** The number of attributes. */
  unsigned int attribute_num_;

  /** The array attributes. */
  std::vector<Attribute*> attributes_;
  /**
   * The tile capacity for the case of sparse fragments.
   */
  uint64_t capacity_;

  /**
   * The cell order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   */
  Layout cell_order_;

  /** Stores the size of every attribute (plus coordinates in the end). */
  std::vector<uint64_t> cell_sizes_;

  /** The compression type used for offsets of variable-sized cells. */
  Compressor cell_var_offsets_compression_;

  /** The compression level used for offsets of variable-sized cells. */
  int cell_var_offsets_compression_level_;

  /** The coordinates compression type. */
  Compressor coords_compression_;

  /** The coordinates compression level. */
  int coords_compression_level_;

  /** The size (in bytes) of the coordinates. */
  uint64_t coords_size_;

  /** The array domain. */
  Domain* domain_;

  /** `true` if the array is a key-value store. */
  bool is_kv_;

  /**
   * The tile order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   */
  Layout tile_order_;

  /** The version under which this object was created. */
  int version_[3];

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Returns false if the union of attribute and dimension names contain
   * duplicates.
   */
  bool check_attribute_dimension_names() const;

  /**
   * Returns false if double delta compression is used with real attributes
   * or coordinates and true otherwise.
   */
  bool check_double_delta_compressor() const;

  /** Clears all members. Use with caution! */
  void clear();

  /** Computes and returns the size of an attribute (or coordinates). */
  uint64_t compute_cell_size(unsigned int attribute_id) const;

  /** Sets the special key-value attributes. */
  Status set_kv_attributes();

  /** Sets the special key-value domain. */
  Status set_kv_domain();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_METADATA_H
