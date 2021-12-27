/**
 * @file   array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#ifndef TILEDB_ARRAY_SCHEMA_H
#define TILEDB_ARRAY_SCHEMA_H

#include <unordered_map>

#include "tiledb/common/status.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/misc/uuid.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Attribute;
class Buffer;
class ConstBuffer;
class Dimension;
class Domain;

enum class ArrayType : uint8_t;
enum class Compressor : uint8_t;
enum class Datatype : uint8_t;
enum class Layout : uint8_t;

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

  /**
   * Returns true if the array allows coordinate duplicates. Applicable
   * only to sparse arrays, dense arrays do not allow duplicates.
   */
  bool allows_dups() const;

  /** Returns the array type. */
  ArrayType array_type() const;

  /** Returns the array uri. */
  const URI& array_uri() const;

  /**
   * Returns a constant pointer to the selected attribute (nullptr if it
   * does not exist).
   */
  const Attribute* attribute(unsigned int id) const;

  /**
   * Returns a constant pointer to the selected attribute (nullptr if it
   * does not exist).
   */
  const Attribute* attribute(const std::string& name) const;

  /** Returns the number of attributes. */
  unsigned int attribute_num() const;

  /** Returns the attributes. */
  const std::vector<Attribute*>& attributes() const;

  /** Returns the capacity. */
  uint64_t capacity() const;

  /** Returns the cell order. */
  Layout cell_order() const;

  /** Returns the size of cell on the input attribute/dimension. */
  uint64_t cell_size(const std::string& name) const;

  /** Returns the number of values per cell of the input attribute/dimension. */
  unsigned int cell_val_num(const std::string& name) const;

  /** Return the filter pipeline used for offsets of variable-sized cells. */
  const FilterPipeline& cell_var_offsets_filters() const;

  /** Return the filter pipeline used for validity cells. */
  const FilterPipeline& cell_validity_filters() const;

  /**
   * Checks the correctness of the array schema.
   *
   * @return Status
   */
  Status check() const;

  /**
   * Throws an error if there is an attribute in the input that does not
   * exist in the schema.
   *
   * @param attributes The attributes to be checked.
   * @return Status
   */
  Status check_attributes(const std::vector<std::string>& attributes) const;

  /**
   * Return the filter pipeline for the given attribute/dimension (can be
   * TILEDB_COORDS).
   */
  const FilterPipeline& filters(const std::string& name) const;

  /** Return the pipeline used for coordinates. */
  const FilterPipeline& coords_filters() const;

  /** True if the array is dense. */
  bool dense() const;

  /** Returns the i-th dimension. */
  const Dimension* dimension(unsigned int i) const;

  /**
   * Returns a constant pointer to the selected dimension (nullptr if it
   * does not exist).
   */
  const Dimension* dimension(const std::string& name) const;

  /** Returns the dimension names. */
  std::vector<std::string> dim_names() const;

  /** Returns the dimension types. */
  std::vector<Datatype> dim_types() const;

  /** Returns the number of dimensions. */
  unsigned int dim_num() const;

  /** Dumps the array schema in ASCII format in the selected output. */
  void dump(FILE* out) const;

  /**
   * Checks if the array schema has a attribute of the given name.
   *
   * @param name Name of attribute to check for
   * @param has_attr Set to true if the array schema has a attribute of the
   * given name.
   * @return Status
   */
  Status has_attribute(const std::string& name, bool* has_attr) const;

  /** Returns true if the input name is an attribute. */
  bool is_attr(const std::string& name) const;

  /** Returns true if the input name is a dimension. */
  bool is_dim(const std::string& name) const;

  /** Returns true if the input name is a dimension, attribute or coords. */
  bool is_field(const std::string& name) const;

  /** Returns true if the input name is nullable. */
  bool is_nullable(const std::string& name) const;

  /**
   * Serializes the array schema object into a buffer.
   *
   * @param buff The buffer the array schema is serialized into.
   * @return Status
   */
  Status serialize(Buffer* buff) const;

  /** Returns the tile order. */
  Layout tile_order() const;

  /**
   * Returns the type of the input attribute/dimension (could also be
   * TILEDB_COORDS).
   */
  Datatype type(const std::string& name) const;

  /**
   * Returns *true* if the input attribute/dimension has variable-sized
   * values.
   */
  bool var_size(const std::string& name) const;

  /**
   * Adds an attribute, copying the input.
   *
   * @param attr The attribute to be added
   * @param check_special If `true` this function will check if the attribute
   *     is special (starting with `__`) and error if that's the case. Setting
   *     to `false` will allow adding attributes starting with `__`, noting
   *     that particular care must be taken (i.e., the user must know what
   *     they are doing in this case).
   * @return Status
   */
  Status add_attribute(const Attribute* attr, bool check_special = true);

  /**
   * Drops an attribute.
   *
   * @param attr_name The name of the attribute to be removed.
   * @return Status
   */
  Status drop_attribute(const std::string& attr_name);

  /**
   * It assigns values to the members of the object from the input buffer.
   *
   * @param buff The binary representation of the object to read from.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff);

  /** Returns the array domain. */
  const Domain* domain() const;

  /**
   * Initializes the ArraySchema object. It also performs a check to see if
   * all the member attributes have been properly set.
   *
   * @return Status
   */
  Status init();

  /**
   * Sets whether the array allows coordinate duplicates.
   * It errors out if set to `1` for dense arrays.
   */
  Status set_allows_dups(bool allows_dups);

  /** Sets an array URI. */
  void set_array_uri(const URI& array_uri);

  /** Sets the filter pipeline for the variable cell offsets. */
  Status set_cell_var_offsets_filter_pipeline(const FilterPipeline* pipeline);

  /** Sets the filter pipeline for the validity cell offsets. */
  Status set_cell_validity_filter_pipeline(const FilterPipeline* pipeline);

  /** Sets the filter pipeline for the coordinates. */
  Status set_coords_filter_pipeline(const FilterPipeline* pipeline);

  /** Sets the tile capacity. */
  void set_capacity(uint64_t capacity);

  /** Sets the cell order. */
  Status set_cell_order(Layout cell_order);

  /**
   * Sets the domain. The function returns an error if the array has been
   * previously set to be a key-value store.
   */
  Status set_domain(Domain* domain);

  /** Sets the tile order. */
  Status set_tile_order(Layout tile_order);

  /** Set version of schema, only used for serialization */
  void set_version(uint32_t version);

  /** Returns the version to write in. */
  uint32_t write_version() const;

  /** Returns the array schema version. */
  uint32_t version() const;

  /** Set a timestamp range for the array schema */
  Status set_timestamp_range(
      const std::pair<uint64_t, uint64_t>& timestamp_range);

  /** Returns the timestamp range. */
  std::pair<uint64_t, uint64_t> timestamp_range() const;

  /** Returns the the first timestamp. */
  uint64_t timestamp_start() const;

  /** Returns the array schema uri. */
  URI uri();

  /** Set schema URI, along with parsing out timestamp ranges and name. */
  void set_uri(const URI& uri);

  /** Get schema URI with return status. */
  Status get_uri(URI* uri);

  /** Returns the schema name. If it is not set, will build it. */
  std::string name();

  /** Returns the schema name. If it is not set, will returns error status. */
  Status get_name(std::string* name) const;

  /** Set the schema name. */
  void set_name(const std::string& name);

  /** Generates a new array schema URI. */
  Status generate_uri();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * True if the array allows coordinate duplicates. Applicable only
   * to sparse arrays.
   */
  bool allows_dups_;

  /** An array name attached to the schema object. */
  URI array_uri_;

  /** The array type. */
  ArrayType array_type_;

  /** It maps each attribute name to the corresponding attribute object. */
  std::unordered_map<std::string, Attribute*> attribute_map_;

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

  /** The filter pipeline run on offset tiles for var-length attributes. */
  FilterPipeline cell_var_offsets_filters_;

  /** The filter pipeline run on validity tiles for nullable attributes. */
  FilterPipeline cell_validity_filters_;

  /** The filter pipeline run on coordinate tiles. */
  FilterPipeline coords_filters_;

  /** It maps each dimension name to the corresponding dimension object. */
  std::unordered_map<std::string, const Dimension*> dim_map_;

  /** The array domain. */
  Domain* domain_;

  /**
   * The tile order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   */
  Layout tile_order_;

  /** The format version of this array schema. */
  uint32_t version_;

  /** Mutex for thread-safety. */
  mutable std::mutex mtx_;

  /**
   * The timestamp the array schema was written.
   * This is used to determine the array schema file name.
   * The two timestamps are identical.
   * It is stored as a pair to keep the usage consistent with metadata
   */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** The URI of the array schema file. */
  URI uri_;

  /** The file name of array schema in the format of timestamp_timestamp_uuid */
  std::string name_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Returns false if the union of attribute and dimension names contain
   * duplicates.
   */
  bool check_attribute_dimension_names() const;

  /**
   * Returns error if double delta compression is used in the zipped
   * coordinate filters and is inherited by a dimension.
   */
  Status check_double_delta_compressor() const;

  /** Clears all members. Use with caution! */
  void clear();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_SCHEMA_H
