/**
 * @file   array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/hilbert.h"
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/domain.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Attribute;
class Buffer;
class ConstBuffer;
class Dimension;
class DimensionLabel;
class Domain;
class Enumeration;
class MemoryTracker;
class CurrentDomain;

enum class ArrayType : uint8_t;
enum class Compressor : uint8_t;
enum class Datatype : uint8_t;
enum class DataOrder : uint8_t;
enum class Layout : uint8_t;

/** Specifies the array schema. */
class ArraySchema {
 public:
  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   *
   * Note: This should be the same as `Domain::dimension_size_type`. We're
   * not including `domain.h`, otherwise we'd use that definition here.
   */
  using dimension_size_type = unsigned int;

  /**
   * Size type for the number of attributes of an array and for attribute
   * indices.
   */
  using attribute_size_type = unsigned int;

  /**
   * Size type for the number of dimension labels in an array and for
   * label indices.
   */
  using dimension_label_size_type = unsigned int;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ArraySchema() = delete;

  /** Constructor.
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   */
  ArraySchema(ArrayType array_type, shared_ptr<MemoryTracker> memory_tracker);

  /** Constructor with std::vector attributes.
   * @param uri The URI of the array schema file.
   * @param version The format version of this array schema.
   * @param timestamp_range The timestamp the array schema was written.
   * @param name The file name of the schema in timestamp_timestamp_uuid format.
   * @param array_type The array type.
   * @param allows_dups True if the (sparse) array allows coordinate duplicates.
   * @param domain The array domain.
   * @param cell_order The cell order.
   * @param tile_order The tile order.
   * @param capacity The tile capacity for the case of sparse fragments.
   * @param attributes The array attributes.
   * @param dimension_labels The array dimension labels.
   * @param enumerations The array enumerations
   * @param enumeration_path_map The array enumeration path map
   * @param cell_var_offsets_filters
   *    The filter pipeline run on offset tiles for var-length attributes.
   * @param cell_validity_filters
   *    The filter pipeline run on validity tiles for nullable attributes.
   * @param coords_filters The filter pipeline run on coordinate tiles.
   * @param current_domain The array current domain object
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   **/
  ArraySchema(
      URI uri,
      uint32_t version,
      std::pair<uint64_t, uint64_t> timestamp_range,
      std::string name,
      ArrayType array_type,
      bool allows_dups,
      shared_ptr<Domain> domain,
      Layout cell_order,
      Layout tile_order,
      uint64_t capacity,
      std::vector<shared_ptr<const Attribute>> attributes,
      std::vector<shared_ptr<const DimensionLabel>> dimension_labels,
      std::vector<shared_ptr<const Enumeration>> enumerations,
      std::unordered_map<std::string, std::string> enumeration_path_map,
      FilterPipeline cell_var_offsets_filters,
      FilterPipeline cell_validity_filters,
      FilterPipeline coords_filters,
      shared_ptr<CurrentDomain> current_domain,
      shared_ptr<MemoryTracker> memory_tracker);

  /**
   * Copy constructor. Clones the input.
   *
   * @param array_schema The array schema to copy.
   */
  ArraySchema(const ArraySchema& array_schema);

  DISABLE_COPY_ASSIGN(ArraySchema);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ArraySchema);

  /** Destructor. */
  ~ArraySchema() = default;

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Returns true if the name is a special attribute.
   */
  static inline bool is_special_attribute(const std::string& name) {
    return name == constants::coords || name == constants::timestamps ||
           name == constants::delete_timestamps ||
           name == constants::delete_condition_index ||
           name == constants::count_of_rows;
  }

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
  const Attribute* attribute(attribute_size_type id) const;

  /**
   * Returns a shared pointer to the selected attribute.
   */
  shared_ptr<const Attribute> shared_attribute(attribute_size_type id) const;

  /**
   * Returns a constant pointer to the selected attribute (nullptr if it
   * does not exist).
   */
  const Attribute* attribute(const std::string& name) const;

  /**
   * Returns a shared pointer to the selected attribute if found. Returns an
   * empty pointer otherwise.
   */
  shared_ptr<const Attribute> shared_attribute(const std::string& name) const;

  /** Returns the number of attributes. */
  inline attribute_size_type attribute_num() const {
    return static_cast<attribute_size_type>(attributes_.size());
  }

  /** Returns the attributes. */
  inline const tdb::pmr::vector<shared_ptr<const Attribute>>& attributes()
      const {
    return attributes_;
  }

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
   * Throws if validation fails
   */
  void check(const Config& cfg) const;

  /**
   * Checks the correctness of the array schema without config access.
   */
  void check_without_config() const;

  /**
   * Throws an error if the provided schema does not match the definition given
   * in the dimension label reference.
   *
   * @param name The name of the dimension label.
   * @param schema The dimension label schema to check.
   */
  void check_dimension_label_schema(
      const std::string& name, const ArraySchema& schema) const;

  /**
   * Return the filter pipeline for the given attribute/dimension (can be
   * TILEDB_COORDS).
   */
  const FilterPipeline& filters(const std::string& name) const;

  /** Return the pipeline used for coordinates. */
  const FilterPipeline& coords_filters() const;

  /** Return the array current domain. */
  inline const CurrentDomain& current_domain() const {
    return *current_domain_;
  }

  /** True if the array is dense. */
  bool dense() const;

  /** Returns the i-th dimension label. */
  const DimensionLabel& dimension_label(dimension_label_size_type i) const;

  /**
   * Returns the selected dimension label.
   *
   * A status exception is thrown if the dimension label does not exist.
   */
  const DimensionLabel& dimension_label(const std::string& name) const;

  /** Returns the i-th dimension. */
  const Dimension* dimension_ptr(dimension_size_type i) const;

  /**
   * Returns a constant pointer to the selected dimension (nullptr if it
   * does not exist).
   */
  const Dimension* dimension_ptr(const std::string& name) const;

  /** Returns the dimension names. */
  std::vector<std::string> dim_names() const;

  /** Returns the dimension types. */
  std::vector<Datatype> dim_types() const;

  /** Returns the number of dimension labels. */
  dimension_label_size_type dim_label_num() const;

  /** Returns the number of dimensions. */
  dimension_size_type dim_num() const;

  /**
   * Checks if the array schema has a attribute of the given name.
   *
   * @param name Name of attribute to check for
   * @param has_attr Set to true if the array schema has a attribute of the
   * given name.
   * @return Status
   */
  Status has_attribute(const std::string& name, bool* has_attr) const;

  bool has_ordered_attributes() const;

  /** Returns true if the input name is an attribute. */
  bool is_attr(const std::string& name) const;

  /** Returns true if the input name is a dimension. */
  bool is_dim(const std::string& name) const;

  /** Returns true if the input name is a dimension label. */
  bool is_dim_label(const std::string& name) const;

  /** Returns true if the input name is a dimension, attribute or coords. */
  bool is_field(const std::string& name) const;

  /** Returns true if the input name is nullable. */
  bool is_nullable(const std::string& name) const;

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
  Status add_attribute(
      shared_ptr<const Attribute> attr, bool check_special = true);

  /**
   * Adds a dimension label to the array schema.
   *
   * @param dim_id The index of the dimension the label applied to.
   * @param name The name of the dimension label.
   * @param label_order The order of the label data.
   * @param label_type The datda type of the label data.
   * @param check_name If ``true``, check the name does not conflict with other
   *     labels, attributes, or dimensions.
   **/
  void add_dimension_label(
      dimension_size_type dim_id,
      const std::string& name,
      DataOrder label_order,
      Datatype label_type,
      bool check_name = true);

  /**
   * Drops an attribute.
   *
   * @param attr_name The name of the attribute to be removed.
   * @return Status
   */
  Status drop_attribute(const std::string& attr_name);

  /**
   * Add an Enumeration to this ArraySchema.
   *
   * @param enmr The enumeration to add.
   */
  void add_enumeration(shared_ptr<const Enumeration> enmr);

  /**
   * Extend an Enumeration on this ArraySchema.
   *
   * N.B., this method is intended to be called via ArraySchemaEvolution.
   * Calling it from anywhere else is likely incorrect.
   *
   * @param enmr The extended enumeration.
   */
  void extend_enumeration(shared_ptr<const Enumeration> enmr);

  /**
   * Check if an enumeration exists with the given name.
   *
   * @param enmr_name The name to check
   * @return bool Whether the enumeration exists.
   */
  bool has_enumeration(const std::string& enmr_name) const;

  /**
   * Store a known enumeration on this ArraySchema after the schema
   * was loaded. This allows for only incuring the cost of loading an
   * enumeration when it is needed. An exception is thrown if the
   * Enumeration is unknown to this ArraySchema.
   *
   * @param enmr The Enumeration to store.
   */
  void store_enumeration(shared_ptr<const Enumeration> enmr);

  /**
   * Get a vector of Enumeration names.
   *
   * @return A vector of enumeration names.
   */
  std::vector<std::string> get_enumeration_names() const;

  /**
   * Get a vector of loaded Enumerations.
   *
   * @return A vector of loaded Enumerations.
   */
  std::vector<shared_ptr<const Enumeration>> get_loaded_enumerations() const;

  /**
   * Get a vector of loaded Enumeration names.
   *
   * @return A vector of loaded enumeration names.
   */
  std::vector<std::string> get_loaded_enumeration_names() const;

  /**
   * Check if a given enumeration has already been loaded.
   *
   * @param enumeration_name The name of the enumeration to check
   * @return bool Whether the enumeration has been loaded or not.
   */
  bool is_enumeration_loaded(const std::string& enumeration_name) const;

  /**
   * Get an Enumeration by name. Throws if the enumeration is unknown.
   *
   * @param enmr_name The name of the Enumeration.
   * @return shared_ptr<Enumeration>
   */
  shared_ptr<const Enumeration> get_enumeration(
      const std::string& enmr_name) const;

  /**
   * Get an Enumeration's object name. Throws if the enumeration is unknown.
   *
   * @param enmr_name The name of the Enumeration.
   * @return The path name of the enumeration on disk
   */
  const std::string& get_enumeration_path_name(
      const std::string& enmr_name) const;

  /**
   * Drop an enumeration
   *
   * @param enumeration_name The enumeration to drop.
   */
  void drop_enumeration(const std::string& enmr_name);

  /**
   * It assigns values to the members of the object from the input buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param uri The uri of the Array.
   * @param memory_tracker The memory tracker to use.
   * @return A new ArraySchema.
   */
  static shared_ptr<ArraySchema> deserialize(
      Deserializer& deserializer,
      const URI& uri,
      shared_ptr<MemoryTracker> memory_tracker);

  /** Return a cloned copy of this array schema. */
  shared_ptr<ArraySchema> clone() const;

  /** Returns the array domain. */
  inline const Domain& domain() const {
    return *const_cast<const Domain*>(domain_.get());
  }

  /**
   * Return a copy of the shared_pointer to the domain.
   */
  inline shared_ptr<Domain> shared_domain() const {
    return domain_;
  };

  /**
   * Sets whether the array allows coordinate duplicates.
   * It errors out if set to `1` for dense arrays.
   */
  Status set_allows_dups(bool allows_dups);

  /** Sets an array URI. */
  void set_array_uri(const URI& array_uri);

  /** Sets the filter pipeline for the variable cell offsets. */
  Status set_cell_var_offsets_filter_pipeline(const FilterPipeline& pipeline);

  /** Sets the filter pipeline for the validity cell offsets. */
  Status set_cell_validity_filter_pipeline(const FilterPipeline& pipeline);

  /** Sets the filter pipeline for the coordinates. */
  Status set_coords_filter_pipeline(const FilterPipeline& pipeline);

  /** Sets the tile capacity. */
  void set_capacity(uint64_t capacity);

  /** Sets the cell order. */
  Status set_cell_order(Layout cell_order);

  /**
   * Sets a filter on a dimension label filter in an array schema.
   *
   * @param label_name The dimension label name.
   * @param filter_list The filter_list to be set.
   */
  void set_dimension_label_filter_pipeline(
      const std::string& label_name, const FilterPipeline& pipeline);

  /**
   * Sets the tile extent on a dimension label in an array schema.
   *
   * @param label_name The dimension label name.
   * @param tile_extent The tile extent for the dimension of the dimension
   * label.
   */
  void set_dimension_label_tile_extent(
      const std::string& label_name,
      const Datatype type,
      const void* tile_extent);

  /**
   * Sets the domain. The function returns an error if the array has been
   * previously set to be a key-value store.
   */
  Status set_domain(shared_ptr<Domain> domain);

  /** Sets the tile order. */
  Status set_tile_order(Layout tile_order);

  /** Set version of schema, only used for serialization */
  void set_version(format_version_t version);

  /** Returns the version to write in. */
  format_version_t write_version() const;

  /** Returns the array schema version. */
  format_version_t version() const;

  /** Set a timestamp range for the array schema */
  void set_timestamp_range(
      const std::pair<uint64_t, uint64_t>& timestamp_range);

  /** Returns the timestamp range. */
  std::pair<uint64_t, uint64_t> timestamp_range() const;

  /** Returns the first timestamp. */
  uint64_t timestamp_start() const;

  /** Returns the end timestamp */
  uint64_t timestamp_end() const;

  /** Returns the array schema uri. */
  const URI& uri() const;

  /** Returns the schema name. If it is not set, will build it. */
  const std::string& name() const;

  /** Set the schema name. */
  void set_name(const std::string& name);

  /** Generates a new array schema URI with optional timestamp range. */
  void generate_uri(
      std::optional<std::pair<uint64_t, uint64_t>> timestamp_range =
          std::nullopt);

  /** Returns the enumeration map. */
  inline const tdb::pmr::
      unordered_map<std::string, shared_ptr<const Enumeration>>&
      enumeration_map() const {
    return enumeration_map_;
  }

  /** Returns the enumeration path map. */
  inline const tdb::pmr::unordered_map<std::string, std::string>&
  enumeration_path_map() const {
    return enumeration_path_map_;
  }

  /** Returns the dimension labels. */
  inline const tdb::pmr::vector<shared_ptr<const DimensionLabel>>&
  dimension_labels() const {
    return dimension_labels_;
  }

  /**
   * Expand the array current domain
   *
   * @param new_current_domain The new array current domain we want to expand to
   */
  void expand_current_domain(shared_ptr<CurrentDomain> new_current_domain);

  /**
   * Set the array current domain on the schema
   *
   * @param current_domain The array current domain we want to set on the schema
   */
  void set_current_domain(shared_ptr<CurrentDomain> current_domain);

  /** Array current domain accessor */
  shared_ptr<CurrentDomain> get_current_domain() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The memory tracker of the ArraySchema.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The URI of the array schema file. */
  URI uri_;

  /** An array name attached to the schema object. */
  URI array_uri_;

  /** The format version of this array schema. */
  format_version_t version_;

  /**
   * The timestamp the array schema was written.
   * This is used to determine the array schema file name.
   * The two timestamps are identical.
   * It is stored as a pair to keep the usage consistent with metadata
   */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** The file name of array schema in the format of timestamp_timestamp_uuid */
  std::string name_;

  /** The array type. */
  ArrayType array_type_;

  /**
   * True if the array allows coordinate duplicates. Applicable only
   * to sparse arrays.
   */
  bool allows_dups_;

  /** The array domain. */
  shared_ptr<Domain> domain_;

  /** It maps each dimension name to the corresponding dimension object. */
  tdb::pmr::unordered_map<std::string, const Dimension*> dim_map_;

  /**
   * The cell order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   */
  Layout cell_order_;

  /**
   * The tile order. It can be one of the following:
   *    - TILEDB_ROW_MAJOR
   *    - TILEDB_COL_MAJOR
   */
  Layout tile_order_;

  /**
   * The tile capacity for the case of sparse fragments.
   */
  uint64_t capacity_;

  /**
   * Container of `shared_ptr<Attribute>` maintains lifespan for all attributes
   * within this array schema. Other member variables reference objects within
   * this container.
   */
  tdb::pmr::vector<shared_ptr<const Attribute>> attributes_;

  /**
   * Type for the range of the map that is member `attribute_map_`. See the
   * invariants of that variable for the meaning of the members of this
   * `struct`.
   */
  struct attribute_reference {
    const Attribute* pointer;
    attribute_size_type index;
  };

  /**
   * Map from an attribute name to its corresponding Attribute object. Lifespan
   * is maintained by the shared_ptr in `attributes_`.
   *
   * Invariant: For each entry `{p,i}` in `attribute_map_`,
   *   `attributes_[i].get() == p`
   * Invariant: The number of entries in `attribute_map_` is the same as the
   *   number of entries in `attributes_`
   */
  tdb::pmr::unordered_map<std::string, attribute_reference> attribute_map_;

  /** The array dimension labels. */
  tdb::pmr::vector<shared_ptr<const DimensionLabel>> dimension_labels_;

  /** A map from the dimension label names to the label schemas. */
  tdb::pmr::unordered_map<std::string, const DimensionLabel*>
      dimension_label_map_;

  /** A map of Enumeration names to Enumeration pointers. */
  tdb::pmr::unordered_map<std::string, shared_ptr<const Enumeration>>
      enumeration_map_;

  /** A map of Enumeration names to Enumeration filenames */
  tdb::pmr::unordered_map<std::string, std::string> enumeration_path_map_;

  /** The filter pipeline run on offset tiles for var-length attributes. */
  FilterPipeline cell_var_offsets_filters_;

  /** The filter pipeline run on validity tiles for nullable attributes. */
  FilterPipeline cell_validity_filters_;

  /** The filter pipeline run on coordinate tiles. */
  FilterPipeline coords_filters_;

  /** The array current domain */
  shared_ptr<CurrentDomain> current_domain_;

  /** Mutex for thread-safety. */
  mutable std::mutex mtx_;

  /**
   * Number of internal dimension labels - used for constructing label URI.
   *
   * WARNING: This is only for array schema construction. It is not
   * loaded from file when loading the array.
   **/
  dimension_label_size_type nlabel_internal_ = 0;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Throws an error if the union of attribute, dimension, and dimension label
   * names contain duplicates.
   */
  void check_attribute_dimension_label_names() const;

  /**
   * Returns error if double delta compression is used in the zipped
   * coordinate filters and is inherited by a dimension.
   */
  Status check_double_delta_compressor(
      const FilterPipeline& coords_filters) const;

  /**
   * Returns error if RLE or Dictionary encoding is used for string
   * dimensions but it is not the only filter in the filter list.
   */
  Status check_string_compressor(const FilterPipeline& coords_filters) const;

  void check_webp_filter() const;

  // Check enumeration sizes are below the configured maximums.
  void check_enumerations(const Config& cfg) const;

  /** Clears all members. Use with caution! */
  void clear();
};

}  // namespace tiledb::sm

/** Converts the filter into a string representation. */
std::ostream& operator<<(
    std::ostream& os, const tiledb::sm::ArraySchema& schema);

/** XXX COMMENT ME PLZ THX */
void expand_to_tiles_helper(
  const tiledb::sm::Domain& domain,
  const tiledb::sm::CurrentDomain& current_domain,
  tiledb::sm::NDRange* ndrange);

#endif  // TILEDB_ARRAY_SCHEMA_H
