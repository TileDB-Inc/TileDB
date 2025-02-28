/**
 * @file   array_schema_evolution.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file defines class ArraySchemaEvolution.
 */

#ifndef TILEDB_ARRAY_SCHEMA_EVOLUTION_H
#define TILEDB_ARRAY_SCHEMA_EVOLUTION_H

#include <unordered_map>
#include <unordered_set>
#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/hilbert.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Attribute;
class Buffer;
class ConstBuffer;
class Dimension;
class Domain;
class Enumeration;
class MemoryTracker;
class ArraySchema;
class CurrentDomain;

enum class ArrayType : uint8_t;
enum class Compressor : uint8_t;
enum class Datatype : uint8_t;
enum class Layout : uint8_t;

/** Specifies the array schema evolution. */
class ArraySchemaEvolution {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ArraySchemaEvolution() = delete;

  /** Constructor with memory tracker. */
  ArraySchemaEvolution(shared_ptr<MemoryTracker> memory_tracker);

  /** Constructor.
   * @param attrs_to_add Attributes to add to the schema.
   * @param enmrs_to_add Enumerations to add to the schema.
   * @param attrs_to_drop Attributes to remove from the schema.
   * @param timestamp_range Timestamp range to use for the new schema.
   * @param timestamp_range CurrentDomain to use for the new schema.
   * @param memory_tracker Memory tracker to use for the new schema.
   */
  ArraySchemaEvolution(
      std::vector<shared_ptr<Attribute>> attrs_to_add,
      std::unordered_set<std::string> attrs_to_drop,
      std::unordered_map<std::string, shared_ptr<const Enumeration>>
          enmrs_to_add,
      std::unordered_map<std::string, shared_ptr<const Enumeration>>
          enmrs_to_extend,
      std::unordered_set<std::string> enmrs_to_drop,
      std::pair<uint64_t, uint64_t> timestamp_range,
      shared_ptr<CurrentDomain> current_domain,
      shared_ptr<MemoryTracker> memory_tracker);

  DISABLE_COPY_AND_COPY_ASSIGN(ArraySchemaEvolution);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ArraySchemaEvolution);

  /** Destructor. */
  ~ArraySchemaEvolution();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  shared_ptr<ArraySchema> evolve_schema(
      const shared_ptr<const ArraySchema>& orig_schema);

  /**
   * Adds an attribute, copying the input.
   *
   * @param attr The attribute to be added
   */
  void add_attribute(shared_ptr<const Attribute> attr);

  /** Returns the names of attributes to add. */
  std::vector<std::string> attribute_names_to_add() const;

  /**
   * Returns a constant pointer to the selected attribute (nullptr if it
   * does not exist).
   */
  const Attribute* attribute_to_add(const std::string& name) const;

  /**
   * Drops an attribute.
   *
   * @param attribute_name The attribute to be dropped.
   */
  void drop_attribute(const std::string& attribute_name);

  /** Returns the names of attributes to drop. */
  std::vector<std::string> attribute_names_to_drop() const;

  /**
   * Adds an enumeration
   *
   * @param enmr The enumeration to add
   */
  void add_enumeration(shared_ptr<const Enumeration> enmr);

  /** Returns the names of the enumerations to add. */
  std::vector<std::string> enumeration_names_to_add() const;

  /**
   * Returns a constant pointer to the selected enumeration or nullptr if
   * it does not exist.
   *
   * @param name The name of the enumeration to add
   * @return shared_ptr<const Enumeration> The enumeration to add.
   */
  shared_ptr<const Enumeration> enumeration_to_add(
      const std::string& name) const;

  /**
   * Extend an enumeration.
   *
   * @param enmr The enumeration with its extension.
   */
  void extend_enumeration(shared_ptr<const Enumeration> enmr);

  /** Returns the names of the enumerations to extend. */
  std::vector<std::string> enumeration_names_to_extend() const;

  /**
   * Returns a constant pointer to the selected enumeration or nullptr if it
   * does not exist.
   *
   * @param name The name of the enumeration to extend.
   * @return shared_ptr<const Enumeration> The enumeration to extend.
   */
  shared_ptr<const Enumeration> enumeration_to_extend(
      const std::string& name) const;

  /**
   * Drops an enumeration
   *
   * @param enumeration_name The enumeration to be dropped.
   */
  void drop_enumeration(const std::string& enumeration_name);

  /** Return the names of enumerations to drop. */
  std::vector<std::string> enumeration_names_to_drop() const;

  /** Set a timestamp range for the array schema evolution */
  void set_timestamp_range(
      const std::pair<uint64_t, uint64_t>& timestamp_range);

  /** Returns the timestamp range. */
  std::pair<uint64_t, uint64_t> timestamp_range() const;

  /**
   * Expands the array current domain
   *
   * @param current_domain The new current domain to expand to
   */
  void expand_current_domain(shared_ptr<CurrentDomain> current_domain);

  /**
   * Accessor for the current domain we want to expand to
   */
  shared_ptr<CurrentDomain> current_domain_to_expand() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The memory tracker of the ArraySchema.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The array attributes to be added. */
  /** It maps each attribute name to the corresponding attribute object. */
  tdb::pmr::vector<shared_ptr<Attribute>> attributes_to_add_;

  /** The names of array attributes to be dropped. */
  std::unordered_set<std::string> attributes_to_drop_;

  /** Enumerations to add with any attribute. */
  tdb::pmr::unordered_map<std::string, shared_ptr<const Enumeration>>
      enumerations_to_add_map_;

  /** Enumerations to extend. */
  tdb::pmr::unordered_map<std::string, shared_ptr<const Enumeration>>
      enumerations_to_extend_map_;

  /** The names of array enumerations to be dropped. */
  std::unordered_set<std::string> enumerations_to_drop_;

  /**
   * A timestamp to explicitly set the timestamp of
   * the evolved schema.  To be consistent with
   * the schema timestamp_range_, two identical
   * timestamps are stored as a pair.
   */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** The array current domain to expand */
  shared_ptr<CurrentDomain> current_domain_to_expand_;

  /** Mutex for thread-safety. */
  mutable std::mutex mtx_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Clears all members. Use with caution! */
  void clear();
};

}  // namespace tiledb::sm

#endif  // TILEDB_SCHEMA_EVOLUTION_H
