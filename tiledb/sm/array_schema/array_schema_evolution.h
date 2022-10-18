/**
 * @file   array_schema_evolution.h
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
 * This file defines class ArraySchemaEvolution.
 */

#ifndef TILEDB_ARRAY_SCHEMA_EVOLUTION_H
#define TILEDB_ARRAY_SCHEMA_EVOLUTION_H

#include <unordered_map>
#include <unordered_set>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/common/uuid.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/hilbert.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Attribute;
class Buffer;
class ConstBuffer;
class Dimension;
class Domain;
class ArraySchema;

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
  ArraySchemaEvolution();

  /** Destructor. */
  ~ArraySchemaEvolution();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  tuple<Status, optional<shared_ptr<ArraySchema>>> evolve_schema(
      const shared_ptr<const ArraySchema>& orig_schema);

  /**
   * Adds an attribute, copying the input.
   *
   * @param attr The attribute to be added
   * @return Status
   */
  Status add_attribute(const Attribute* attr);

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
   * @param attr The attribute to be dropped
   * @return Status
   */
  Status drop_attribute(const std::string& attribute_name);

  /** Returns the names of attributes to drop. */
  std::vector<std::string> attribute_names_to_drop() const;

  /** Set a timestamp range for the array schema evolution */
  Status set_timestamp_range(
      const std::pair<uint64_t, uint64_t>& timestamp_range);

  /** Returns the timestamp range. */
  std::pair<uint64_t, uint64_t> timestamp_range() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array attributes to be added. */
  /** It maps each attribute name to the corresponding attribute object. */
  std::unordered_map<std::string, shared_ptr<Attribute>> attributes_to_add_map_;

  /** The names of array attributes to be dropped. */
  std::unordered_set<std::string> attributes_to_drop_;

  /**
   * A timestamp to explicitly set the timestamp of
   * the evolved schema.  To be consistent with
   * the schema timestamp_range_, two identical
   * timestamps are stored as a pair.
   */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** Mutex for thread-safety. */
  mutable std::mutex mtx_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Clears all members. Use with caution! */
  void clear();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SCHEMA_EVOLUTION_H