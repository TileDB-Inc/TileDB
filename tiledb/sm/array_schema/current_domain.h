/**
 * @file current_domain.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines class CurrentDomain.
 */

#ifndef TILEDB_CURRENT_DOMAIN_H
#define TILEDB_CURRENT_DOMAIN_H

#include <iostream>

#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/ndrectangle.h"
#include "tiledb/sm/enums/current_domain_type.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/storage_format/serialization/serializers.h"

namespace tiledb::sm {

class MemoryTracker;
class ArraySchema;
class Domain;

/** Defines an array current domain */
class CurrentDomain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** No default constructor*/
  CurrentDomain() = delete;

  DISABLE_COPY(CurrentDomain);
  DISABLE_MOVE(CurrentDomain);

  /** Constructor
   *
   * @param memory_tracker The memory tracker.
   * @param version the disk version of this current domain
   */
  CurrentDomain(
      shared_ptr<MemoryTracker> memory_tracker, format_version_t version);

  /** Constructor
   *
   * @param memory_tracker The memory tracker.
   * @param version the disk version of this current domain
   * @param ndr the NDRectangle to to set on this instance
   */
  CurrentDomain(
      shared_ptr<MemoryTracker> memory_tracker,
      format_version_t version,
      shared_ptr<const NDRectangle> ndr);

  /** Destructor. */
  ~CurrentDomain() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  DISABLE_COPY_ASSIGN(CurrentDomain);
  DISABLE_MOVE_ASSIGN(CurrentDomain);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */
  /**
   * Deserialize a CurrentDomain
   *
   * @param deserializer The deserializer to deserialize from.
   * @param memory_tracker The memory tracker associated with this
   * CurrentDomain.
   * @param domain The array schema domain.
   * @return A new CurrentDomain.
   */
  static shared_ptr<const CurrentDomain> deserialize(
      Deserializer& deserializer,
      shared_ptr<MemoryTracker> memory_tracker,
      shared_ptr<Domain> domain);

  /**
   * Serializes the CurrentDomain into a buffer.
   *
   * @param serializer The object the array schema is serialized into.
   */
  void serialize(Serializer& serializer) const;

  /**
   * @return Returns the type of current domain stored in this instance
   */
  CurrentDomainType type() const {
    return type_;
  }

  /**
   * @return Returns whether this current domain is empty or not.
   */
  bool empty() const {
    return empty_;
  }

  /**
   * @return Returns the format version of this current domain
   */
  format_version_t version() const {
    return version_;
  }

  /**
   * Dump a textual representation of the CurrentDomain to the FILE
   *
   * @param out A file pointer to write to. If out is nullptr, use stdout
   */
  void dump(FILE* out) const;

  /**
   * Sets a NDRectangle to this current domain and adjusts its type to reflect
   * that. Throws if the current domain is not empty.
   *
   * @param ndr A NDRectangle to be set on this CurrentDomain object.
   */
  void set_ndrectangle(std::shared_ptr<const NDRectangle> ndr);

  /**
   * Throws if the current domain doesn't have a NDRectangle set
   *
   * @return Returns the ndrectangle set on a current domain.
   */
  shared_ptr<const NDRectangle> ndrectangle() const;

  /**
   * Checks if the argument fully contains this current domain.
   *
   * @param expanded_current_domain The current domain we want to compare
   *     against
   * @return True if the argument is a superset of the current instance
   */
  bool covered(shared_ptr<const CurrentDomain> expanded_current_domain) const;

  /**
   * Checks if the arg fully contains this current domain.
   *
   * @param expanded_range The ndrange we want to compare against
   * @return True if the argument is a superset of the current instance
   */
  bool covered(const NDRange& expanded_range) const;

  /**
   * Perform various checks to ensure the current domain is coherent with the
   * array schema
   *
   * @param schema_domain The array schema domain
   */
  void check_schema_sanity(shared_ptr<Domain> schema_domain) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The memory tracker of the CurrentDomain.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The type of the current domain */
  CurrentDomainType type_;

  /** A flag which enables or disables inequality comparisons */
  bool empty_;

  /** The ndrectangle current domain */
  shared_ptr<const NDRectangle> ndrectangle_;

  /** The format version of this CurrentDomain */
  format_version_t version_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_CURRENT_DOMAIN_H
