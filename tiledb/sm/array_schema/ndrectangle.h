/**
 * @file ndrectangle.h
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
 * This file defines class NDRectangle.
 */

#ifndef TILEDB_NDRECTANGLE_H
#define TILEDB_NDRECTANGLE_H

#include <iostream>

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/current_domain_type.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/storage_format/serialization/serializers.h"

namespace tiledb::sm {

class MemoryTracker;
class Domain;

/** Defines a NDRectangle */
class NDRectangle {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** No default constructor. */
  NDRectangle() = delete;

  DISABLE_COPY(NDRectangle);
  DISABLE_MOVE(NDRectangle);

  /**
   * Constructor
   *
   * @param memory_tracker The memory tracker.
   * @param dom Array schema domain.
   * @param nd The vector of ranges per dimension.
   */
  NDRectangle(
      shared_ptr<MemoryTracker> memory_tracker,
      shared_ptr<Domain> dom,
      const NDRange& nd);

  /**
   * Constructs a NDRectangle with dim_num empty internal ranges
   *
   * @param memory_tracker The memory tracker.
   * @param dom Array schema domain.
   */
  NDRectangle(shared_ptr<MemoryTracker> memory_tracker, shared_ptr<Domain> dom);

  /** Destructor. */
  ~NDRectangle() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  DISABLE_COPY_ASSIGN(NDRectangle);
  DISABLE_MOVE_ASSIGN(NDRectangle);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */
  /**
   * Deserialize a ndrectangle
   *
   * @param deserializer The deserializer to deserialize from.
   * @param memory_tracker The memory tracker associated with this ndrectangle.
   * @param domain The domain from array schema
   * @return A new ndrectangle.
   */
  static shared_ptr<NDRectangle> deserialize(
      Deserializer& deserializer,
      shared_ptr<MemoryTracker> memory_tracker,
      shared_ptr<Domain> domain);

  /**
   * Deserialize a ndrange
   *
   * @param deserializer The deserializer to deserialize from.
   * @param domain The domain from array schema
   * @return A new NDRange.
   */
  static NDRange deserialize_ndranges(
      Deserializer& deserializer, shared_ptr<Domain> domain);

  /**
   * Serializes the NDRectangle into a buffer.
   *
   * @param serializer The object the NDRectangle is serialized into.
   */
  void serialize(Serializer& serializer) const;

  /**
   * Dump a textual representation of the NDRectangle to the FILE
   *
   * @param out A file pointer to write to. If out is nullptr, use stdout
   */
  void dump(FILE* out) const;

  /** nd ranges accessor */
  const NDRange& get_ndranges() const {
    return range_data_;
  }

  /** domain accessor */
  shared_ptr<Domain> domain() const {
    // Guards for a special cased behavior in REST array schema evolution, see
    // domain_ declaration for a more detailed explanation
    if (domain_ == nullptr) {
      throw std::runtime_error(
          "The Domain instance on this NDRectangle is nullptr");
    }
    return domain_;
  }

  /**
   * Used in REST array schema evolution to set a domain during evolution time
   * because one isn't available during deserialization.
   */
  void set_domain(shared_ptr<Domain> domain);

  /**
   * Set a range for the dimension at idx
   *
   * @param r The range to set
   * @param idx The idx of the dimension given the array schema domain ordering
   */
  void set_range(const Range& r, uint32_t idx);

  /**
   * Set a range for the dimension by name
   *
   * @param r The range to set for the dimension specified by name
   * @param name The name of the dimension
   */
  void set_range_for_name(const Range& r, const std::string& name);

  /**
   * Get the range for the dimension at idx
   *
   * @param name The dimension index
   * @return The range of the dimension
   */
  const Range& get_range(uint32_t idx) const;

  /**
   * Get the range for the dimension specified by name
   *
   * @param name The name of the dimension
   * @return The range of the dimension
   */
  const Range& get_range_for_name(const std::string& name) const;

  /**
   * Get the data type of the range at idx
   *
   * @param idx The index of the range
   * @return The range datatype
   */
  Datatype range_dtype(uint32_t idx) const;

  /**
   * Get the data type of the range at idx
   *
   * @param name The dimension name
   * @return The range datatype
   */
  Datatype range_dtype_for_name(const std::string& name) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The memory tracker of the ndrectangle. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** Per dimension ranges of the rectangle */
  NDRange range_data_;

  /**
   * Array Schema domain. This can be nullptr during
   * array schema evolution on REST when we construct with a nullptr domain_
   * and set it later during ArraySchema::expand_current_domain to avoid
   * serializing the domain on the evolution object */
  shared_ptr<Domain> domain_;
};

}  // namespace tiledb::sm

std::ostream& operator<<(std::ostream& os, const tiledb::sm::NDRectangle& ndr);

#endif  // TILEDB_NDRECTANGLE_H
