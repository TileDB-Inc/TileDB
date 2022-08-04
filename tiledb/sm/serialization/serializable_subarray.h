/**
 * @file serializable_subarray.h
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
 * This file declares the class SerializableSubarray.
 */

#ifndef TILEDB_SERIALIZABLE_SUBARRAY_H
#define TILEDB_SERIALIZABLE_SUBARRAY_H

#include "tiledb/sm/serialization/tiledb-rest.capnp.h"

namespace tiledb {
namespace sm {

namespace stats {
class Stats;
}

class Array;
class ArraySchema;
class RangeSetAndSuperset;
class RelevantFragments;
class Subarray;

enum class Layout : uint8_t;

namespace serialization {

class SerializableSubarray {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /**
   * Constructor, to be called from the Subarray object.
   *
   * @param layout Subarray layout.
   * @param array_schema Reference to the array schema.
   * @param range_subset Reference to the vector of ranges.
   * @param is_default Reference to the vector specifying if a dimension has
   * default ranges.
   * @param relevant_fragments Reference to the vector of relevant fragments.
   * @param stats Pointer to the stats.
   */
  SerializableSubarray(
      const Layout layout,
      const ArraySchema& array_schema,
      const std::vector<RangeSetAndSuperset>& range_subset,
      const std::vector<bool>& is_default,
      const RelevantFragments& relevant_fragments,
      const stats::Stats* stats)
      : layout_(layout)
      , array_schema_(array_schema)
      , range_subset_(range_subset)
      , is_default_(is_default)
      , relevant_fragments_(relevant_fragments)
      , stats_(stats) {
  }

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /**
   * Serializes the object to Json.
   *
   * @return Json string.
   */
  std::string to_json();

  /**
   * Serializes the object to Capnp.
   *
   * @param builder The Subarray Capnp builder.
   */
  void to_capnp(capnp::Subarray::Builder& builder);

  /**
   * Deserializes from a Capnp reader and create a Stats object.
   *
   * @param array Const pointer to the array object.
   * @param query_stats Pointer to the query stats to create a child from.
   * @param query_layout Layout coming from the query.
   * @param reader Capnp reader to deserialize from.
   */
  static Subarray from_capnp(
      const Array* array,
      stats::Stats* query_stats,
      const Layout query_layout,
      const capnp::Subarray::Reader& reader);

 private:
  /* ****************************** */
  /*       PRIVATE ATTRIBUTES       */
  /* ****************************** */

  /** Subarray layout. */
  const Layout layout_;

  /** Const reference to the array schema. */
  const ArraySchema& array_schema_;

  /** Const reference to the vector of ranges. */
  const std::vector<RangeSetAndSuperset>& range_subset_;

  /**
   * Const reference to the vector specifying if a dimension has default
   * ranges.
   */
  const std::vector<bool>& is_default_;

  /** Const reference to the vector of relevant fragments. */
  const RelevantFragments& relevant_fragments_;

  /** Const pointer to the stats. */
  const stats::Stats* stats_;
};

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZABLE_SUBARRAY_H