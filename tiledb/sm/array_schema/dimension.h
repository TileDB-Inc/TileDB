/**
 * @file   dimension.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines class Dimension.
 */

#ifndef TILEDB_DIMENSION_H
#define TILEDB_DIMENSION_H

#include <string>

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

/** Manipulates a TileDB dimension. */
class Dimension {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Dimension();

  /**
   * Constructor.
   *
   * @param name The name of the dimension.
   * @param type The type of the dimension.
   */
  Dimension(const std::string& name, Datatype type);

  /**
   * Constructor. It clones the input.
   *
   * @param dim The dimension to clone.
   */
  explicit Dimension(const Dimension* dim);

  /** Destructor. */
  ~Dimension() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the number of values per cell. */
  unsigned cell_val_num() const;

  /** Returns the size (in bytes) of a coordinate in this dimension. */
  uint64_t coord_size() const;

  /** Returns the input coordinate in string format. */
  std::string coord_to_str(const void* coord) const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param type The type of the dimension.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff, Datatype type);

  /** Returns the domain. */
  const Range& domain() const;

  /** Dumps the dimension contents in ASCII form in the selected output. */
  void dump(FILE* out) const;

  /** Returns the filter pipeline of this dimension. */
  const FilterPipeline* filters() const;

  /** Returns the dimension name. */
  const std::string& name() const;

  /** Returns true if this is an anonymous (unlabled) dimension **/
  bool is_anonymous() const;

  /**
   * Returns true if the input coordinate is out-of-bounds with respect
   * to the dimension domain.
   *
   * @param coord The coordinate to be checked. It will properly be
   *     type-cast to the dimension datatype.
   * @param err_msg An error message to be retrieved in case the function
   *     returns true.
   * @return True if the input coordinates is out-of-bounds.
   */
  bool oob(const void* coord, std::string* err_msg) const;

  /**
   * Returns true if the input coordinate is out-of-bounds with respect
   * to the dimension domain.
   *
   * @param dim The dimension to apply the oob check on.
   * @param coord The coordinate to be checked. It will properly be
   *     type-cast to the dimension datatype.
   * @param err_msg An error message to be retrieved in case the function
   *     returns true.
   * @return True if the input coordinates is out-of-bounds.
   */
  template <class T>
  static bool oob(
      const Dimension* dim, const void* coord, std::string* err_msg);

  /** Returns `true` if `value` is within the 1D `range`. */
  template <class T>
  static bool value_in_range(
      const Dimension* dim, const Value& value, const Range& range);

  /** Returns `true` if `value` is within the 1D `range`. */
  bool value_in_range(const Value& value, const Range& range) const;

  /** Returns `true` if 1D ranges `a` and `b` overlap. */
  template <class T>
  static bool overlap(const Dimension* dim, const Range& a, const Range& b);

  /** Returns `true` if 1D ranges `a` and `b` overlap. */
  bool overlap(const Range& a, const Range& b) const;

  /** Crops 1D range `a` using 1D range `b`. */
  template <class T>
  static void crop(const Dimension* dim, Range* a, const Range& b);

  /** Crops 1D range `a` using 1D range `b`. */
  void crop(Range* a, const Range& b) const;

  /** Expands range `a` using range `b`. */
  template <class T>
  static void expand_range(const Dimension* dim, Range* a, const Range& b);

  /** Expands range `a` using range `b`. */
  void expand_range(Range* a, const Range& b) const;

  /** Expands range `a` using range `b`. */
  /** Expands range `a` to coincide with the dimension tiles. */
  template <class T>
  static void expand_range_to_tile(const Dimension* dim, Range* a);

  /** Expands range `a` to coincide with the dimension tiles. */
  void expand_range_to_tile(Range* range) const;

  /**
   * Returns the number of tiles the input 1D range consists of,
   * based on the dimension's tile extent.
   */
  template <class T>
  static uint64_t tile_num(const Dimension* dim, const Range& range);

  /**
   * Returns the number of tiles the input 1D range consists of,
   * based on the dimension's tile extent.
   */
  uint64_t tile_num(const Range& range) const;

  /** Returns the 1D tile domain of `range` based on `domain`. */
  template <class T>
  static Range tile_domain(
      const Dimension* dim, const Range& range, const Range& domain);

  /** Returns the 1D tile domain of `range` based on `domain`. */
  Range tile_domain(const Range& range, const Range& domain) const;

  /** Returns the start value of `range`. */
  template <class T>
  static Value range_start(const Dimension* dim, const Range& range);

  /** Returns the start value of `range`. */
  Value range_start(const Range& range) const;

  /** Returns a * b, after casting `a` to `uint64_t`. */
  template <class T>
  static uint64_t mul(const Dimension* dim, const Value& a, uint64_t b);

  /** Returns a * b, after casting `a` to `uint64_t`. */
  uint64_t mul(const Value& a, uint64_t b) const;

  /** Adds `b` to value `a`. */
  template <class T>
  static void add(const Dimension* dim, Value* a, uint64_t b);

  /** Adds `b` to value `a`. */
  void add(Value* a, uint64_t b) const;

  /** Returns true if `value` exceeds the high bound of `range`. */
  template <class T>
  static bool value_after_range(
      const Dimension* dim, const Value& value, const Range& range);

  /** Returns true if `value` exceeds the high bound of `range`. */
  bool value_after_range(const Value& value, const Range& range) const;

  /** Sets `value` to the `range` start value. */
  template <class T>
  static void set_value_to_range_start(
      const Dimension* dim, Value* value, const Range& range);

  /** Sets `value` to the `range` start value. */
  void set_value_to_range_start(Value* value, const Range& range) const;

  /**
   * Returns the overlap percentage between `range` and the tile in `domain`
   * indicated by `tile_coords`.
   */
  template <class T>
  static double tile_coverage(
      const Dimension* dim,
      const Range& domain,
      const Value& tile_coords,
      const Range& range);

  /**
   * Returns the overlap percentage between `range` and the tile in `domain`
   * indicated by `tile_coords`.
   */
  double tile_coverage(
      const Range& domain, const Value& tile_coords, const Range& range) const;

  /** Returns the overlap between `a` and `b` over the volume of `b`. */
  template <class T>
  static double coverage(const Dimension* dim, const Range& a, const Range& b);

  /** Returns the overlap between `a` and `b` over the volume of `b`. */
  double coverage(const Range& a, const Range& b) const;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  Status serialize(Buffer* buff);

  /** Sets the domain. */
  Status set_domain(const void* domain);

  /** Sets the domain. */
  Status set_domain(const Range& domain);

  /** Sets the tile extent. */
  Status set_tile_extent(const void* tile_extent);

  /**
   * If the tile extent is `null`, this function sets the
   * the tile extent to the dimension domain range.
   */
  Status set_null_tile_extent_to_range();

  /**
   * If the tile extent is `null`, this function sets the
   * the tile extent to the dimension domain range.
   *
   * @tparam T The dimension type.
   */
  template <class T>
  Status set_null_tile_extent_to_range();

  /** Returns the tile extent. */
  const void* tile_extent() const;

  /** Returns the dimension type. */
  Datatype type() const;

  /** Returns true if the dimension is var-sized. */
  bool var_size() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The dimension domain. */
  Range domain_;

  /** The dimension name. */
  std::string name_;

  /** The tile extent of the dimension. */
  std::vector<uint8_t> tile_extent_;

  /** The dimension type. */
  Datatype type_;

  /**
   * Stores the appropriate templated oob() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension* dim, const void*, std::string*)>
      oob_func_;

  /**
   * Stores the appropriate templated value_in_range() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension* dim, const Value&, const Range&)>
      value_in_range_func_;

  /**
   * Stores the appropriate templated overlap() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension* dim, const Range&, const Range&)>
      overlap_func_;

  /**
   * Stores the appropriate templated crop() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, Range*, const Range&)> crop_func_;

  /**
   * Stores the appropriate templated expand_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, Range*, const Range&)>
      expand_range_func_;

  /**
   * Stores the appropriate templated expand_range_to_tile() function based on
   * the dimension datatype.
   */
  std::function<void(const Dimension* dim, Range*)> expand_range_to_tile_func_;

  /**
   * Stores the appropriate templated tile_num() function based on the
   * dimension datatype.
   */
  std::function<uint64_t(const Dimension* dim, const Range&)> tile_num_func_;

  /**
   * Stores the appropriate templated tile_domain() function based on the
   * dimension datatype.
   */
  std::function<Range(const Dimension* dim, const Range&, const Range&)>
      tile_domain_func_;

  /**
   * Stores the appropriate templated range_start() function based on the
   * dimension datatype.
   */
  std::function<Value(const Dimension* dim, const Range&)> range_start_func_;

  /**
   * Stores the appropriate templated mul() function based on the
   * dimension datatype.
   */
  std::function<uint64_t(const Dimension* dim, const Value&, uint64_t)>
      mul_func_;

  /**
   * Stores the appropriate templated add() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, Value*, uint64_t)> add_func_;

  /**
   * Stores the appropriate templated value_after_range() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension* dim, const Value&, const Range&)>
      value_after_range_func_;

  /**
   * Stores the appropriate templated set_value_to_range_start() function based
   * on the dimension datatype.
   */
  std::function<void(const Dimension* dim, Value*, const Range&)>
      set_value_to_range_start_func_;

  /**
   * Stores the appropriate templated tile_coverage() function based
   * on the dimension datatype.
   */
  std::function<double(
      const Dimension* dim, const Range&, const Value&, const Range&)>
      tile_coverage_func_;

  /**
   * Stores the appropriate templated coverage() function based
   * on the dimension datatype.
   */
  std::function<double(const Dimension* dim, const Range&, const Range&)>
      coverage_func_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Returns an error if the set domain is invalid. */
  Status check_domain() const;

  /**
   * Returns an error if the set domain is invalid.
   * Applicable only to integer domains
   *
   * @tparam T The type of the dimension domain.
   * @return Status
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  Status check_domain() const {
    assert(!domain_.empty());
    auto domain = (const T*)&domain_[0];

    // Upper bound should not be smaller than lower
    if (domain[1] < domain[0])
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Upper domain bound should "
          "not be smaller than the lower one"));

    // Domain range must not exceed the maximum uint64_t number
    // for integer domains
    uint64_t diff = domain[1] - domain[0];
    if (diff == std::numeric_limits<uint64_t>::max())
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Domain range (upper + lower + 1) is larger "
          "than the maximum uint64 number"));

    return Status::Ok();
  }

  /**
   * Returns an error if the set domain is invalid.
   * Applicable only to real domains.
   *
   * @tparam T The type of the dimension domain.
   * @return Status
   */
  template <
      typename T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  Status check_domain() const {
    assert(!domain_.empty());
    auto domain = (const T*)&domain_[0];

    // Check for NAN and INF
    if (std::isinf(domain[0]) || std::isinf(domain[1]))
      return LOG_STATUS(
          Status::DimensionError("Domain check failed; domain contains NaN"));
    if (std::isnan(domain[0]) || std::isnan(domain[1]))
      return LOG_STATUS(
          Status::DimensionError("Domain check failed; domain contains NaN"));

    // Upper bound should not be smaller than lower
    if (domain[1] < domain[0])
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Upper domain bound should "
          "not be smaller than the lower one"));

    return Status::Ok();
  }

  /** Returns an error if the set tile extent is invalid. */
  Status check_tile_extent() const;

  /**
   * Returns an error if the set tile extent is invalid.
   *
   * @tparam T The type of the dimension domain.
   * @return Status
   */
  template <class T>
  Status check_tile_extent() const;

  /** Sets the templated oob() function. */
  void set_oob_func();

  /** Sets the templated value_in_range() function. */
  void set_value_in_range_func();

  /** Sets the templated overlap() function. */
  void set_overlap_func();

  /** Sets the templated crop() function. */
  void set_crop_func();

  /** Sets the templated expand_range() function. */
  void set_expand_range_func();

  /** Sets the templated expand_range_to_tile() function. */
  void set_expand_range_to_tile_func();

  /** Sets the templated tile_num() function. */
  void set_tile_num_func();

  /** Sets the templated tile_domain() function. */
  void set_tile_domain_func();

  /** Sets the templated range_start() function. */
  void set_range_start_func();

  /** Sets the templated mul() function. */
  void set_mul_func();

  /** Sets the templated add() function. */
  void set_add_func();

  /** Sets the templated value_after_range() function. */
  void set_value_after_range_func();

  /** Sets the templated set_value_to_range_start() function. */
  void set_set_value_to_range_start_func();

  /** Sets the templated tile_coverage() function. */
  void set_tile_coverage_func();

  /** Sets the templated coverage() function. */
  void set_coverage_func();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DIMENSION_H
