/**
 * @file   dimension.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include <bitset>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class FilterPipeline;

enum class Compressor : uint8_t;
enum class Datatype : uint8_t;

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

  /** Copy constructor. */
  DISABLE_COPY(Dimension);

  /** Destructor. */
  ~Dimension() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Copy-assignment operator. */
  DISABLE_COPY_ASSIGN(Dimension);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the number of values per coordinate. */
  unsigned cell_val_num() const;

  /** Sets the number of values per coordinate. */
  Status set_cell_val_num(unsigned int cell_val_num);

  /** Returns the size (in bytes) of a coordinate in this dimension. */
  uint64_t coord_size() const;

  /**
   *  Returns a coordinate in string format.
   *
   * @param buff The query buffer that contains all coordinates.
   * @param i The position of the coordinate in the buffer.
   * @return The coordinate in string format.
   */
  std::string coord_to_str(const QueryBuffer& buff, uint64_t i) const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param type The type of the dimension.
   * @param version The array schema version.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff, uint32_t version, Datatype type);

  /** Returns the domain. */
  const Range& domain() const;

  /** Dumps the dimension contents in ASCII form in the selected output. */
  void dump(FILE* out) const;

  /** Returns the filter pipeline of this dimension. */
  const FilterPipeline& filters() const;

  /** Returns the dimension name. */
  const std::string& name() const;

  /**
   *  Returns the tile index for integer values.
   *
   * @param v The value.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The index of the tile.
   */
  template <
      class T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static uint64_t tile_idx(
      const T& v, const T& domain_low, const T& tile_extent) {
    typedef typename std::make_unsigned<T>::type unsigned_t;
    return ((unsigned_t)v - (unsigned_t)domain_low) / (unsigned_t)tile_extent;
  }

  /**
   *  Returns the tile index for floating point values.
   *
   * @param v The value.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The index of the tile.
   */
  template <
      class T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  static uint64_t tile_idx(
      const T& v, const T& domain_low, const T& tile_extent) {
    return (v - domain_low) / tile_extent;
  }

  /**
   *  Rounds the value down to the tile boundary for integer values.
   *
   * @param v The value.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The value rounded down to the tile boundary.
   */
  template <
      class T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static T round_to_tile(
      const T& v, const T& domain_low, const T& tile_extent) {
    typedef typename std::make_unsigned<T>::type unsigned_t;
    return ((unsigned_t)v - (unsigned_t)domain_low) / (unsigned_t)tile_extent *
               (unsigned_t)tile_extent +
           (unsigned_t)domain_low;
  }

  /**
   *  Rounds the value down to the tile boundary for floating point values.
   *
   * @param v The value.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The value rounded down to the tile boundary.
   */
  template <
      class T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  static T round_to_tile(
      const T& v, const T& domain_low, const T& tile_extent) {
    return floor((v - domain_low) / tile_extent) * tile_extent + domain_low;
  }

  /**
   * Returns the tile lower coordinate for integer values.
   *
   * @param tile_num The tile index.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The tile lower coordinate.
   */
  template <
      class T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static T tile_coord_low(
      uint64_t tile_num, const T& domain_low, const T& tile_extent) {
    typedef typename std::make_unsigned<T>::type unsigned_t;
    return (unsigned_t)domain_low + tile_num * (unsigned_t)tile_extent;
  }

  /**
   * Returns the tile lower coordinate for floating point values.
   *
   * @param tile_num The tile index.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The tile lower coordinate.
   */
  template <
      class T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  static T tile_coord_low(
      uint64_t tile_num, const T& domain_low, const T& tile_extent) {
    return domain_low + tile_num * tile_extent;
  }

  /**
   * Returns the tile upper coordinate for integer values.
   *
   * @param tile_num The tile index.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The tile upper coordinate.
   */
  template <
      class T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static T tile_coord_high(
      uint64_t tile_num, const T& domain_low, const T& tile_extent) {
    typedef typename std::make_unsigned<T>::type unsigned_t;
    if ((unsigned_t)tile_extent == std::numeric_limits<unsigned_t>::max())
      return std::numeric_limits<T>::max() -
             (domain_low == std::numeric_limits<T>::min());
    return (unsigned_t)domain_low + ++tile_num * (unsigned_t)tile_extent - 1;
  }

  /**
   * Returns the tile upper coordinate for floating point values.
   *
   * @param tile_num The tile index.
   * @param domain_low The minimum value for the domain.
   * @param tile_extent The tile extent.
   * @return The tile upper coordinate.
   */
  template <
      class T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  static T tile_coord_high(
      uint64_t tile_num, const T& domain_low, const T& tile_extent) {
    return std::nextafter(
        domain_low + ++tile_num * tile_extent, std::numeric_limits<T>::min());
  }

  /**
   * Used to multiply values by the tile extent for integer values.
   *
   * @param v The value to multiply.
   * @param tile_extent The tile extent.
   * @return The result of the multiplication.
   */
  template <
      class T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static T tile_extent_mult(const T& v, const T& tile_extent) {
    typedef typename std::make_unsigned<T>::type unsigned_t;
    return (unsigned_t)v * (unsigned_t)tile_extent;
  }

  /**
   * Used to multiply values by the tile extent for floating point values.
   *
   * @param v The value to multiply.
   * @param tile_extent The tile extent.
   * @return The result of the multiplication.
   */
  template <
      class T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  static T tile_extent_mult(const T& v, const T& tile_extent) {
    return v * tile_extent;
  }

  /**
   * Retrieves the value `v` that lies at the end (ceil) of the tile
   * that is `tile_num` tiles apart from the beginning of `r`.
   */
  void ceil_to_tile(const Range& r, uint64_t tile_num, ByteVecValue* v) const;

  /**
   * Returns the value that lies at the end (ceil) of the tile
   * that is `tile_num` tiles apart from the beginning of `r`.
   */
  template <class T>
  static void ceil_to_tile(
      const Dimension* dim, const Range& r, uint64_t tile_num, ByteVecValue* v);

  /**
   * Performs correctness checks on the input range.
   *
   * Specifically, it checks
   *     - if the lower range bound is larger than the upper
   *     - if the range falls outside the dimension domain
   *     - for real domains, if any range bound is NaN
   *
   */
  Status check_range(const Range& range) const;

  /**
   * Adjust a range so that the upper/lower bounds are within the dimension's
   * domain.
   * @param range Query range object that might be mutated
   * @return status if error
   */
  Status adjust_range_oob(Range* range) const;

  /**
   * Performs correctness checks on the input range. Returns `true`
   * upon error and stores an error message to `err_msg`.
   *
   * Applicable to integral domains.
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static bool check_range(
      const Dimension* dim, const Range& range, std::string* err_msg) {
    auto domain = (const T*)dim->domain().data();
    auto r = (const T*)range.data();

    // Check range bounds
    if (r[0] > r[1]) {
      std::stringstream ss;
      ss << "Cannot add range to dimension; Lower range "
         << "bound " << r[0] << " cannot be larger than the higher bound "
         << r[1];
      *err_msg = ss.str();
      return false;
    }

    // Check out-of-bounds
    if (r[0] < domain[0] || r[1] > domain[1]) {
      std::stringstream ss;
      ss << "Range [" << r[0] << ", " << r[1] << "] is out of domain bounds ["
         << domain[0] << ", " << domain[1] << "] on dimension '" << dim->name()
         << "'";
      *err_msg = ss.str();
      return false;
    }

    return true;
  }

  /**
   * Performs correctness checks on the input range. Returns `true`
   * upon error and stores an error message to `err_msg`.
   *
   * Applicable to real domains.
   */
  template <
      typename T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  static bool check_range(
      const Dimension* dim, const Range& range, std::string* err_msg) {
    auto domain = (const T*)dim->domain().data();
    auto r = (const T*)range.data();

    // Check for NaN
    if (std::isnan(r[0]) || std::isnan(r[1])) {
      *err_msg = "Cannot add range to dimension; Range contains NaN";
      return false;
    }

    // Check range bounds
    if (r[0] > r[1]) {
      std::stringstream ss;
      ss << "Cannot add range to dimension; Lower range "
         << "bound " << r[0] << " cannot be larger than the higher bound "
         << r[1];
      *err_msg = ss.str();
      return false;
    }

    // Check out-of-bounds
    if (r[0] < domain[0] || r[1] > domain[1]) {
      std::stringstream ss;
      ss << "Range [" << r[0] << ", " << r[1] << "] is out of domain bounds ["
         << domain[0] << ", " << domain[1] << "] on dimension '" << dim->name()
         << "'";
      *err_msg = ss.str();

      return false;
    }

    return true;
  }

  /**
   * Takes a range from a query and might mutate it so the lower/upper values
   * are within the domain of the dimension. If mutation occurs a warning is
   * logged
   *
   * @tparam T datatype
   * @param dim dimension object to get domain from
   * @param range Query range objects to mutate
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static void adjust_range_oob(const Dimension* dim, const Range* range) {
    auto domain = (const T*)dim->domain().data();
    auto r = (T*)range->data();

    // Check out-of-bounds
    if (r[0] < domain[0]) {
      std::stringstream ss;
      ss << "Range lower bound " << r[0] << " is out of domain bounds ["
         << domain[0] << ", " << domain[1]
         << "]. Adjusting range lower bound to be " << domain[0]
         << " on dimension '" << dim->name() << "'";
      global_logger().warn(ss.str().c_str());

      r[0] = domain[0];
    }

    if (r[1] > domain[1]) {
      std::stringstream ss;
      ss << "Range upper bound " << r[1] << " is out of domain bounds ["
         << domain[0] << ", " << domain[1]
         << "]. Adjusting range upper bound to be " << domain[1]
         << " on dimension '" << dim->name() << "'";
      global_logger().warn(ss.str().c_str());

      r[1] = domain[1];
    }
  }

  /**
   * Takes a range from a query and might mutate it so the lower/upper values
   * are within the domain of the dimension. If mutation occurs a warning is
   * logged
   *
   * @tparam T datatype
   * @param dim dimension object to get domain from
   * @param range Query range objects to mutate
   */
  template <
      typename T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  static void adjust_range_oob(const Dimension* dim, const Range* range) {
    auto domain = (const T*)dim->domain().data();
    auto r = (T*)range->data();

    // Check out-of-bounds
    if (r[0] < domain[0]) {
      std::stringstream ss;
      ss << "Range lower bound " << r[0] << " is out of domain bounds ["
         << domain[0] << ", " << domain[1]
         << "]. Adjusting range lower bound to be " << domain[0]
         << " on dimension '" << dim->name() << "'";
      global_logger().warn(ss.str().c_str());

      r[0] = domain[0];
    }

    if (r[1] > domain[1]) {
      std::stringstream ss;
      ss << "Range upper bound " << r[1] << " is out of domain bounds ["
         << domain[0] << ", " << domain[1]
         << "]. Adjusting range upper bound to be " << domain[1]
         << " on dimension '" << dim->name() << "'";
      global_logger().warn(ss.str().c_str());

      r[1] = domain[1];
    }
  }

  /** Returns true if the input range coincides with tile boundaries. */
  bool coincides_with_tiles(const Range& r) const;

  /** Returns true if the input range coincides with tile boundaries. */
  template <class T>
  static bool coincides_with_tiles(const Dimension* dim, const Range& r);

  /**
   * Computes the minimum bounding range of the values stored in
   * `tile`. Applicable only to fixed-size dimensions.
   */
  Status compute_mbr(const Tile& tile, Range* mbr) const;

  /**
   * Computed the minimum bounding range of the values stored in
   * `tile`.
   */
  template <class T>
  static Status compute_mbr(const Tile& tile, Range* mbr);

  /**
   * Computes the minimum bounding range of the values stored in
   * `tile_val`. Applicable only to var-sized dimensions.
   */
  Status compute_mbr_var(
      const Tile& tile_off, const Tile& tile_val, Range* mbr) const;

  /**
   * Computes the minimum bounding range of the values stored in
   * `tile_val`. Applicable only to var-sized dimensions.
   */
  template <class T>
  static Status compute_mbr_var(
      const Tile& tile_off, const Tile& tile_val, Range* mbr);

  /**
   * Crops the input 1D range such that it does not exceed the
   * dimension domain.
   */
  void crop_range(Range* range) const;

  /**
   * Crops the input 1D range such that it does not exceed the
   * dimension domain.
   */
  template <class T>
  static void crop_range(const Dimension* dim, Range* range);

  /**
   * Returns the domain range (high - low + 1) of the input
   * 1D range. It returns 0 in case the dimension datatype
   * is not integer or if there is an overflow.
   */
  uint64_t domain_range(const Range& range) const;

  /**
   * Returns the domain range (high - low + 1) of the input
   * 1D range. It returns MAX_UINT64 in case the dimension datatype
   * is not integer or if there is an overflow.
   */
  template <class T>
  static uint64_t domain_range(const Range& range);

  /** Expand fixed-sized 1D range `r` using value `v`. */
  void expand_range_v(const void* v, Range* r) const;

  /** Expand fixed-sized 1D range `r` using value `v`. */
  template <class T>
  static void expand_range_v(const void* v, Range* r);

  /** Expand var-sized 1D range `r` using value `v`. */
  static void expand_range_var_v(const char* v, uint64_t v_size, Range* r);

  /** Expand 1D range `r2` using 1D range `r1`. */
  void expand_range(const Range& r1, Range* r2) const;

  /** Expand 1D range `r2` using 1D range `r1`. */
  template <class T>
  static void expand_range(const Range& r1, Range* r2);

  /**
   * Expand 1D range `r2` using 1D range `r1`.
   * Applicable to var-sized ranges.
   */
  void expand_range_var(const Range& r1, Range* r2) const;

  /**
   * Expands the input 1D range to coincide with the dimension tiles.
   * It is a noop if the tile extents are null and for real domains.
   */
  void expand_to_tile(Range* range) const;

  /**
   * Expands the input 1D range to coincide with the dimension tiles.
   * It is a noop if the tile extents are null and for real domains.
   */
  template <class T>
  static void expand_to_tile(const Dimension* dim, Range* range);

  /**
   * Returns error if the input coordinate is out-of-bounds with respect
   * to the dimension domain.
   */
  Status oob(const void* coord) const;

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

  /** Return true if r1 is fully covered by r2. */
  bool covered(const Range& r1, const Range& r2) const;

  /** Return true if r1 is fully covered by r2. */
  template <class T>
  static bool covered(const Range& r1, const Range& r2);

  /** Return true if the input 1D ranges overlap. */
  bool overlap(const Range& r1, const Range& r2) const;

  /** Return true if the input 1D ranges overlap. */
  template <class T>
  static bool overlap(const Range& r1, const Range& r2);

  /** Return ratio of the overlap of the two input 1D ranges over `r2`. */
  double overlap_ratio(const Range& r1, const Range& r2) const;

  /** Return ratio of the overlap of the two input 1D ranges over `r2`. */
  template <class T>
  static double overlap_ratio(const Range& r1, const Range& r2);

  /** Splits `r` at point `v`, producing 1D ranges `r1` and `r2`. */
  void split_range(
      const Range& r, const ByteVecValue& v, Range* r1, Range* r2) const;

  /** Splits `r` at point `v`, producing 1D ranges `r1` and `r2`. */
  template <class T>
  static void split_range(
      const Range& r, const ByteVecValue& v, Range* r1, Range* r2);

  /**
   * Computes the splitting point `v` of `r`, and sets `unsplittable`
   * to true if `r` cannot be split.
   */
  void splitting_value(
      const Range& r, ByteVecValue* v, bool* unsplittable) const;

  /**
   * Computes the splitting point `v` of `r`, and sets `unsplittable`
   * to true if `r` cannot be split.
   */
  template <class T>
  static void splitting_value(
      const Range& r, ByteVecValue* v, bool* unsplittable);

  /** Return the number of tiles the input range intersects. */
  uint64_t tile_num(const Range& range) const;

  /** Return the number of tiles the input range intersects. */
  template <class T>
  static uint64_t tile_num(const Dimension* dim, const Range& range);

  /**
   * Maps the c-th cell in the input query buffer to a uint64 value,
   * based on discretizing the domain from 0 to `max_bucket_val`.
   * This value is used to compute a Hilbert value.
   */
  uint64_t map_to_uint64(
      const QueryBuffer* buff,
      uint64_t c,
      uint64_t coords_num,
      int bits,
      uint64_t max_bucket_val) const;

  /**
   * Maps the c-th cell in the input query buffer to a uint64 value,
   * based on discretizing the domain from 0 to `max_bucket_val`.
   * This value is used to compute a Hilbert value.
   */
  template <class T>
  static uint64_t map_to_uint64(
      const Dimension* dim,
      const QueryBuffer* buff,
      uint64_t c,
      uint64_t coords_num,
      int bits,
      uint64_t max_bucket_val);

  /**
   * Maps the input coordinate to a uint64 value,
   * based on discretizing the domain from 0 to `max_bucket_val`.
   * This value is used to compute a Hilbert value.
   */
  uint64_t map_to_uint64(
      const void* coord,
      uint64_t coord_size,
      int bits,
      uint64_t max_bucket_val) const;

  /**
   * Maps the input coordinate to a uint64 value,
   * based on discretizing the domain from 0 to `max_bucket_val`.
   * This value is used to compute a Hilbert value.
   */
  template <class T>
  static uint64_t map_to_uint64_2(
      const Dimension* dim,
      const void* coord,
      uint64_t coord_size,
      int bits,
      uint64_t max_bucket_val);

  /**
   * Maps the input result coordinate to a uint64 value,
   * based on discretizing the domain from 0 to `max_bucket_val`.
   * This value is used to compute a Hilbert value.
   */
  uint64_t map_to_uint64(
      const ResultCoords& coord,
      uint32_t dim_idx,
      int bits,
      uint64_t max_bucket_val) const;

  /**
   * Maps the input result coordinate to a uint64 value,
   * based on discretizing the domain from 0 to `max_bucket_val`.
   * This value is used to compute a Hilbert value.
   */
  template <class T>
  static uint64_t map_to_uint64_3(
      const Dimension* dim,
      const ResultCoords& coord,
      uint32_t dim_idx,
      int bits,
      uint64_t max_bucket_val);

  /**
   * Maps a uint64 value (produced by `map_to_uint64`) to its corresponding
   * value in the original dimension domain. `max_bucket_val` is the maximum
   * value used to discretize the original value.
   */
  ByteVecValue map_from_uint64(
      uint64_t value, int bits, uint64_t max_bucket_val) const;

  /**
   * Maps a uint64 value (produced by `map_to_uint64`) to its corresponding
   * value in the original dimension domain. `max_bucket_val` is the maximum
   * value used to discretize the original value.
   */
  template <class T>
  static ByteVecValue map_from_uint64(
      const Dimension* dim, uint64_t value, int bits, uint64_t max_bucket_val);

  /** Returns `true` if `value` is smaller than the start of `range`. */
  bool smaller_than(const ByteVecValue& value, const Range& range) const;

  /** Returns `true` if `value` is smaller than the start of `range`. */
  template <class T>
  static bool smaller_than(
      const Dimension* dim, const ByteVecValue& value, const Range& range);

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @param version The array schema version
   * @return Status
   */
  Status serialize(Buffer* buff, uint32_t version);

  /** Sets the domain. */
  Status set_domain(const void* domain);

  /** Sets the domain. */
  Status set_domain(const Range& domain);

  /** Sets the filter pipeline for this dimension. */
  Status set_filter_pipeline(const FilterPipeline* pipeline);

  /** Sets the tile extent. */
  Status set_tile_extent(const void* tile_extent);

  /** Sets the tile extent. */
  Status set_tile_extent(const ByteVecValue& tile_extent);

  /**
   * If the tile extent is `null`, this function sets the
   * the tile extent to the dimension domain range.
   *
   * @note This is applicable only to dense arrays.
   */
  Status set_null_tile_extent_to_range();

  /**
   * If the tile extent is `null`, this function sets the
   * the tile extent to the dimension domain range.
   *
   * @tparam T The dimension type.
   *
   * @note This is applicable only to dense arrays.
   */
  template <class T>
  Status set_null_tile_extent_to_range();

  /** Returns the tile extent. */
  const ByteVecValue& tile_extent() const;

  /** Returns the dimension type. */
  Datatype type() const;

  /** Returns true if the dimension is var-sized. */
  bool var_size() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The number of values per coordinate. */
  unsigned cell_val_num_;

  /** The dimension domain. */
  Range domain_;

  /** The dimension filter pipeline. */
  FilterPipeline filters_;

  /** The dimension name. */
  std::string name_;

  /** The tile extent of the dimension. */
  ByteVecValue tile_extent_;

  /** The dimension type. */
  Datatype type_;

  /**
   * Stores the appropriate templated ceil_to_tile() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension*, const Range&, uint64_t, ByteVecValue*)>
      ceil_to_tile_func_;

  /**
   * Stores the appropriate templated check_range() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension*, const Range&, std::string*)>
      check_range_func_;

  /**
   * Stores the appropriate templated check_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension*, const Range*)> adjust_range_oob_func_;

  /**
   * Stores the appropriate templated coincides_with_tiles() function based on
   * the dimension datatype.
   */
  std::function<bool(const Dimension*, const Range&)>
      coincides_with_tiles_func_;

  /**
   * Stores the appropriate templated compute_mbr() function based on the
   * dimension datatype.
   */
  std::function<Status(const Tile&, Range*)> compute_mbr_func_;

  /**
   * Stores the appropriate templated compute_mbr_var() function based on the
   * dimension datatype.
   */
  std::function<Status(const Tile&, const Tile&, Range*)> compute_mbr_var_func_;

  /**
   * Stores the appropriate templated crop_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, Range*)> crop_range_func_;

  /**
   * Stores the appropriate templated crop_range() function based on the
   * dimension datatype.
   */
  std::function<uint64_t(const Range&)> domain_range_func_;

  /**
   * Stores the appropriate templated expand_range() function based on the
   * dimension datatype.
   */
  std::function<void(const void*, Range*)> expand_range_v_func_;

  /**
   * Stores the appropriate templated expand_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Range&, Range*)> expand_range_func_;

  /**
   * Stores the appropriate templated expand_to_tile() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, Range*)> expand_to_tile_func_;

  /**
   * Stores the appropriate templated oob() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension* dim, const void*, std::string*)>
      oob_func_;

  /**
   * Stores the appropriate templated covered() function based on the
   * dimension datatype.
   */
  std::function<bool(const Range&, const Range&)> covered_func_;

  /**
   * Stores the appropriate templated overlap() function based on the
   * dimension datatype.
   */
  std::function<bool(const Range&, const Range&)> overlap_func_;

  /**
   * Stores the appropriate templated overlap_ratio() function based on the
   * dimension datatype.
   */
  std::function<double(const Range&, const Range&)> overlap_ratio_func_;

  /**
   * Stores the appropriate templated split_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Range&, const ByteVecValue&, Range*, Range*)>
      split_range_func_;

  /**
   * Stores the appropriate templated splitting_value() function based on the
   * dimension datatype.
   */
  std::function<void(const Range&, ByteVecValue*, bool* unsplittable)>
      splitting_value_func_;

  /**
   * Stores the appropriate templated tile_num() function based on the
   * dimension datatype.
   */
  std::function<uint64_t(const Dimension* dim, const Range&)> tile_num_func_;

  /**
   * Stores the appropriate templated map_to_uint64() function based on
   * the dimension datatype.
   */
  std::function<uint64_t(
      const Dimension*, const QueryBuffer*, uint64_t, uint64_t, int, uint64_t)>
      map_to_uint64_func_;

  /**
   * Stores the appropriate templated map_to_uint64_2() function based on
   * the dimension datatype.
   */
  std::function<uint64_t(
      const Dimension*, const void*, uint64_t, int, uint64_t)>
      map_to_uint64_2_func_;

  /**
   * Stores the appropriate templated map_to_uint64_3() function based on
   * the dimension datatype.
   */
  std::function<uint64_t(
      const Dimension*, const ResultCoords&, uint32_t, int, uint64_t)>
      map_to_uint64_3_func_;

  /**
   * Stores the appropriate templated map_from_uint64() function based on
   * the dimension datatype.
   */
  std::function<ByteVecValue(const Dimension*, uint64_t, int, uint64_t)>
      map_from_uint64_func_;

  /**
   * Stores the appropriate templated smaller_than() function based on
   * the dimension datatype.
   */
  std::function<bool(const Dimension*, const ByteVecValue&, const Range&)>
      smaller_than_func_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Returns an error if the set domain is invalid. */
  Status check_domain() const;

  /**
   * Returns an error if the set domain is invalid.
   * Applicable to integral domains.
   */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  Status check_domain() const {
    assert(!domain_.empty());
    auto domain = (const T*)domain_.data();

    // Upper bound should not be smaller than lower
    if (domain[1] < domain[0])
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Upper domain bound should "
          "not be smaller than the lower one"));

    // Domain range must not exceed the maximum unsigned number
    // for integer domains
    if (domain[0] == std::numeric_limits<T>::min() &&
        domain[1] == std::numeric_limits<T>::max())
      return LOG_STATUS(Status::DimensionError(
          "Domain check failed; Domain range (upper + lower + 1) is larger "
          "than the maximum unsigned number"));

    return Status::Ok();
  }

  /**
   * Returns an error if the set domain is invalid.
   * Applicable to real domains.
   */
  template <
      typename T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  Status check_domain() const {
    assert(!domain_.empty());
    auto domain = (const T*)domain_.data();

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

  /** Returns an error if the set tile extent is invalid. */
  template <class T>
  Status check_tile_extent() const;

  /**
   * Returns an error if the set tile extent exceeds the
   * upper floor.
   */
  template <typename T>
  Status check_tile_extent_upper_floor(const T* domain, T tile_extent) const;

  /**
   * The internal work routine for `check_tile_extent_upper_floor`
   * that accepts a template type for the floor type.
   */
  template <typename T_EXTENT, typename T_FLOOR>
  Status check_tile_extent_upper_floor_internal(
      const T_EXTENT* domain, T_EXTENT tile_extent) const;

  /** Returns the domain in string format. */
  std::string domain_str() const;

  /** Returns the tile extent in string format. */
  std::string tile_extent_str() const;

  /** Sets the templated ceil_to_tile() function. */
  void set_ceil_to_tile_func();

  /** Sets the templated check_range() function. */
  void set_check_range_func();

  /** Set the templated adjust_range_oob_func() function. */
  void set_adjust_range_oob_func();

  /** Sets the templated coincides_with_tiles() function. */
  void set_coincides_with_tiles_func();

  /** Sets the templated compute_mbr() function. */
  void set_compute_mbr_func();

  /** Sets the templated crop_range() function. */
  void set_crop_range_func();

  /** Sets the templated domain_range() function. */
  void set_domain_range_func();

  /** Sets the templated expand_range() function. */
  void set_expand_range_func();

  /** Sets the templated expand_range_v() function. */
  void set_expand_range_v_func();

  /** Sets the templated expand_to_tile() function. */
  void set_expand_to_tile_func();

  /** Sets the templated oob() function. */
  void set_oob_func();

  /** Sets the templated covered() function. */
  void set_covered_func();

  /** Sets the templated overlap() function. */
  void set_overlap_func();

  /** Sets the templated overlap_ratio() function. */
  void set_overlap_ratio_func();

  /** Sets the templated split_range() function. */
  void set_split_range_func();

  /** Sets the templated splitting_value() function. */
  void set_splitting_value_func();

  /** Sets the templated tile_num() function. */
  void set_tile_num_func();

  /** Sets the templated map_to_uint64() function. */
  void set_map_to_uint64_func();

  /** Sets the templated map_to_uint64_2() function. */
  void set_map_to_uint64_2_func();

  /** Sets the templated map_to_uint64_3() function. */
  void set_map_to_uint64_3_func();

  /** Sets the templated map_from_uint64() function. */
  void set_map_from_uint64_func();

  /** Sets the templated smaller_than() function. */
  void set_smaller_than_func();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DIMENSION_H
