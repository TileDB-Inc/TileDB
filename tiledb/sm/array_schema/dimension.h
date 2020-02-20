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

#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/tile/tile.h"

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

  /** Destructor. */
  ~Dimension();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the number of values per cell. */
  unsigned int cell_val_num() const;

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
  void* domain() const;

  /** Dumps the dimension contents in ASCII form in the selected output. */
  void dump(FILE* out) const;

  /** Returns the filter pipeline of this dimension. */
  const FilterPipeline* filters() const;

  /** Returns the dimension name. */
  const std::string& name() const;

  /** Returns true if this is an anonymous (unlabled) dimension **/
  bool is_anonymous() const;

  /**
   * Computed the minimum bounding range of the values stored in
   * `tile`.
   */
  void compute_mbr(const Tile& tile, Range* mbr) const;

  /**
   * Computed the minimum bounding range of the values stored in
   * `tile`.
   */
  template <class T>
  static void compute_mbr(const Dimension* dim, const Tile& tile, Range* mbr);

  /**
   * Crops the input 1D range such that it does not exceed the
   * dimension domain.
   */
  void crop_range(Range* range) const;

  /**
   * Crops the input 1D range such that it does not exceed the
   * input dimension domain.
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
   * 1D range. It returns 0 in case the dimension datatype
   * is not integer or if there is an overflow.
   */
  template <class T>
  static uint64_t domain_range(const Dimension* dim, const Range& range);

  /** Expand 1D range `r` using value `v`. */
  void expand_range_v(const void* v, Range* r) const;

  /** Expand 1D range `r` using value `v`. */
  template <class T>
  static void expand_range_v(const Dimension* dim, const void* v, Range* r);

  /** Expand 1D range `r2` using 1D range `r1`. */
  void expand_range(const Range& r1, Range* r2) const;

  /** Expand 1D range `r2` using 1D range `r1`. */
  template <class T>
  static void expand_range(const Dimension* dim, const Range& r1, Range* r2);

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

  /** Return true if r1 is fully covered by r2. */
  bool covered(const Range& r1, const Range& r2) const;

  /** Return true if r1 is fully covered by r2. */
  template <class T>
  static bool covered(const Dimension* dim, const Range& r1, const Range& r2);

  /** Return true if the input 1D ranges overlap. */
  bool overlap(const Range& r1, const Range& r2) const;

  /** Return true if the input 1D ranges overlap. */
  template <class T>
  static bool overlap(const Dimension* dim, const Range& r1, const Range& r2);

  /** Return ratio of the overalp of the two input 1D ranges over `r2`. */
  double overlap_ratio(const Range& r1, const Range& r2) const;

  /** Return ratio of the overalp of the two input 1D ranges over `r2`. */
  template <class T>
  static double overlap_ratio(
      const Dimension* dim, const Range& r1, const Range& r2);

  /** Return the number of tiles the input range intersects. */
  uint64_t tile_num(const Range& range) const;

  /** Return the number of tiles the input range intersects. */
  template <class T>
  static uint64_t tile_num(const Dimension* dim, const Range& range);

  /** Returns `true` if `value` is within the 1D `range`. */
  template <class T>
  static bool value_in_range(
      const Dimension* dim, const void* value, const Range& range);

  /** Returns `true` if `value` is within the 1D `range`. */
  bool value_in_range(const void* value, const Range& range) const;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  Status serialize(Buffer* buff);

  /** Sets the domain. */
  Status set_domain(const void* domain);

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
  void* tile_extent() const;

  /** Returns the dimension type. */
  Datatype type() const;

  /** Returns true if the dimension is var-sized. */
  bool var_size() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The dimension domain. */
  void* domain_;

  /** The dimension name. */
  std::string name_;

  /** The tile extent of the dimension. */
  void* tile_extent_;

  /** The dimension type. */
  Datatype type_;

  /**
   * Stores the appropriate templated compute_mbr() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, const Tile&, Range*)>
      compute_mbr_func_;

  /**
   * Stores the appropriate templated crop_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, Range*)> crop_range_func_;

  /**
   * Stores the appropriate templated crop_range() function based on the
   * dimension datatype.
   */
  std::function<uint64_t(const Dimension* dim, const Range&)>
      domain_range_func_;

  /**
   * Stores the appropriate templated expand_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, const void*, Range*)>
      expand_range_v_func_;

  /**
   * Stores the appropriate templated expand_range() function based on the
   * dimension datatype.
   */
  std::function<void(const Dimension* dim, const Range&, Range*)>
      expand_range_func_;

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
  std::function<bool(const Dimension* dim, const Range&, const Range&)>
      covered_func_;

  /**
   * Stores the appropriate templated overlap() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension* dim, const Range&, const Range&)>
      overlap_func_;

  /**
   * Stores the appropriate templated overlap_ratio() function based on the
   * dimension datatype.
   */
  std::function<double(const Dimension* dim, const Range&, const Range&)>
      overlap_ratio_func_;

  /**
   * Stores the appropriate templated tile_num() function based on the
   * dimension datatype.
   */
  std::function<uint64_t(const Dimension* dim, const Range&)> tile_num_func_;

  /**
   * Stores the appropriate templated value_in_range() function based on the
   * dimension datatype.
   */
  std::function<bool(const Dimension* dim, const void*, const Range&)>
      value_in_range_func_;

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
    assert(domain_ != nullptr);
    auto domain = static_cast<const T*>(domain_);

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
    assert(domain_ != nullptr);
    auto domain = static_cast<const T*>(domain_);

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

  /** Sets the templated tile_num() function. */
  void set_tile_num_func();

  /** Sets the templated value_in_range() function. */
  void set_value_in_range_func();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DIMENSION_H
