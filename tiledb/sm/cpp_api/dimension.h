/**
 * @file   tiledb_cpp_api_dimension.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Dimension object.
 */

#ifndef TILEDB_CPP_API_DIMENSION_H
#define TILEDB_CPP_API_DIMENSION_H

#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "object.h"
#include "tiledb.h"
#include "type.h"

#include <functional>
#include <memory>
#include <sstream>

namespace tiledb {

/**
 * Describes one dimension of an Array. The dimension consists
 * of a type, lower and upper bound, and tile-extent describing
 * the memory ordering. Dimensions are added to a Domain.
 *
 * **Example:**
 * @code{.cpp}
 * tiledb::Context ctx;
 * tiledb::Domain domain(ctx);
 * // Create a dimension with inclusive domain [0,1000] and tile extent 100.
 * domain.add_dimension(Dimension::create<int32_t>(ctx, "d", {{0, 1000}}, 100));
 * @endcode
 **/
class Dimension {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Dimension(const Context& ctx, tiledb_dimension_t* dim)
      : ctx_(ctx) {
    dim_ = std::shared_ptr<tiledb_dimension_t>(dim, deleter_);
  }

  Dimension(const Dimension&) = default;
  Dimension(Dimension&&) = default;
  Dimension& operator=(const Dimension&) = default;
  Dimension& operator=(Dimension&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the name of the dimension. */
  const std::string name() const {
    const char* name;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dimension_get_name(ctx, dim_.get(), &name));
    return name;
  }

  /** Returns the dimension datatype. */
  tiledb_datatype_t type() const {
    tiledb_datatype_t type;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dimension_get_type(ctx, dim_.get(), &type));
    return type;
  }

  /**
   * Returns the domain of the dimension.
   *
   * @tparam T Domain datatype
   * @return Pair of [lower, upper] inclusive bounds.
   */
  template <typename T>
  std::pair<T, T> domain() const {
    impl::type_check<T>(type(), 1);
    auto d = static_cast<T*>(_domain());
    return std::pair<T, T>(d[0], d[1]);
  };

  /**
   * Returns a string representation of the domain.
   * @throws TileDBError if the domain cannot be stringified (TILEDB_ANY)
   */
  std::string domain_to_str() const {
    auto domain = _domain();
    auto type = this->type();
    int8_t* di8;
    uint8_t* dui8;
    int16_t* di16;
    uint16_t* dui16;
    int32_t* di32;
    uint32_t* dui32;
    int64_t* di64;
    uint64_t* dui64;
    float* df32;
    double* df64;

    std::stringstream ss;
    ss << "[";

    switch (type) {
      case TILEDB_INT8:
        di8 = static_cast<int8_t*>(domain);
        ss << di8[0] << "," << di8[1];
        break;
      case TILEDB_UINT8:
        dui8 = static_cast<uint8_t*>(domain);
        ss << dui8[0] << "," << dui8[1];
        break;
      case TILEDB_INT16:
        di16 = static_cast<int16_t*>(domain);
        ss << di16[0] << "," << di16[1];
        break;
      case TILEDB_UINT16:
        dui16 = static_cast<uint16_t*>(domain);
        ss << dui16[0] << "," << dui16[1];
        break;
      case TILEDB_INT32:
        di32 = static_cast<int32_t*>(domain);
        ss << di32[0] << "," << di32[1];
        break;
      case TILEDB_UINT32:
        dui32 = static_cast<uint32_t*>(domain);
        ss << dui32[0] << "," << dui32[1];
        break;
      case TILEDB_INT64:
        di64 = static_cast<int64_t*>(domain);
        ss << di64[0] << "," << di64[1];
        break;
      case TILEDB_UINT64:
        dui64 = static_cast<uint64_t*>(domain);
        ss << dui64[0] << "," << dui64[1];
        break;
      case TILEDB_FLOAT32:
        df32 = static_cast<float*>(domain);
        ss << df32[0] << "," << df32[1];
        break;
      case TILEDB_FLOAT64:
        df64 = static_cast<double*>(domain);
        ss << df64[0] << "," << df64[1];
        break;
      case TILEDB_CHAR:
      case TILEDB_STRING_ASCII:
      case TILEDB_STRING_UTF8:
      case TILEDB_STRING_UTF16:
      case TILEDB_STRING_UTF32:
      case TILEDB_STRING_UCS2:
      case TILEDB_STRING_UCS4:
      case TILEDB_ANY:
        // Not supported domain types
        throw TileDBError("Invalid Dim type");
    }

    ss << "]";

    return ss.str();
  }

  /** Returns the tile extent of the dimension. */
  template <typename T>
  T tile_extent() const {
    impl::type_check<T>(type(), 1);
    return *static_cast<T*>(_tile_extent());
  }

  /**
   * Returns a string representation of the extent.
   * @throws TileDBError if the domain cannot be stringified (TILEDB_ANY)
   */
  std::string tile_extent_to_str() const {
    auto tile_extent = _tile_extent();
    auto type = this->type();
    int8_t* ti8;
    uint8_t* tui8;
    int16_t* ti16;
    uint16_t* tui16;
    int32_t* ti32;
    uint32_t* tui32;
    int64_t* ti64;
    uint64_t* tui64;
    float* tf32;
    double* tf64;

    std::stringstream ss;

    switch (type) {
      case TILEDB_INT8:
        ti8 = static_cast<int8_t*>(tile_extent);
        ss << *ti8;
        break;
      case TILEDB_UINT8:
        tui8 = static_cast<uint8_t*>(tile_extent);
        ss << *tui8;
        break;
      case TILEDB_INT16:
        ti16 = static_cast<int16_t*>(tile_extent);
        ss << *ti16;
        break;
      case TILEDB_UINT16:
        tui16 = static_cast<uint16_t*>(tile_extent);
        ss << *tui16;
        break;
      case TILEDB_INT32:
        ti32 = static_cast<int32_t*>(tile_extent);
        ss << *ti32;
        break;
      case TILEDB_UINT32:
        tui32 = static_cast<uint32_t*>(tile_extent);
        ss << *tui32;
        break;
      case TILEDB_INT64:
        ti64 = static_cast<int64_t*>(tile_extent);
        ss << *ti64;
        break;
      case TILEDB_UINT64:
        tui64 = static_cast<uint64_t*>(tile_extent);
        ss << *tui64;
        break;
      case TILEDB_FLOAT32:
        tf32 = static_cast<float*>(tile_extent);
        ss << *tf32;
        break;
      case TILEDB_FLOAT64:
        tf64 = static_cast<double*>(tile_extent);
        ss << *tf64;
        break;
      case TILEDB_CHAR:
      case TILEDB_STRING_ASCII:
      case TILEDB_STRING_UTF8:
      case TILEDB_STRING_UTF16:
      case TILEDB_STRING_UTF32:
      case TILEDB_STRING_UCS2:
      case TILEDB_STRING_UCS4:
      case TILEDB_ANY:
        throw TileDBError("Invalid Dim type");
    }

    return ss.str();
  }

  /** Returns a shared pointer to the C TileDB dimension object. */
  std::shared_ptr<tiledb_dimension_t> ptr() const {
    return dim_;
  }

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_dimension_t*() const {
    return dim_.get();
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Factory function for creating a new dimension with datatype T.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * // Create a dimension with inclusive domain [0,1000] and tile extent 100.
   * auto dim = Dimension::create<int32_t>(ctx, "d", {{0, 1000}}, 100);
   * @endcode
   *
   * @tparam T int, char, etc...
   * @param ctx The TileDB context.
   * @param name The dimension name.
   * @param domain The dimension domain. A pair [lower,upper] of inclusive
   * bounds.
   * @param extent The tile extent on the dimension.
   * @return A new `Dimension` object.
   */
  template <typename T>
  static Dimension create(
      const Context& ctx,
      const std::string& name,
      const std::array<T, 2>& domain,
      T extent) {
    using DataT = impl::TypeHandler<T>;
    static_assert(
        DataT::tiledb_num == 1,
        "Dimension types cannot be compound, use arithmetic type.");
    return create_impl(ctx, name, DataT::tiledb_type, &domain, &extent);
  }

  /**
   * Factory function for creating a new dimension with datatype T with no
   * tile extent. TileDB uses a tile extent equal to the whole domain extent in
   * this dimension.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * // Create a dimension with inclusive domain [0,1000] and implicit
   * // tile extent 1001.
   * auto dim = Dimension::create<int32_t>(ctx, "d", {{0, 1000}});
   * @endcode
   *
   * @tparam T int, char, etc...
   * @param ctx The TileDB context.
   * @param name The dimension name.
   * @param domain The dimension domain. A pair [lower,upper] of inclusive
   * bounds.
   * @return A new `Dimension` object.
   */
  template <typename T>
  static Dimension create(
      const Context& ctx,
      const std::string& name,
      const std::array<T, 2>& domain) {
    using DataT = impl::TypeHandler<T>;
    static_assert(
        DataT::tiledb_num == 1,
        "Dimension types cannot be compound, use arithmetic type.");
    return create_impl(ctx, name, DataT::tiledb_type, &domain, nullptr);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** A deleter wrapper. */
  impl::Deleter deleter_;

  /** The C TileDB dimension object. */
  std::shared_ptr<tiledb_dimension_t> dim_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Returns the binary representation of the dimension domain. */
  void* _domain() const {
    auto& ctx = ctx_.get();
    void* domain;
    ctx.handle_error(tiledb_dimension_get_domain(ctx, dim_.get(), &domain));
    return domain;
  }

  /** Returns the binary representation of the dimension extent. */
  void* _tile_extent() const {
    void* tile_extent;
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_dimension_get_tile_extent(ctx, dim_.get(), &tile_extent));
    return tile_extent;
  }

  /* ********************************* */
  /*     PRIVATE STATIC FUNCTIONS      */
  /* ********************************* */

  /**
   * Creates a dimension with the input name, datatype, domain and tile
   * extent.
   */
  static Dimension create_impl(
      const Context& ctx,
      const std::string& name,
      tiledb_datatype_t type,
      const void* domain,
      const void* tile_extent) {
    tiledb_dimension_t* d;
    ctx.handle_error(tiledb_dimension_alloc(
        ctx, name.c_str(), type, domain, tile_extent, &d));
    return Dimension(ctx, d);
  }
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of a dimension for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Dimension& dim) {
  os << "Dim<" << dim.name() << "," << dim.domain_to_str() << ","
     << dim.tile_extent_to_str() << ">";
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_DIMENSION_H
