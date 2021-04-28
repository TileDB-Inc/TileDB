/**
 * @file   domain.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for the TileDB Domain.
 */

#ifndef TILEDB_CPP_API_DOMAIN_H
#define TILEDB_CPP_API_DOMAIN_H

#include "context.h"
#include "deleter.h"
#include "dimension.h"
#include "tiledb.h"
#include "type.h"

#include <functional>
#include <memory>
#include <type_traits>

namespace tiledb {

/**
 * Represents the domain of an array.
 *
 * @details
 * A Domain defines the set of Dimension objects for a given array. The
 * properties of a Domain derive from the underlying dimensions. A
 * Domain is a component of an ArraySchema.
 *
 * @note The dimension can only be signed or unsigned integral types,
 * as well as floating point for sparse array domains.
 *
 * **Example:**
 *
 * @code{.cpp}
 *
 * tiledb::Context ctx;
 * tiledb::Domain domain;
 *
 * // Note the dimension bounds are inclusive.
 * auto d1 = tiledb::Dimension::create<int>(ctx, "d1", {-10, 10});
 * auto d2 = tiledb::Dimension::create<uint64_t>(ctx, "d2", {1, 10});
 * auto d3 = tiledb::Dimension::create<int>(ctx, "d3", {-100, 100});
 *
 * domain.add_dimension(d1);
 * domain.add_dimension(d2); // Throws error, all dims must be same type
 * domain.add_dimension(d3);
 *
 * domain.cell_num(); // (10 - -10 + 1) * (10 - 1 + 1) = 210 max cells
 * domain.type(); // TILEDB_INT32, determined from the dimensions
 * domain.rank(); // 2, d1 and d2
 *
 * tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
 * schema.set_domain(domain); // Set the array's domain
 *
 * @endcode
 *
 **/
class Domain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Domain(const Context& ctx)
      : ctx_(ctx) {
    tiledb_domain_t* domain;
    ctx.handle_error(tiledb_domain_alloc(ctx.ptr().get(), &domain));
    domain_ = std::shared_ptr<tiledb_domain_t>(domain, deleter_);
  }

  Domain(const Context& ctx, tiledb_domain_t* domain)
      : ctx_(ctx) {
    domain_ = std::shared_ptr<tiledb_domain_t>(domain, deleter_);
  }

  Domain(const Domain&) = default;
  Domain(Domain&&) = default;
  Domain& operator=(const Domain&) = default;
  Domain& operator=(Domain&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns the total number of cells in the domain. Throws an exception
   * if the domain type is `float32` or `float64`.
   * @throws TileDBError if cell_num cannot be computed.
   */
  uint64_t cell_num() const {
    auto type = this->type();
    if (type == TILEDB_FLOAT32 || type == TILEDB_FLOAT64)
      throw TileDBError(
          "[TileDB::C++API::Domain] Cannot compute number of cells for a "
          "non-integer domain");

    switch (type) {
      case TILEDB_INT8:
        return cell_num<int8_t>();
      case TILEDB_UINT8:
        return cell_num<uint8_t>();
      case TILEDB_INT16:
        return cell_num<int16_t>();
      case TILEDB_UINT16:
        return cell_num<uint16_t>();
      case TILEDB_INT32:
        return cell_num<int32_t>();
      case TILEDB_UINT32:
        return cell_num<uint32_t>();
      case TILEDB_INT64:
        return cell_num<int64_t>();
      case TILEDB_UINT64:
        return cell_num<uint64_t>();
      default:
        throw TileDBError(
            "[TileDB::C++API::Domain] Cannot compute number of cells; Unknown "
            "domain type");
    }

    return 0;
  }

  /**
   * Dumps the domain in an ASCII representation to an output.
   *
   * @param out (Optional) File to dump output to. Defaults to `nullptr`
   * which will lead to selection of `stdout`.
   */
  void dump(FILE* out = nullptr) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_domain_dump(ctx.ptr().get(), domain_.get(), out));
  }

  /** Returns the domain type. */
  tiledb_datatype_t type() const {
    auto& ctx = ctx_.get();
    tiledb_datatype_t type;
    ctx.handle_error(
        tiledb_domain_get_type(ctx.ptr().get(), domain_.get(), &type));
    return type;
  }

  /** Returns the number of dimensions. */
  unsigned ndim() const {
    auto& ctx = ctx_.get();
    unsigned n;
    ctx.handle_error(
        tiledb_domain_get_ndim(ctx.ptr().get(), domain_.get(), &n));
    return n;
  }

  /** Returns the current set of dimensions in the domain. */
  std::vector<Dimension> dimensions() const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    unsigned int ndim;
    tiledb_dimension_t* dimptr;
    std::vector<Dimension> dims;
    ctx.handle_error(tiledb_domain_get_ndim(c_ctx, domain_.get(), &ndim));
    for (unsigned int i = 0; i < ndim; ++i) {
      ctx.handle_error(tiledb_domain_get_dimension_from_index(
          c_ctx, domain_.get(), i, &dimptr));
      dims.emplace_back(Dimension(ctx, dimptr));
    }
    return dims;
  }

  /** Returns the dimensions with the given index. */
  Dimension dimension(unsigned idx) const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    tiledb_dimension_t* dimptr;
    ctx.handle_error(tiledb_domain_get_dimension_from_index(
        c_ctx, domain_.get(), idx, &dimptr));
    return Dimension(ctx, dimptr);
  }

  /** Returns the dimensions with the given name. */
  Dimension dimension(const std::string& name) const {
    auto& ctx = ctx_.get();
    tiledb_ctx_t* c_ctx = ctx.ptr().get();
    tiledb_dimension_t* dimptr;
    ctx.handle_error(tiledb_domain_get_dimension_from_name(
        c_ctx, domain_.get(), name.c_str(), &dimptr));
    return Dimension(ctx, dimptr);
  }

  /**
   * Adds a new dimension to the domain.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Domain domain;
   * auto d1 = tiledb::Dimension::create<int>(ctx, "d1", {-10, 10});
   * domain.add_dimension(d1);
   * @endcode
   *
   * @param d Dimension to add
   * @return Reference to this Domain
   */
  Domain& add_dimension(const Dimension& d) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_domain_add_dimension(
        ctx.ptr().get(), domain_.get(), d.ptr().get()));
    return *this;
  }

  /**
   * Adds multiple dimensions to the domain.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::Domain domain;
   * auto d1 = tiledb::Dimension::create<int>(ctx, "d1", {-10, 10});
   * auto d2 = tiledb::Dimension::create<int>(ctx, "d2", {1, 10});
   * auto d3 = tiledb::Dimension::create<int>(ctx, "d3", {-100, 100});
   * domain.add_dimensions(d1, d2, d3);
   * @endcode
   *
   * @tparam Args Variadic dimension datatype
   * @param dims Dimensions to add
   * @return Reference to this Domain.
   */
  template <typename... Args>
  Domain& add_dimensions(Args... dims) {
    for (const auto& attr : {dims...}) {
      add_dimension(attr);
    }
    return *this;
  }

  /**
   * Checks if the domain has a dimension of the given name.
   *
   * @param name Name of dimension to check for
   * @return True if the domain has a dimension of the given name.
   */
  bool has_dimension(const std::string& name) const {
    auto& ctx = ctx_.get();
    int32_t has_dim;
    ctx.handle_error(tiledb_domain_has_dimension(
        ctx.ptr().get(), domain_.get(), name.c_str(), &has_dim));
    return has_dim == 1;
  }

  /** Returns a shared pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_domain_t> ptr() const {
    return domain_;
  }

 private:
  /**
   * Returns the total number of cells in the domain.
   *
   * @tparam T The domain datatype.
   */
  template <class T>
  uint64_t cell_num() const {
    uint64_t ret = 1;
    auto dimensions = this->dimensions();
    for (const auto& dim : dimensions) {
      const auto& d = dim.domain<T>();
      ret *= (d.second - d.first + 1);
    }

    return ret;
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the C TileDB domain object. */
  std::shared_ptr<tiledb_domain_t> domain_;
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Get a string representation of the domain for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Domain& d) {
  os << "Domain<";
  for (const auto& dimension : d.dimensions()) {
    os << " " << dimension;
  }
  os << '>';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_DOMAIN_H
