/**
 * @file   tiledb_cpp_api_attribute.h
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
 * This file declares the C++ API for the TileDB Attribute object.
 */

#ifndef TILEDB_CPP_API_ATTRIBUTE_H
#define TILEDB_CPP_API_ATTRIBUTE_H

#include "compressor.h"
#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "object.h"
#include "tiledb.h"
#include "type.h"

#include <array>
#include <functional>
#include <memory>
#include <type_traits>

namespace tiledb {

/**
 * Describes an attribute of an Array cell.
 *
 * @details
 * An attribute specifies a datatype for a particular parameter in each
 * array cell. There are 3 supported attribute types:
 *
 * - Fundamental types, such as char, int, double, uint64_t, etc..
 * - Fixed sized arrays: std::array<T, N> where T is fundamental
 * - Variable length data: std::string, std::vector<T> where T is fundamental
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
 * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
 * auto a3 = tiledb::Attribute::create<std::array<float, 3>>(ctx, "a3");
 *
 * // Change compression scheme
 * a1.set_compressor({TILEDB_BLOSC, -1});
 * a1.cell_val_num(); // 1
 * a2.cell_val_num(); // Variable sized, TILEDB_VAR_NUM
 * a3.cell_val_num(); // sizeof(std::array<float, 3>), Objects stored as char
 * array a3.cell_size(); // same as cell_val_num since type is char
 * a3.type_size(); // sizeof(char)
 *
 * tiledb::ArraySchema schema(ctx);
 * schema.add_attributes(a1, a2, a3);
 * @endcode
 */
class Attribute {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Attribute(const Context& ctx, tiledb_attribute_t* attr)
      : ctx_(ctx)
      , deleter_(ctx) {
    attr_ = std::shared_ptr<tiledb_attribute_t>(attr, deleter_);
  }
  Attribute(const Attribute& attr) = default;
  Attribute(Attribute&& o) = default;
  Attribute& operator=(const Attribute&) = default;
  Attribute& operator=(Attribute&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the name of the attribute. */
  std::string name() const {
    auto& ctx = ctx_.get();
    const char* name;
    ctx.handle_error(tiledb_attribute_get_name(ctx, attr_.get(), &name));
    return name;
  }

  /** Returns the attribute datatype. */
  tiledb_datatype_t type() const {
    auto& ctx = ctx_.get();
    tiledb_datatype_t type;
    ctx.handle_error(tiledb_attribute_get_type(ctx, attr_.get(), &type));
    return type;
  }

  /** Returns the size (in bytes) of one cell on this attribute. */
  uint64_t cell_size() const {
    auto& ctx = ctx_.get();
    uint64_t cell_size;
    ctx.handle_error(
        tiledb_attribute_get_cell_size(ctx, attr_.get(), &cell_size));
    return cell_size;
  }

  /**
   * Returns the number of values stored in each cell.
   * This is equal to the size of the attribute * sizeof(attr_type).
   * For variable size attributes this is TILEDB_VAR_NUM.
   */
  unsigned cell_val_num() const {
    auto& ctx = ctx_.get();
    unsigned num;
    ctx.handle_error(tiledb_attribute_get_cell_val_num(ctx, attr_.get(), &num));
    return num;
  }

  /**
   * Sets the number of attribute values per cell. This is
   * inferred from from the attribute constructor, but can
   * also be set manually. E.x. a1 and a2 are equivilant:
   *
   * a1 = Attribute::create<std::vector<int>>(...);
   * a2 = Attribute::create<int>(...);
   * a2.set_cel_val_num(TILEDB_VAR_NUM);
   **/
  Attribute& set_cell_val_num(unsigned num) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_set_cell_val_num(ctx, attr_.get(), num));
    return *this;
  }

  /** Check if attribute is variable sized. **/
  bool variable_sized() const {
    return cell_val_num() == TILEDB_VAR_NUM;
  }

  /** Returns the attribute compressor. */
  Compressor compressor() const {
    auto& ctx = ctx_.get();
    tiledb_compressor_t compressor;
    int level;
    ctx.handle_error(
        tiledb_attribute_get_compressor(ctx, attr_.get(), &compressor, &level));
    Compressor cmp(compressor, level);
    return cmp;
  }

  /** Sets the attribute compressor. */
  Attribute& set_compressor(Compressor c) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_set_compressor(
        ctx, attr_.get(), c.compressor(), c.level()));
    return *this;
  }

  /** Returns the C TileDB attribute object pointer. */
  std::shared_ptr<tiledb_attribute_t> ptr() const {
    return attr_;
  }

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_attribute_t*() const {
    return attr_.get();
  }

  /** Dump information about the attribute to a FILE. **/
  void dump(FILE* out = stdout) const {
    ctx_.get().handle_error(
        tiledb_attribute_dump(ctx_.get(), attr_.get(), out));
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Factory function for creating a new attribute with datatype T.
   *
   * @tparam T Datatype of the attribute. Can either be arithmetic type,
   *         C-style array, std::string, std::vector, or any trivially
   *         copyable classes (defined by std::is_trivially_copyable).
   * @param ctx The TileDB context.
   * @param name The attribute name.
   * @return A new Attribute object.
   */
  template <typename T>
  static Attribute create(const Context& ctx, const std::string& name) {
    using DataT = typename impl::TypeHandler<T>;
    auto a = create(ctx, name, DataT::tiledb_type);
    a.set_cell_val_num(DataT::tiledb_num);
    return a;
  }

  /**
   * Factory function for creating a new attribute with datatype T.
   *
   * @tparam T Datatype of the attribute. Can either be arithmetic type,
   *         C-style array, std::string, std::vector, or any trivially
   *         copyable classes (defined by std::is_trivially_copyable).
   * @param ctx The TileDB context.
   * @param name The attribute name.
   * @param compressor Compressor to use for attribute
   * @return A new Attribute object.
   */
  template <typename T>
  static Attribute create(
      const Context& ctx,
      const std::string& name,
      const Compressor& compressor) {
    auto a = create<T>(ctx, name);
    a.set_compressor(compressor);
    return a;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** An auxiliary deleter. */
  impl::Deleter deleter_;

  /** The pointer to the C TileDB attribute object. */
  std::shared_ptr<tiledb_attribute_t> attr_;

  /* ********************************* */
  /*         PRIVATE FUNCTIONS         */
  /* ********************************* */

  /** Creates an attribute with the input name and datatype. */
  static Attribute create(
      const Context& ctx, const std::string& name, tiledb_datatype_t type) {
    tiledb_attribute_t* attr;
    ctx.handle_error(tiledb_attribute_create(ctx, &attr, name.c_str(), type));
    return Attribute(ctx, attr);
  }
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Gets a string representation of an attribute for an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Attribute& a) {
  os << "Attr<" << a.name() << ',' << tiledb::impl::to_str(a.type()) << ','
     << (a.cell_val_num() == TILEDB_VAR_NUM ? "VAR" :
                                              std::to_string(a.cell_val_num()))
     << '>';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ATTRIBUTE_H
