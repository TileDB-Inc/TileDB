/**
 * @file   attribute.h
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
 * This file declares the C++ API for the TileDB Attribute object.
 */

#ifndef TILEDB_CPP_API_ATTRIBUTE_H
#define TILEDB_CPP_API_ATTRIBUTE_H

#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "filter_list.h"
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
 * An attribute specifies a name and datatype for a particular value in each
 * array cell. There are 3 supported attribute types:
 *
 * - Fundamental types, such as `char`, `int`, `double`, `uint64_t`, etc..
 * - Fixed sized arrays: `T[N]` or `std::array<T, N>`, where T is a fundamental
 * type
 * - Variable length data: `std::string`, `std::vector<T>` where T is a
 * fundamental type
 *
 * Fixed-size array types using POD types like `std::array<T, N>` are internally
 * converted to byte-array attributes. E.g. an attribute of type
 * `std::array<float, 3>` will be created as an attribute of type `TILEDB_CHAR`
 * with cell_val_num `sizeof(std::array<float, 3>)`.
 *
 * Therefore, for fixed-length attributes it is recommended to use C-style
 * arrays instead, e.g. `float[3]` instead of `std::array<float, 3>`.
 *
 * **Example:**
 *
 * @code{.cpp}
 * tiledb::Context ctx;
 * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
 * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
 * auto a3 = tiledb::Attribute::create<float[3]>(ctx, "a3");
 *
 * // Change compression scheme
 * tiledb::FilterList filters(ctx);
 * filters.add_filter({ctx, TILEDB_FILTER_BZIP2});
 * a1.set_filter_list(filters);
 *
 * // Add attributes to a schema
 * tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
 * schema.add_attributes(a1, a2, a3);
 * @endcode
 */
class Attribute {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Attribute(const Context& ctx, tiledb_attribute_t* attr)
      : ctx_(ctx) {
    attr_ = std::shared_ptr<tiledb_attribute_t>(attr, deleter_);
  }

  /**
   * Construct an attribute with a name and enumerated type. `cell_val_num` will
   * be set to 1.
   *
   * @param ctx TileDB context
   * @param name Name of attribute
   * @param type Enumerated type of attribute
   */
  Attribute(const Context& ctx, const std::string& name, tiledb_datatype_t type)
      : ctx_(ctx) {
    init_from_type(name, type);
  }

  /** Construct an attribute with an enumerated type and given filter list. */
  Attribute(
      const Context& ctx,
      const std::string& name,
      tiledb_datatype_t type,
      const FilterList& filter_list)
      : ctx_(ctx) {
    init_from_type(name, type);
    set_filter_list(filter_list);
  }

  Attribute(const Attribute&) = default;
  Attribute(Attribute&&) = default;
  Attribute& operator=(const Attribute&) = default;
  Attribute& operator=(Attribute&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the name of the attribute. */
  std::string name() const {
    auto& ctx = ctx_.get();
    const char* name;
    ctx.handle_error(
        tiledb_attribute_get_name(ctx.ptr().get(), attr_.get(), &name));
    return name;
  }

  /** Returns the attribute datatype. */
  tiledb_datatype_t type() const {
    auto& ctx = ctx_.get();
    tiledb_datatype_t type;
    ctx.handle_error(
        tiledb_attribute_get_type(ctx.ptr().get(), attr_.get(), &type));
    return type;
  }

  /**
   * Returns the size (in bytes) of one cell on this attribute. For
   * variable-sized attributes returns TILEDB_VAR_NUM.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
   * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
   * auto a3 = tiledb::Attribute::create<float[3]>(ctx, "a3");
   * auto a4 = tiledb::Attribute::create<std::array<float, 3>>(ctx, "a4");
   * a1.cell_size();    // Returns sizeof(int)
   * a2.cell_size();    // Variable sized attribute, returns TILEDB_VAR_NUM
   * a3.cell_size();    // Returns 3 * sizeof(float)
   * a4.cell_size();    // Stored as byte array, returns sizeof(char).
   * @endcode
   */
  uint64_t cell_size() const {
    auto& ctx = ctx_.get();
    uint64_t cell_size;
    ctx.handle_error(tiledb_attribute_get_cell_size(
        ctx.ptr().get(), attr_.get(), &cell_size));
    return cell_size;
  }

  /**
   * Returns number of values of one cell on this attribute. For variable-sized
   * attributes returns TILEDB_VAR_NUM.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
   * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
   * auto a3 = tiledb::Attribute::create<float[3]>(ctx, "a3");
   * auto a4 = tiledb::Attribute::create<std::array<float, 3>>(ctx, "a4");
   * a1.cell_val_num();   // Returns 1
   * a2.cell_val_num();   // Variable sized attribute, returns TILEDB_VAR_NUM
   * a3.cell_val_num();   // Returns 3
   * a4.cell_val_num();   // Stored as byte array, returns
   *                         sizeof(std::array<float, 3>).
   * @endcode
   */
  unsigned cell_val_num() const {
    auto& ctx = ctx_.get();
    unsigned num;
    ctx.handle_error(
        tiledb_attribute_get_cell_val_num(ctx.ptr().get(), attr_.get(), &num));
    return num;
  }

  /**
   * Sets the number of attribute values per cell. This is inferred from
   * the type parameter of the `Attribute::create<T>()` function, but can also
   * be set manually.
   *
   * **Example:**
   * @code{.cpp}
   * // a1 and a2 are equivalent:
   * auto a1 = Attribute::create<std::vector<int>>(...);
   * auto a2 = Attribute::create<int>(...);
   * a2.set_cell_val_num(TILEDB_VAR_NUM);
   * @endcode
   *
   * @param num Cell val number to set.
   * @return Reference to this Attribute
   */
  Attribute& set_cell_val_num(unsigned num) {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_attribute_set_cell_val_num(ctx.ptr().get(), attr_.get(), num));
    return *this;
  }

  /**
   * Sets the default fill value for the input attribute. This value will
   * be used for the input attribute whenever querying (1) an empty cell in
   * a dense array, or (2) a non-empty cell (in either dense or sparse array)
   * when values on the input attribute are missing (e.g., if the user writes
   * a subset of the attributes in a write operation).
   *
   * Applicable to var-sized attributes.
   *
   * **Example:**
   *
   * @code{.c}
   * tiledb::Context ctx;
   *
   * // Fixed-sized attribute
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
   * int32_t value = 0;
   * uint64_t size = sizeof(value);
   * a1.set_fill_value(&value, size);
   *
   * // Var-sized attribute
   * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
   * std::string value("null");
   * a2.set_fill_value(value.c_str(), value.size());
   * @endcode
   *
   * @param value The fill value to set.
   * @param size The fill value size in bytes.
   *
   * @note A call to `cell_val_num` sets the fill value
   *     of the attribute to its default. Therefore, make sure you invoke
   *     `set_fill_value` after deciding on the number
   *     of values this attribute will hold in each cell.
   *
   * @note For fixed-sized attributes, the input `size` should be equal
   *     to the cell size.
   */
  Attribute& set_fill_value(const void* value, uint64_t size) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_set_fill_value(
        ctx.ptr().get(), attr_.get(), value, size));
    return *this;
  }

  /**
   * Gets the default fill value for the input attribute. This value will
   * be used for the input attribute whenever querying (1) an empty cell in
   * a dense array, or (2) a non-empty cell (in either dense or sparse array)
   * when values on the input attribute are missing (e.g., if the user writes
   * a subset of the attributes in a write operation).
   *
   * Applicable to both fixed-sized and var-sized attributes.
   *
   * **Example:**
   *
   * @code{.c}
   * // Fixed-sized attribute
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
   * const int32_t* value;
   * uint64_t size;
   * a1.get_fill_value(&value, &size);
   *
   * // Var-sized attribute
   * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
   * const char* value;
   * uint64_t size;
   * a2.get_fill_value(&value, &size);
   * @endcode
   *
   * @param value A pointer to the fill value to get.
   * @param size The size of the fill value to get.
   */
  void get_fill_value(const void** value, uint64_t* size) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_get_fill_value(
        ctx.ptr().get(), attr_.get(), value, size));
  }

  /**
   * Sets the default fill value for the input, nullable attribute. This value
   * will be used for the input attribute whenever querying (1) an empty cell in
   * a dense array, or (2) a non-empty cell (in either dense or sparse array)
   * when values on the input attribute are missing (e.g., if the user writes
   * a subset of the attributes in a write operation).
   *
   * Applicable to var-sized attributes.
   *
   * **Example:**
   *
   * @code{.c}
   * tiledb::Context ctx;
   *
   * // Fixed-sized attribute
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
   * a1.set_nullable(true);
   * int32_t value = 0;
   * uint64_t size = sizeof(value);
   * uint8_t valid = 0;
   * a1.set_fill_value(&value, size, valid);
   *
   * // Var-sized attribute
   * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
   * a2.set_nullable(true);
   * std::string value("null");
   * uint8_t valid = 0;
   * a2.set_fill_value(value.c_str(), value.size(), valid);
   * @endcode
   *
   * @param value The fill value to set.
   * @param size The fill value size in bytes.
   * @param valid The validity fill value, zero for a null value and
   *     non-zero for a valid attribute.
   *
   * @note A call to `cell_val_num` sets the fill value
   *     of the attribute to its default. Therefore, make sure you invoke
   *     `set_fill_value` after deciding on the number
   *     of values this attribute will hold in each cell.
   *
   * @note For fixed-sized attributes, the input `size` should be equal
   *     to the cell size.
   */
  Attribute& set_fill_value(const void* value, uint64_t size, uint8_t valid) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_set_fill_value_nullable(
        ctx.ptr().get(), attr_.get(), value, size, valid));
    return *this;
  }

  /**
   * Gets the default fill value for the input attribute. This value will
   * be used for the input attribute whenever querying (1) an empty cell in
   * a dense array, or (2) a non-empty cell (in either dense or sparse array)
   * when values on the input attribute are missing (e.g., if the user writes
   * a subset of the attributes in a write operation).
   *
   * Applicable to both fixed-sized and var-sized attributes.
   *
   * **Example:**
   *
   * @code{.c}
   * // Fixed-sized attribute
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
   * a1.set_nullable(true);
   * const int32_t* value;
   * uint64_t size;
   * uint8_t valid;
   * a1.get_fill_value(&value, &size, &valid);
   *
   * // Var-sized attribute
   * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
   * a2.set_nullable(true);
   * const char* value;
   * uint64_t size;
   * uint8_t valid;
   * a2.get_fill_value(&value, &size, &valid);
   * @endcode
   *
   * @param value A pointer to the fill value to get.
   * @param size The size of the fill value to get.
   * @param valid The fill value validity to get.
   */
  void get_fill_value(const void** value, uint64_t* size, uint8_t* valid) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_get_fill_value_nullable(
        ctx.ptr().get(), attr_.get(), value, size, valid));
  }

  /** Check if attribute is variable sized. **/
  bool variable_sized() const {
    return cell_val_num() == TILEDB_VAR_NUM;
  }

  /**
   * Returns a copy of the FilterList of the attribute.
   * To change the filter list, use `set_filter_list()`.
   *
   * @return Copy of the attribute FilterList.
   */
  FilterList filter_list() const {
    auto& ctx = ctx_.get();
    tiledb_filter_list_t* filter_list;
    ctx.handle_error(tiledb_attribute_get_filter_list(
        ctx.ptr().get(), attr_.get(), &filter_list));
    return FilterList(ctx, filter_list);
  }

  /**
   * Sets the attribute filter list, which is an ordered list of filters that
   * will be used to process and/or transform the attribute data (such as
   * compression).
   *
   * @param filter_list Filter list to set
   * @return Reference to this Attribute
   */
  Attribute& set_filter_list(const FilterList& filter_list) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_set_filter_list(
        ctx.ptr().get(), attr_.get(), filter_list.ptr().get()));
    return *this;
  }

  /**
   * Sets the nullability of an attribute.
   *
   * **Example:**
   * @code{.cpp}
   * auto a1 = Attribute::create<int>(...);
   * a1.set_nullable(true);
   * @endcode
   *
   * @param nullable Whether the attribute is nullable.
   * @return Reference to this Attribute
   */
  Attribute& set_nullable(bool nullable) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_attribute_set_nullable(
        ctx.ptr().get(), attr_.get(), static_cast<uint8_t>(nullable)));
    return *this;
  }

  /**
   * Gets the nullability of an attribute.
   *
   * **Example:**
   * @code{.cpp}
   * auto a1 = Attribute::create<int>(...);
   * auto nullable = a1.nullable();
   * @endcode
   *
   * @return Whether the attribute is nullable.
   */
  bool nullable() const {
    auto& ctx = ctx_.get();
    uint8_t nullable;
    ctx.handle_error(
        tiledb_attribute_get_nullable(ctx.ptr().get(), attr_.get(), &nullable));
    return static_cast<bool>(nullable);
  }

  /** Returns the C TileDB attribute object pointer. */
  std::shared_ptr<tiledb_attribute_t> ptr() const {
    return attr_;
  }

  /**
   * Dumps information about the attribute in an ASCII representation to an
   * output.
   *
   * @param out (Optional) File to dump output to. Defaults to `nullptr`
   * which will lead to selection of `stdout`.
   */
  void dump(FILE* out = nullptr) const {
    ctx_.get().handle_error(
        tiledb_attribute_dump(ctx_.get().ptr().get(), attr_.get(), out));
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Factory function for creating a new attribute with datatype T.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
   * auto a2 = tiledb::Attribute::create<std::string>(ctx, "a2");
   * auto a3 = tiledb::Attribute::create<std::array<float, 3>>(ctx, "a3");
   * auto a4 = tiledb::Attribute::create<std::vector<double>>(ctx, "a4");
   * auto a5 = tiledb::Attribute::create<char[8]>(ctx, "a5");
   * @endcode
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
    Attribute a(ctx, name, DataT::tiledb_type);
    a.set_cell_val_num(DataT::tiledb_num);
    return a;
  }

  /** Factory function taking the type as a tiledb_datatype_t variable. */
  static Attribute create(
      const Context& ctx, const std::string& name, tiledb_datatype_t type) {
    Attribute a(ctx, name, type);
    return a;
  }

  /**
   * Factory function for creating a new attribute with datatype T and
   * a FilterList.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * tiledb::FilterList filter_list(ctx);
   * filter_list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
   *     .add_filter({ctx, TILEDB_FILTER_BZIP2});
   * auto a1 = tiledb::Attribute::create<int>(ctx, "a1", filter_list);
   * @endcode
   *
   * @tparam T Datatype of the attribute. Can either be arithmetic type,
   *         C-style array, `std::string`, `std::vector`, or any trivially
   *         copyable classes (defined by `std::is_trivially_copyable`).
   * @param ctx The TileDB context.
   * @param name The attribute name.
   * @param filter_list FilterList to use for attribute
   * @return A new Attribute object.
   */
  template <typename T>
  static Attribute create(
      const Context& ctx,
      const std::string& name,
      const FilterList& filter_list) {
    auto a = create<T>(ctx, name);
    a.set_filter_list(filter_list);
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

  void init_from_type(const std::string& name, tiledb_datatype_t type) {
    tiledb_attribute_t* attr;
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_attribute_alloc(ctx.ptr().get(), name.c_str(), type, &attr));
    attr_ = std::shared_ptr<tiledb_attribute_t>(attr, deleter_);
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
