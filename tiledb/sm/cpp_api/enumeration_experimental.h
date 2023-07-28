/**
 * @file enumeration_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Enumeration class.
 */

#ifndef TILEDB_CPP_API_ENUMERATION_EXPERIMENTAL_H
#define TILEDB_CPP_API_ENUMERATION_EXPERIMENTAL_H

#include "context.h"
#include "tiledb.h"
#include "tiledb_experimental.h"

namespace tiledb {

class Enumeration {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Create an enumeration by wrapping a pointer allocated by the C API
   *
   * @param ctx The Context to use.
   * @param enmr The tiledb_enumeration_t allocated by the C API.
   */
  Enumeration(const Context& ctx, tiledb_enumeration_t* enmr)
      : ctx_(ctx) {
    enumeration_ = std::shared_ptr<tiledb_enumeration_t>(enmr, deleter_);
  }

  Enumeration(const Enumeration&) = default;
  Enumeration(Enumeration&&) = default;
  Enumeration& operator=(const Enumeration&) = default;
  Enumeration& operator=(Enumeration&&) = default;

  /** Destructor. */
  ~Enumeration() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Get the C TileDB context object.
   *
   * @return shared_ptr<tiledb_enumeration_t>
   */
  std::shared_ptr<tiledb_enumeration_t> ptr() const {
    return enumeration_;
  }

  /**
   * Get the name of the enumeration
   *
   * @return std::string The name of the enumeration.
   */
  std::string name() const {
    // Get the enumeration name as a string handle
    tiledb_string_t* enmr_name;
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();
    ctx_.get().handle_error(
        tiledb_enumeration_get_name(c_ctx, enumeration_.get(), &enmr_name));

    // Convert string handle to a std::string
    const char* name_ptr;
    size_t name_len;
    ctx_.get().handle_error(
        tiledb_string_view(enmr_name, &name_ptr, &name_len));
    std::string ret(name_ptr, name_len);

    // Release the string handle
    ctx_.get().handle_error(tiledb_string_free(&enmr_name));

    return ret;
  }

  /**
   * Get the type of the enumeration
   *
   * @return tiledb_datatype_t The datatype of the enumeration values.
   */
  tiledb_datatype_t type() const {
    tiledb_datatype_t ret;
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();
    ctx_.get().handle_error(
        tiledb_enumeration_get_type(c_ctx, enumeration_.get(), &ret));
    return ret;
  }

  /**
   * Get the cell_val_num of the enumeration
   *
   * @return uint32_t The cell_val_num of the enumeration.
   */
  uint32_t cell_val_num() const {
    uint32_t ret;
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();
    ctx_.get().handle_error(
        tiledb_enumeration_get_cell_val_num(c_ctx, enumeration_.get(), &ret));
    return ret;
  }

  /**
   * Get whether this enumeration is considered ordered.
   *
   * If an enumeration is not considered ordered inequality operators are
   * disabled in QueryConditions applied against the enumeration values.
   *
   * @return bool Whether the enumeration is considered ordered.
   */
  bool ordered() const {
    int is_ordered;
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();
    ctx_.get().handle_error(
        tiledb_enumeration_get_ordered(c_ctx, enumeration_.get(), &is_ordered));
    return is_ordered ? true : false;
  }

  /**
   * Convert the enumeration values into a vector
   *
   * @param vec The vector that will store the values of the enumeration.
   */
  template <typename T>
  std::vector<typename std::enable_if_t<std::is_trivially_copyable_v<T>, T>>
  as_vector() {
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();

    const void* data;
    uint64_t data_size;
    ctx_.get().handle_error(tiledb_enumeration_get_data(
        c_ctx, enumeration_.get(), &data, &data_size));

    const T* elems = static_cast<const T*>(data);
    size_t count = data_size / sizeof(T);

    std::vector<T> ret;
    ret.reserve(count);
    for (size_t i = 0; i < count; i++) {
      ret.push_back(elems[i]);
    }

    return ret;
  }

  /**
   * Convert the enumeration values into a vector of std::string values
   *
   * @param vec The vector used to return the string data.
   */
  template <typename T>
  std::vector<typename std::enable_if_t<std::is_same_v<std::string, T>, T>>
  as_vector() {
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();

    const void* data;
    uint64_t data_size;
    ctx_.get().handle_error(tiledb_enumeration_get_data(
        c_ctx, enumeration_.get(), &data, &data_size));

    const void* offsets;
    uint64_t offsets_size;
    ctx_.get().handle_error(tiledb_enumeration_get_offsets(
        c_ctx, enumeration_.get(), &offsets, &offsets_size));

    const char* str_data = static_cast<const char*>(data);
    const uint64_t* elems = static_cast<const uint64_t*>(offsets);
    size_t count = offsets_size / sizeof(uint64_t);

    std::vector<std::string> ret;
    ret.reserve(count);
    for (size_t i = 0; i < count; i++) {
      uint64_t len;
      if (i + 1 < count) {
        len = elems[i + 1] - elems[i];
      } else {
        len = data_size - elems[i];
      }
      ret.emplace_back(str_data + elems[i], len);
    }

    return ret;
  }

  /**
   * Dump a string representation of the Enumeration to the given FILE pointer.
   *
   * @param out A FILE pointer to write to. stdout is used if nullptr is passed.
   */
  void dump(FILE* out = nullptr) const {
    ctx_.get().handle_error(tiledb_enumeration_dump(
        ctx_.get().ptr().get(), enumeration_.get(), out));
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Create an enumeration from a vector of trivial values (i.e., int's or other
   * integral or floating point values)
   *
   * @param ctx The context to use.
   * @param values The list of values to use for this enumeration.
   * @param ordered Whether or not to consider this enumeration ordered.
   * @param type The datatype of the enumeration values. This is automatically
   *        deduced if not provided.
   */
  template <typename T, impl::enable_trivial<T>* = nullptr>
  static Enumeration create(
      const Context& ctx,
      const std::string& name,
      std::vector<T>& values,
      bool ordered = false,
      tiledb_datatype_t type = static_cast<tiledb_datatype_t>(255)) {
    using DataT = impl::TypeHandler<T>;

    if (type == static_cast<tiledb_datatype_t>(255)) {
      type = DataT::tiledb_type;
    }

    return create(
        ctx,
        name,
        type,
        DataT::tiledb_num,
        ordered,
        values.data(),
        values.size() * sizeof(T),
        nullptr,
        0);
  }

  /**
   * Create an enumeration from a vector of strings
   *
   * @param ctx The context to use.
   * @param values The vector of values for the enumeration.
   * @param ordered Whether to consider the enumerationv alues as ordered.
   * @param type The datatype of the enumeration values. This is automatically
   *        deduced if not provided. However, this can be used to override the
   *        deduced type if need be. For instance, TILEDB_STRING_ASCII is the
   *        default type for strings but TILEDB_STRING_UTF8 can be specified.
   */
  template <typename T, impl::enable_trivial<T>* = nullptr>
  static Enumeration create(
      const Context& ctx,
      const std::string& name,
      std::vector<std::basic_string<T>>& values,
      bool ordered = false,
      tiledb_datatype_t type = static_cast<tiledb_datatype_t>(255)) {
    using DataT = impl::TypeHandler<T>;
    static_assert(
        DataT::tiledb_num == 1, "Enumeration string values cannot be compound");

    if (type == static_cast<tiledb_datatype_t>(255)) {
      type = DataT::tiledb_type;
    }

    uint64_t total_size = 0;
    for (auto v : values) {
      total_size += v.size() * sizeof(T);
    }

    std::vector<uint8_t> data(total_size, 0);
    std::vector<uint64_t> offsets;
    offsets.reserve(values.size());
    uint64_t curr_offset = 0;

    for (auto v : values) {
      std::memcpy(data.data() + curr_offset, v.data(), v.size() * sizeof(T));
      offsets.push_back(curr_offset);
      curr_offset += v.size() * sizeof(T);
    }

    return create(
        ctx,
        name,
        type,
        TILEDB_VAR_NUM,
        ordered,
        data.data(),
        total_size,
        offsets.data(),
        offsets.size() * sizeof(uint64_t));
  }

  /**
   * Create an enumeration
   *
   * @param ctx The context to use.
   * @param type The datatype of the enumeration values.
   * @param cell_val_num The cell_val_num of the enumeration.
   * @param ordered Whether this enumeration should be considered ordered.
   * @param data A pointer to a buffer of values for this enumeration.
   * @param data_size The size of the buffer pointed to by data.
   * @param offsets A pointer to the offsets buffer if required.
   * @param offsets_size The size of the buffer pointed to by offsets.
   */
  static Enumeration create(
      const Context& ctx,
      const std::string& name,
      tiledb_datatype_t type,
      uint32_t cell_val_num,
      bool ordered,
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size) {
    tiledb_enumeration_t* enumeration;
    ctx.handle_error(tiledb_enumeration_alloc(
        ctx.ptr().get(),
        name.c_str(),
        type,
        cell_val_num,
        ordered,
        data,
        data_size,
        offsets,
        offsets_size,
        &enumeration));
    return Enumeration(ctx, enumeration);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C Enumeration object. */
  std::shared_ptr<tiledb_enumeration_t> enumeration_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ENUMERATION_EXPERIMENTAL_H
