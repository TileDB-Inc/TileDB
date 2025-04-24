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

#include <optional>

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
   * Extend this enumeration using the provided values.
   *
   * @param values The values to extend the enumeration with.
   * @return The newly created enumeration.
   */
  template <typename T, impl::enable_trivial<T>* = nullptr>
  Enumeration extend(std::vector<T> values) {
    if (values.size() == 0) {
      throw TileDBError(
          "Unable to extend an enumeration with an empty vector.");
    }

    if (cell_val_num() == TILEDB_VAR_NUM) {
      throw TileDBError(
          "Error extending var sized enumeration with fixed size data.");
    }

    if constexpr (std::is_same_v<T, bool>) {
      // This awkward dance for std::vector<bool> is due to the fact that
      // C++ provides a template specialization that uses a bitmap which does
      // not provide `data()` member method.
      std::vector<uint8_t> converted(values.size());
      for (size_t i = 0; i < values.size(); i++) {
        converted[i] = values[i] ? 1 : 0;
      }

      return extend(converted.data(), converted.size() * sizeof(T), nullptr, 0);
    } else {
      return extend(values.data(), values.size() * sizeof(T), nullptr, 0);
    }
  }

  /**
   * Extend this enumeration using the provided string values.
   *
   * @param values The string values to extend the enumeration with.
   * @return The newly created enumeration.
   */
  template <typename T, impl::enable_trivial<T>* = nullptr>
  Enumeration extend(std::vector<std::basic_string<T>>& values) {
    if (values.size() == 0) {
      throw TileDBError(
          "Unable to extend an enumeration with an empty vector.");
    }

    if (cell_val_num() != TILEDB_VAR_NUM) {
      throw TileDBError(
          "Error extending fixed sized enumeration with var size data.");
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

    return extend(
        data.data(),
        data.size(),
        offsets.data(),
        offsets.size() * sizeof(uint64_t));
  }

  /**
   * Extend this enumeration using the provided pointers.
   *
   * @return The newly created enumeration.
   */
  Enumeration extend(
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size) {
    tiledb_enumeration_t* old_enmr = enumeration_.get();
    tiledb_enumeration_t* new_enmr = nullptr;
    ctx_.get().handle_error(tiledb_enumeration_extend(
        ctx_.get().ptr().get(),
        old_enmr,
        data,
        data_size,
        offsets,
        offsets_size,
        &new_enmr));
    return Enumeration(ctx_, new_enmr);
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

  template <typename T, impl::enable_trivial<T>* = nullptr>
  std::optional<uint64_t> index_of(T value) {
    int exist = 0;
    uint64_t index;
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();
    ctx_.get().handle_error(tiledb_enumeration_get_value_index(
        c_ctx, enumeration_.get(), &value, sizeof(T), &exist, &index));

    return exist == 1 ? std::make_optional(index) : std::nullopt;
  }

  template <typename T, impl::enable_trivial<T>* = nullptr>
  std::optional<uint64_t> index_of(std::basic_string_view<T>& value) {
    int exist = 0;
    uint64_t index;
    tiledb_ctx_t* c_ctx = ctx_.get().ptr().get();
    ctx_.get().handle_error(tiledb_enumeration_get_value_index(
        c_ctx,
        enumeration_.get(),
        value.data(),
        value.size() * sizeof(T),
        &exist,
        &index));

    return exist == 1 ? std::make_optional(index) : std::nullopt;
  }

  /**
   * Get the context of the enumeration
   * @return Context The context of the enumeration.
   */
  Context context() const {
    return ctx_;
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

#ifndef TILEDB_REMOVE_DEPRECATIONS
  /**
   * Dump a string representation of the Enumeration to the given FILE pointer.
   *
   * @param out A FILE pointer to write to. Defaults to `stdout`.
   */
  TILEDB_DEPRECATED
  void dump(FILE* out = stdout) const {
    ctx_.get().handle_error(tiledb_enumeration_dump(
        ctx_.get().ptr().get(), enumeration_.get(), out));
  }
#endif

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Create an empty enumeration.
   *
   * @param ctx The context to use.
   * @param name The name of the enumeration.
   * @param type The datatype of the enumeration values. This is automatically
   *        deduced if not provided.
   * @param cell_val_num The number of values per cell.
   * @param ordered Whether or not to consider this enumeration ordered.
   * @return Enumeration The newly constructed enumeration.
   */
  static Enumeration create_empty(
      const Context& ctx,
      const std::string& name,
      tiledb_datatype_t type,
      uint32_t cell_val_num,
      bool ordered = false) {
    return create(
        ctx, name, type, cell_val_num, ordered, nullptr, 0, nullptr, 0);
  }

  /**
   * Create an enumeration from a vector of trivial values (i.e., int's or other
   * integral or floating point values)
   *
   * @param ctx The context to use.
   * @param name The name of the enumeration.
   * @param values A vector of enumeration values
   * @param ordered Whether or not to consider this enumeration ordered.
   * @param type A specific type if you want to override the default.
   * @return Enumeration The newly constructed enumeration.
   */
  template <typename T, impl::enable_trivial<T>* = nullptr>
  static Enumeration create(
      const Context& ctx,
      const std::string& name,
      std::vector<T>& values,
      bool ordered = false,
      std::optional<tiledb_datatype_t> type = std::nullopt) {
    using DataT = impl::TypeHandler<T>;
    tiledb_datatype_t dtype = type.value_or(DataT::tiledb_type);

    if constexpr (!std::is_same_v<T, bool>) {
      return create(
          ctx,
          name,
          dtype,
          DataT::tiledb_num,
          ordered,
          values.data(),
          values.size() * sizeof(T),
          nullptr,
          0);
    } else {
      // This awkward dance for std::vector<bool> is due to the fact that
      // C++ provides a template specialization that uses a bitmap which does
      // not provide `data()` member method.
      std::vector<uint8_t> converted(values.size());
      for (size_t i = 0; i < values.size(); i++) {
        converted[i] = values[i] ? 1 : 0;
      }
      return create(
          ctx,
          name,
          dtype,
          DataT::tiledb_num,
          ordered,
          converted.data(),
          converted.size() * sizeof(uint8_t),
          nullptr,
          0);
    }
  }

  /**
   * Create an enumeration from a vector of strings
   *
   * @param ctx The context to use.
   * @param name The name of the enumeration.
   * @param values A vector of enumeration values
   * @param ordered Whether or not to consider this enumeration ordered.
   * @param type A specific type if you want to override the default.
   * @return Enumeration The newly constructed enumeration.
   */

  template <typename T, impl::enable_trivial<T>* = nullptr>
  static Enumeration create(
      const Context& ctx,
      const std::string& name,
      std::vector<std::basic_string<T>>& values,
      bool ordered = false,
      std::optional<tiledb_datatype_t> type = std::nullopt) {
    using DataT = impl::TypeHandler<T>;
    static_assert(
        DataT::tiledb_num == 1, "Enumeration string values cannot be compound");
    tiledb_datatype_t dtype = type.value_or(DataT::tiledb_type);

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
        dtype,
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
   * @param name The name of the enumeration.
   * @param type The datatype of the enumeration values.
   * @param cell_val_num The number of values per cell of the values.
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

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Converts the array schema into a string representation. */
inline std::ostream& operator<<(
    std::ostream& os, const Enumeration& enumeration) {
  const auto& ctx = enumeration.context();
  tiledb_string_t* tdb_string;

  ctx.handle_error(tiledb_enumeration_dump_str(
      ctx.ptr().get(), enumeration.ptr().get(), &tdb_string));

  os << impl::convert_to_string(&tdb_string).value();

  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_ENUMERATION_EXPERIMENTAL_H
