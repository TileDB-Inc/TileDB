/**
 * @file dictionary.h
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
 * This file declares the C++ API for the TileDB Dictionary object.
 */

#ifndef TILEDB_CPP_API_DICTIONARY_H
#define TILEDB_CPP_API_DICTIONARY_H

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


class Dictionary {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Dictionary(const Context& ctx, tiledb_dictionary_t* dict)
      : ctx_(ctx) {
    dict_ = std::shared_ptr<tiledb_dictionary_t>(dict, deleter_);
  }

  Dictionary(const Dictionary&) = default;
  Dictionary(Dictionary&&) = default;
  Dictionary& operator=(const Dictionary&) = default;
  Dictionary& operator=(Dictionary&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  std::vector<std::string> get_values() {
    // Assert that type() == TILEDB_STRING_UTF8?

    void* v_buffer;
    uint64_t buffer_size;
    void* v_offsets;
    uint64_t offsets_size;

    get_data_buffer(static_cast<void**>(&v_buffer), &buffer_size);
    get_offsets_buffer(static_cast<void**>(&v_offsets), &offsets_size);
    auto num_offsets = offsets_size / sizeof(uint64_t);

    char* buffer = static_cast<char*>(v_buffer);
    uint64_t* offsets = static_cast<uint64_t*>(v_offsets);

    std::vector<std::string> ret;

    for(size_t i = 0; i < num_offsets; i++) {
      uint64_t len = 0;
      if (i < num_offsets  - 1) {
        len = offsets[i + 1] - offsets[i];
      } else {
        len = buffer_size - offsets[i];
      }

      ret.emplace_back(buffer + offsets[i], len);
    }

    return ret;
  }

  /** Returns the dictionary datatype. */
  tiledb_datatype_t type() const {
    tiledb_datatype_t type;
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_dictionary_get_type(ctx.ptr().get(), dict_.get(), &type));
    return type;
  }

  Dictionary& set_cell_val_num(uint32_t cell_val_num) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_set_cell_val_num(
        ctx.ptr().get(), dict_.get(), cell_val_num));
    return *this;
  }

  uint32_t cell_val_num() const {
    uint32_t cell_val_num;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_get_cell_val_num(
      ctx.ptr().get(), dict_.get(), &cell_val_num));
    return cell_val_num;
  }

  Dictionary& set_nullable(bool nullable) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_set_nullable(
      ctx.ptr().get(), dict_.get(), static_cast<uint8_t>(nullable)));
    return *this;
  }

  bool nullable() const {
    uint8_t nullable;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_get_nullable(
        ctx.ptr().get(), dict_.get(), &nullable));
    return static_cast<bool>(nullable);
  }

  Dictionary& set_ordered(bool ordered) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_set_ordered(
      ctx.ptr().get(), dict_.get(), static_cast<uint8_t>(ordered)));
    return *this;
  }

  bool ordered() const {
    uint8_t ordered;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_get_ordered(
        ctx.ptr().get(), dict_.get(), &ordered));
    return static_cast<bool>(ordered);
  }

  Dictionary& set_data_buffer(void* buffer, uint64_t buffer_size) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_set_data_buffer(
        ctx.ptr().get(), dict_.get(), buffer, buffer_size));
    return *this;
  }

  void get_data_buffer(void** buffer, uint64_t* buffer_size) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_get_data_buffer(
        ctx.ptr().get(), dict_.get(), buffer, buffer_size));
  }

  Dictionary& set_offsets_buffer(void* buffer, uint64_t buffer_size) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_set_offsets_buffer(
        ctx.ptr().get(), dict_.get(), buffer, buffer_size));
    return *this;
  }

  void get_offsets_buffer(void** buffer, uint64_t* buffer_size) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_get_offsets_buffer(
        ctx.ptr().get(), dict_.get(), buffer, buffer_size));
  }

  Dictionary& set_validity_buffer(void* buffer, uint64_t buffer_size) {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_set_validity_buffer(
        ctx.ptr().get(), dict_.get(), buffer, buffer_size));
    return *this;
  }

  void get_validity_buffer(void** buffer, uint64_t* buffer_size) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_get_validity_buffer(
        ctx.ptr().get(), dict_.get(), buffer, buffer_size));
  }

  void dump(FILE* out = nullptr) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_dictionary_dump(ctx.ptr().get(), dict_.get(), out));
  }

  /** Returns a shared pointer to the C TileDB dictionary object. */
  std::shared_ptr<tiledb_dictionary_t> ptr() const {
    return dict_;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  static Dictionary create(
      const Context& ctx,
      std::vector<std::string> data,
      bool nullable = false,
      bool ordered = false) {

    uint64_t buffer_size = 0;
    for(auto s : data) {
      buffer_size += s.size();
    }

    uint8_t buffer[buffer_size];
    std::vector<uint64_t> offsets;
    uint64_t offset = 0;
    for(auto s: data) {
      std::memcpy(buffer + offset, s.data(), s.size());
      offsets.push_back(offset);
      offset += s.size();
    }

    return create(
        ctx,
        TILEDB_STRING_UTF8,
        std::numeric_limits<uint32_t>::max(),
        nullable,
        ordered,
        buffer,
        buffer_size,
        offsets.data(),
        offsets.size() * sizeof(uint64_t));
  }

  static Dictionary create(
      const Context& ctx,
      tiledb_datatype_t type,
      uint32_t cell_val_num = 1,
      bool nullable = false,
      bool ordered = false,
      void* buffer = nullptr,
      uint64_t buffer_size = 0,
      void* offsets = nullptr,
      uint64_t offsets_size = 0) {

    tiledb_dictionary_t* dict;
    ctx.handle_error(tiledb_dictionary_alloc(
        ctx.ptr().get(), type, &dict));

    auto ret = Dictionary(ctx, dict);

    ret.set_cell_val_num(cell_val_num)
      .set_nullable(nullable)
      .set_ordered(ordered);

    if (buffer != nullptr) {
      ret.set_data_buffer(buffer, buffer_size);
    }

    if (offsets != nullptr) {
      ret.set_offsets_buffer(offsets, offsets_size);
    }

    return ret;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** A deleter wrapper. */
  impl::Deleter deleter_;

  /** The C TileDB dictionary object. */
  std::shared_ptr<tiledb_dictionary_t> dict_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_DIMENSION_H
