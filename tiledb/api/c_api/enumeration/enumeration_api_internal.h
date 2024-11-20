/**
 * @file tiledb/api/c_api/enumeration/enumeration_api_internal.h
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
 * This file declares the enumeration section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_ENUMERATION_INTERNAL_H
#define TILEDB_CAPI_ENUMERATION_INTERNAL_H

#include "enumeration_api_experimental.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/enumeration.h"

/**
 * Handle `struct` for API enumeration objects.
 */
struct tiledb_enumeration_handle_t : public tiledb::api::CAPIHandle {
 private:
  shared_ptr<const tiledb::sm::Enumeration> enumeration_;

 public:
  /** Type name. */
  static constexpr std::string_view object_type_name{"enumeration"};

  template <typename... Arg>
  tiledb_enumeration_handle_t(Arg&&... arg)
      : enumeration_(
            tiledb::sm::Enumeration::create(std::forward<Arg>(arg)...)) {
  }

  /**
   * Constructor from `shared_ptr<Enumeration>` copies the shared pointer.
   */
  explicit tiledb_enumeration_handle_t(
      shared_ptr<const tiledb::sm::Enumeration>& e)
      : enumeration_(e) {
  }

  /**
   * Accessor for the underlying tiledb::sm::Enumeration.
   */
  shared_ptr<const tiledb::sm::Enumeration> enumeration() const {
    return enumeration_;
  }

  /**
   * Copy the underlying enumeration object.
   */
  [[nodiscard]] shared_ptr<const tiledb::sm::Enumeration> copy() {
    return enumeration_;
  }

  /**
   * Extend a given enumeration.
   */
  [[nodiscard]] shared_ptr<const tiledb::sm::Enumeration> extend(
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size) const {
    return enumeration_->extend(data, data_size, offsets, offsets_size);
  }

  /**
   * Returns wether or not an enumeration is an extension of this one.
   */
  [[nodiscard]] inline bool is_extension_of(
      tiledb_enumeration_handle_t* rhs) const {
    return enumeration_->is_extension_of(rhs->enumeration_);
  }

  /**
   * Return the name of the enumeration.
   */
  [[nodiscard]] inline const std::string& name() const {
    return enumeration_->name();
  }

  /**
   * Return the data type of the enumeration values.
   */
  [[nodiscard]] inline tiledb_datatype_t type() const {
    return static_cast<tiledb_datatype_t>(enumeration_->type());
  }

  /**
   * Return the cell_val_num of the enumeration values
   */
  [[nodiscard]] inline uint32_t cell_val_num() const {
    return enumeration_->cell_val_num();
  }

  /**
   * Return a bool indicating whether the enumeration values are ordered.
   */
  [[nodiscard]] inline bool ordered() const {
    return enumeration_->ordered();
  }

  /**
   * Return a pointer and size tuple for the underlying data type.
   */
  [[nodiscard]] inline const span<uint8_t> data() const {
    return enumeration_->data();
  }

  /**
   * Return a pointer and size tuple for the underlying offsets buffer.
   */
  [[nodiscard]] inline const span<uint8_t> offsets() const {
    return enumeration_->offsets();
  }

  friend std::ostream& operator<<(
      std::ostream& os, const tiledb_enumeration_handle_t& enumeration);
};

namespace tiledb::api {

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param dim A possibly-valid enumeration handle.
 */
inline void ensure_enumeration_is_valid(const tiledb_enumeration_handle_t* e) {
  ensure_handle_is_valid(e);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_ENUMERATION_INTERNAL_H
