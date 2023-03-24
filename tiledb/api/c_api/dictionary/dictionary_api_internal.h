/**
 * @file tiledb/api/c_api/dictionary/dictionary_api_internal.h
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
 * This file declares the dictionary section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_DICTIONARY_INTERNAL_H
#define TILEDB_CAPI_DICTIONARY_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/dictionary.h"
#include "dictionary_api_external.h"

/**
 * Handle `struct` for API dimension objects.
 */
struct tiledb_dictionary_handle_t
    : public tiledb::api::CAPIHandle<tiledb_dictionary_handle_t> {
 private:
  using dictionary_type = shared_ptr<tiledb::sm::Dictionary>;
  dictionary_type dictionary_;

 public:
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"dictionary"};

  tiledb_dictionary_handle_t(tiledb::sm::Datatype type)
      : dictionary_(make_shared<tiledb::sm::Dictionary>(HERE(), type)) {
  }

  /**
   * Constructor from `shared_ptr<Dictionary>` copies the shared pointer.
   */
  explicit tiledb_dictionary_handle_t(const dictionary_type& x)
      : dictionary_(x) {
  }

  /**
   * Copy the underlying dimension object.
   */
  [[nodiscard]] shared_ptr<tiledb::sm::Dictionary> copy_dictionary() {
    return dictionary_;
  }

  [[nodiscard]] inline tiledb::sm::Datatype type() const {
    return dictionary_->type();
  }

  inline void set_cell_val_num(unsigned int x) {
    dictionary_->set_cell_val_num(x);
  }

  [[nodiscard]] inline unsigned cell_val_num() const {
    return dictionary_->cell_val_num();
  }

  inline void set_nullable(bool nullable) {
    dictionary_->set_nullable(nullable);
  }

  [[nodiscard]] inline bool nullable() const {
    return dictionary_->nullable();
  }

  inline void set_ordered(bool ordered) {
    dictionary_->set_ordered(ordered);
  }

  [[nodiscard]] inline bool ordered() const {
    return dictionary_->ordered();
  }

  inline void set_data_buffer(void* const buffer, uint64_t buffer_size) {
    dictionary_->set_data_buffer(buffer, buffer_size);
  }

  inline void get_data_buffer(void** buffer, uint64_t* buffer_size) {
    dictionary_->get_data_buffer(buffer, buffer_size);
  }

  inline void set_offsets_buffer(void* const buffer, uint64_t buffer_size) {
    dictionary_->set_offsets_buffer(buffer, buffer_size);
  }

  inline void get_offsets_buffer(void** buffer, uint64_t* buffer_size) {
    dictionary_->get_offsets_buffer(buffer, buffer_size);
  }

  inline void set_validity_buffer(void* const buffer, uint64_t buffer_size) {
    dictionary_->set_validity_buffer(buffer, buffer_size);
  }

  inline void get_validity_buffer(void** buffer, uint64_t* buffer_size) {
    dictionary_->get_validity_buffer(buffer, buffer_size);
  }

  inline void dump(FILE* out) const {
    dictionary_->dump(out);
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param dict A possibly-valid dictionary handle
 */
inline void ensure_dictionary_is_valid(const tiledb_dictionary_handle_t* dict) {
  ensure_handle_is_valid(dict);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_DICTIONARY_INTERNAL_H
