/**
 * @file tiledb/api/c_api/attribute/attribute_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file declares the attribute section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_ATTRIBUTE_INTERNAL_H
#define TILEDB_CAPI_ATTRIBUTE_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/attribute.h"

/*
 * Handle `struct` for API attribute objects.
 */
struct tiledb_attribute_handle_t
    : public tiledb::api::CAPIHandle<tiledb_attribute_handle_t> {
 private:
  /**
   * Storage type matches how attributes are stored in the array schema class.
   */
  using attribute_type = shared_ptr<const tiledb::sm::Attribute>;

  /**
   * The underlying attribute object
   */
  attribute_type attr_;

  /**
   * Internal function to cast away `const` for modifying the underlying
   * attribute. This will only be needed until the underlying object is
   * converted to use `class Proxy`. During that conversion all the functions
   * that need to modify the object will operate on construction data.
   */
  tiledb::sm::Attribute* nonconst_attribute() {
    return const_cast<tiledb::sm::Attribute*>(attr_.get());
  }

 public:
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"attribute"};

  /**
   * Default constructor is deleted.
   */
  tiledb_attribute_handle_t() = delete;

  tiledb_attribute_handle_t(
      const std::string& name, const tiledb::sm::Datatype type)
      : attr_(make_shared<tiledb::sm::Attribute>(HERE(), name, type)) {
  }

  /**
   * Constructor from `shared_ptr<Attribute>` copies the shared pointer.
   */
  explicit tiledb_attribute_handle_t(const attribute_type& x)
      : attr_(x) {
  }

  /**
   * Copy the underlying attribute object.
   */
  [[nodiscard]] attribute_type copy_attribute() const {
    return attr_;
  }

  /**
   * Facade for non-C.41 `Attribute` function
   */
  void set_nullable(bool nullable) {
    nonconst_attribute()->set_nullable(nullable);
  }

  /**
   * Facade for non-C.41 `Attribute` function
   */
  void set_filter_pipeline(const tiledb::sm::FilterPipeline& pipeline) {
    nonconst_attribute()->set_filter_pipeline(pipeline);
  }

  /**
   * Facade for non-C.41 `Attribute` function
   */
  void set_cell_val_num(unsigned int cell_val_num) {
    nonconst_attribute()->set_cell_val_num(cell_val_num);
  }

  /**
   * Facade for non-C.41 `Attribute` function
   */
  void set_fill_value(const void* value, uint64_t size) {
    nonconst_attribute()->set_fill_value(value, size);
  }

  /**
   * Facade for non-C.41 `Attribute` function
   */
  void set_fill_value(const void* value, uint64_t size, uint8_t valid) {
    nonconst_attribute()->set_fill_value(value, size, valid);
  }
  /**
   * Facade for non-C.41 `Attribute` function
   */
  void set_enumeration_name(std::optional<std::string> enmr_name) {
    nonconst_attribute()->set_enumeration_name(enmr_name);
  }

  /**
   * Facade for `Attribute` function
   */
  [[nodiscard]] inline const std::string& name() const {
    return attr_->name();
  }
  /**
   * Facade for `Attribute` function
   */
  [[nodiscard]] inline tiledb::sm::Datatype type() const {
    return attr_->type();
  }
  /**
   * Facade for `Attribute` function
   */
  [[nodiscard]] inline unsigned int cell_val_num() const {
    return attr_->cell_val_num();
  }
  /**
   * Facade for `Attribute` function
   */
  [[nodiscard]] inline bool nullable() const {
    return attr_->nullable();
  }
  /**
   * Facade for `Attribute` function
   */
  [[nodiscard]] inline uint64_t cell_size() const {
    return attr_->cell_size();
  }
  /**
   * Facade for `Attribute` function
   */
  void get_fill_value(const void** value, uint64_t* size) const {
    return attr_->get_fill_value(value, size);
  }
  /**
   * Facade for `Attribute` function
   */
  void get_fill_value(
      const void** value, uint64_t* size, uint8_t* valid) const {
    attr_->get_fill_value(value, size, valid);
  }
  /**
   * Facade for `Attribute` function
   */
  [[nodiscard]] inline const tiledb::sm::FilterPipeline& filters() const {
    return attr_->filters();
  }
  /**
   * Facade for `Attribute` function
   */
  [[nodiscard]] std::optional<std::string> get_enumeration_name() const {
    const auto eref = attr_->get_enumeration_name();
    if (eref.has_value()) {
      return eref.value().get();
    } else {
      return std::nullopt;
    }
  };
  /**
   * Facade for `Attribute` function
   */
  friend std::ostream& operator<<(
      std::ostream& os, const tiledb_attribute_handle_t& attr);
};

namespace tiledb::api {

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param dim A possibly-valid configuration handle
 */
inline void ensure_attribute_is_valid(const tiledb_attribute_handle_t* attr) {
  ensure_handle_is_valid(attr);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_ATTRIBUTE_INTERNAL_H
