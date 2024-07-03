/**
 * @file tiledb/api/c_api/dimension/dimension_api_internal.h
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
 * This file declares the dimension section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_DIMENSION_INTERNAL_H
#define TILEDB_CAPI_DIMENSION_INTERNAL_H

#include <memory>
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/dimension.h"

/**
 * Handle `struct` for API dimension objects.
 */
struct tiledb_dimension_handle_t
    : public tiledb::api::CAPIHandle<tiledb_dimension_handle_t> {
 private:
  /**
   * The underlying type of this handle is an allocated object because that's
   * how `Domain` stores its `Dimension` objects. Using `shared_ptr` here
   * acknowledges that there will be an eventual allocation and gets it out of
   * the way. At some future point the life cycle of array schema objects may
   * change, and with it the benefit of allocating in this handle class.
   */
  using dimension_type = shared_ptr<tiledb::sm::Dimension>;
  /**
   * The underlying `Dimension` object
   */
  dimension_type dimension_;

 public:
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"dimension"};

  tiledb_dimension_handle_t(
      const std::string& name,
      tiledb::sm::Datatype type,
      shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
      : dimension_(make_shared<tiledb::sm::Dimension>(
            HERE(), name, type, memory_tracker)) {
  }

  /**
   * Constructor from `shared_ptr<Dimension>` copies the shared pointer.
   */
  explicit tiledb_dimension_handle_t(const dimension_type& x)
      : dimension_(x) {
  }

  /**
   * Copy the underlying dimension object.
   */
  [[nodiscard]] shared_ptr<tiledb::sm::Dimension> copy_dimension() {
    return dimension_;
  }

  inline void set_domain(const void* x) {
    throw_if_not_ok(dimension_->set_domain(x));
  }

  inline void set_tile_extent(const void* x) {
    throw_if_not_ok(dimension_->set_tile_extent(x));
  }

  inline void set_filter_pipeline(const tiledb::sm::FilterPipeline& x) {
    dimension_->set_filter_pipeline(x);
  }

  inline void set_cell_val_num(unsigned int x) {
    throw_if_not_ok(dimension_->set_cell_val_num(x));
  }

  [[nodiscard]] inline const tiledb::sm::FilterPipeline& filters() const {
    return dimension_->filters();
  }

  [[nodiscard]] inline unsigned cell_val_num() const {
    return dimension_->cell_val_num();
  }

  [[nodiscard]] inline const std::string& name() const {
    return dimension_->name();
  }

  [[nodiscard]] inline tiledb::sm::Datatype type() const {
    return dimension_->type();
  }

  [[nodiscard]] inline const Range& domain() const {
    return dimension_->domain();
  }

  [[nodiscard]] inline const tiledb::sm::ByteVecValue& tile_extent() const {
    return dimension_->tile_extent();
  }

  friend std::ostream& operator<<(
      std::ostream& os, const tiledb_dimension_handle_t& dim);
};

namespace tiledb::api {

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param dim A possibly-valid dimension handle
 */
inline void ensure_dimension_is_valid(const tiledb_dimension_handle_t* dim) {
  ensure_handle_is_valid(dim);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_DIMENSION_INTERNAL_H
