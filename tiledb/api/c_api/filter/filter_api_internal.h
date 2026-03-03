/**
 * @file tiledb/api/c_api/filter/filter_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file declares the internals of the filter section of the C API.
 */

#ifndef TILEDB_CAPI_FILTER_INTERNAL_H
#define TILEDB_CAPI_FILTER_INTERNAL_H

#include <memory>
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/filter/filter_create.h"

/**
 * Handle `struct` for API filter list objects.
 *
 * This class has responsibility for maintaining an allocation. For practical
 * reasons, detailed below, its constructor takes a pointer to allocated
 * memory. This is part of a much larger implementation pattern. Filters are
 * stored within `FilterPipeline` as `shared_ptr<Filter>`, but these are
 * initialized with with directly allocations.
 *
 * Existing (legacy) implementation pattern:
 *   - Calls `tdb_new` to allocate filters
 *   - Calls `delete` to release allocation
 *   - Calls `Filter::clone` to copy filters
 *     - `Filter::clone` calls virtual `Filter::clone_impl`; these functions
 *       call `new` or `tdb_new` internally
 *   - Calls `FilterPipeline::add_filter` to build a filter list
 *     - which calls `Filter::clone` internally and uses the value to
 *       initialize `shared_ptr`
 *
 * Changing this will be extensive, although not particularly difficult:
 *   - Store `shared_ptr<Filter>` in the API handle
 *   - Call `make_shared` to allocate filters and release allocations
 *   - Eliminate `Filter::clone`
 *   - Add `FilterPipeline::add_filter` that takes `shared_ptr` argument
 *     - copied into argument, moved from argument
 *   - Add `FilterPipeline::add_filter` that takes a filter rvalue
 *     - must be templated to capture subclass
 *     - `make_shared` with move constructor from argument
 *   - Change `FilterCreate::make` to return `shared_ptr`
 *
 * Changing this will not require changes to deserialization, either from
 * storage or from network. These functions call `make_shared` with constructor
 * arguments (C.41-complaint).
 */
struct tiledb_filter_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"filter"};

 private:
  using filter_type = tiledb::sm::Filter;
  std::shared_ptr<filter_type> value_;

 public:
  /**
   *
   * @param f pointer to a Filter in newly-allocated memory
   */
  explicit tiledb_filter_handle_t(filter_type* f)
      : value_(f) {
  }

  filter_type& filter() {
    return *value_;
  }

  [[nodiscard]] tiledb::sm::FilterType type() const {
    return value_->type();
  };

  Status set_option(tiledb::sm::FilterOption option, const void* value_arg) {
    return value_->set_option(option, value_arg);
  }

  Status get_option(tiledb::sm::FilterOption option, void* value_arg) const {
    return value_->get_option(option, value_arg);
  }
};

namespace tiledb::api {
/**
 * Returns after successfully validating a filter.
 *
 * @param filter_list Possibly-valid pointer to a filter
 */
inline void ensure_filter_is_valid(const tiledb_filter_handle_t* filter) {
  ensure_handle_is_valid(filter);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_FILTER_INTERNAL_H
