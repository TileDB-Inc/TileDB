/**
 * @file tiledb/api/c_api/domain/domain_api_internal.h
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
 */

#ifndef TILEDB_CAPI_DOMAIN_INTERNAL_H
#define TILEDB_CAPI_DOMAIN_INTERNAL_H

// for `FILE`, used in `tiledb_domain_handle_t::dump`
#include <cstdio>

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/array_schema/domain.h"

namespace tiledb::sm {
class MemoryTracker;
}

struct tiledb_domain_handle_t
    : public tiledb::api::CAPIHandle<tiledb_domain_handle_t> {
 private:
  using domain_type = shared_ptr<tiledb::sm::Domain>;
  using dimension_size_type = tiledb::sm::Domain::dimension_size_type;
  domain_type domain_;

 public:
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"domain"};

  /**
   * Default constructor
   *
   * `class Domain` is principally a container for `Dimension` objects. Domain
   * handles are first constructed as empty containers.
   */
  explicit tiledb_domain_handle_t(
      shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
      : domain_{make_shared<tiledb::sm::Domain>(HERE(), memory_tracker)} {
  }

  /**
   * Constructor from shared pointer to a `Domain`.
   */
  explicit tiledb_domain_handle_t(domain_type x)
      : domain_(x) {
  }

  /**
   * Copy the underlying domain object.
   */
  [[nodiscard]] domain_type copy_domain() const {
    return domain_;
  }

  Status add_dimension(shared_ptr<tiledb::sm::Dimension> dim) {
    domain_->add_dimension(dim);
    return Status::Ok();
  }

  [[nodiscard]] inline dimension_size_type dim_num() const {
    return domain_->dim_num();
  }

  [[nodiscard]] inline bool all_dims_same_type() const {
    return domain_->all_dims_same_type();
  }

  [[nodiscard]] inline const tiledb::sm::Dimension* dimension_ptr(
      dimension_size_type i) const {
    return domain_->dimension_ptr(i);
  }

  [[nodiscard]] inline shared_ptr<tiledb::sm::Dimension> shared_dimension(
      dimension_size_type i) const {
    return domain_->shared_dimension(i);
  }

  [[nodiscard]] shared_ptr<tiledb::sm::Dimension> shared_dimension(
      const std::string& name) const {
    return domain_->shared_dimension(name);
  }

  [[nodiscard]] bool has_dimension(const std::string& name) const {
    return domain_->has_dimension(name);
  }

  friend std::ostream& operator<<(
      std::ostream& os, const tiledb_domain_handle_t& domain);
};

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param h A possibly-valid dimension handle
 */
inline void ensure_domain_is_valid(const tiledb_domain_handle_t* h) {
  ensure_handle_is_valid(h);
}

#endif
