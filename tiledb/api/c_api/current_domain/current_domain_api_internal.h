/**
 * @file tiledb/api/c_api/current_domain/current_domain_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the internal C API implementation of the Current Domain
 */

#ifndef TILEDB_CAPI_CURRENT_DOMAIN_INTERNAL_H
#define TILEDB_CAPI_CURRENT_DOMAIN_INTERNAL_H

#include "tiledb/api/c_api_support/argument_validation.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/array_schema/current_domain.h"

struct tiledb_current_domain_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"tiledb_current_domain_t"};

 private:
  std::shared_ptr<tiledb::sm::CurrentDomain> current_domain_;

 public:
  /**
   * Default constructor doesn't make sense
   */
  tiledb_current_domain_handle_t() = delete;

  /**
   * Constructs a handle with an empty CurrentDomain instance.
   * @param memory_tracker The tracker to use in the internal CurrentDomain
   * @param version The on-disk format version of the CurrentDomain
   */
  explicit tiledb_current_domain_handle_t(
      shared_ptr<tiledb::sm::MemoryTracker> memory_tracker,
      format_version_t version)
      : current_domain_{make_shared<tiledb::sm::CurrentDomain>(
            HERE(), memory_tracker, version)} {
  }

  /**
   * Constructor.
   * @param current_domain A CurrentDomain instance to assign to this handle
   */
  explicit tiledb_current_domain_handle_t(
      std::shared_ptr<tiledb::sm::CurrentDomain> current_domain)
      : current_domain_(current_domain) {
  }

  [[nodiscard]] inline shared_ptr<tiledb::sm::CurrentDomain> current_domain()
      const {
    return current_domain_;
  }
};

#endif  // TILEDB_CAPI_CURRENT_DOMAIN_INTERNAL_H
