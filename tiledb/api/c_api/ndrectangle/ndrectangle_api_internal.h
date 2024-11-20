/**
 * @file tiledb/api/c_api/ndrectangle/ndrectangle_api_internal.h
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
 * This file declares the internal C API implementation of a ND rectangle
 */

#ifndef TILEDB_CAPI_NDRECTANGLE_INTERNAL_H
#define TILEDB_CAPI_NDRECTANGLE_INTERNAL_H

#include "tiledb/api/c_api_support/argument_validation.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/array_schema/ndrectangle.h"

struct tiledb_ndrectangle_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"tiledb_ndrectangle_t"};

 private:
  std::shared_ptr<tiledb::sm::NDRectangle> ndrectangle_;

 public:
  /**
   * Default constructor doesn't make sense
   */
  tiledb_ndrectangle_handle_t() = delete;

  /**
   * Constructs a handle with a NDRectangle instance.
   * @param memory_tracker The memory tracker to use in the internal NDRectangle
   * @param domain The ArraySchema domain used for internal validations
   */
  explicit tiledb_ndrectangle_handle_t(
      shared_ptr<tiledb::sm::MemoryTracker> memory_tracker,
      shared_ptr<tiledb::sm::Domain> domain)
      : ndrectangle_{make_shared<tiledb::sm::NDRectangle>(
            HERE(), memory_tracker, domain)} {
  }

  /**
   * Ordinary constructor.
   * @param ndrectangle An internal tiledb NDRectangle instance
   */
  explicit tiledb_ndrectangle_handle_t(
      shared_ptr<tiledb::sm::NDRectangle> ndrectangle)
      : ndrectangle_(ndrectangle) {
  }

  /**
   * Get the internal instance of NDRectangle
   * @returns The internal NDRectangle
   */
  [[nodiscard]] inline shared_ptr<tiledb::sm::NDRectangle> ndrectangle() const {
    return ndrectangle_;
  }

  /**
   * Set the internal NDRectangle instance to be managed by this handle
   * @param ndr The NDRectangle to manage
   */
  inline void set_ndrectangle(shared_ptr<tiledb::sm::NDRectangle> ndr) {
    ndrectangle_ = ndr;
  }
};

#endif  // TILEDB_CAPI_NDRECTANGLE_INTERNAL_H
