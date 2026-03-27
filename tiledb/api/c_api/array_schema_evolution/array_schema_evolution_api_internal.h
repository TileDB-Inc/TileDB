/**
 * @file
 * tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_internal.h
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
 * This file declares the internals of the array schema evolution section of
 * the C API.
 */

#ifndef TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_INTERNAL_H
#define TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_INTERNAL_H

#include "array_schema_evolution_api_experimental.h"

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"

/** Handle `struct` for API array schema evolution objects. */
struct tiledb_array_schema_evolution_t : public tiledb::api::CAPIHandle {
  /** Type name */
  static constexpr std::string_view object_type_name{"array_schema_evolution"};

 private:
  shared_ptr<tiledb::sm::ArraySchemaEvolution> array_schema_evolution_;

 public:
  explicit tiledb_array_schema_evolution_t(
      shared_ptr<tiledb::sm::MemoryTracker> memory_tracker)
      : array_schema_evolution_(
            make_shared<tiledb::sm::ArraySchemaEvolution>(
                HERE(), memory_tracker)) {
  }

  explicit tiledb_array_schema_evolution_t(
      shared_ptr<tiledb::sm::ArraySchemaEvolution> ase)
      : array_schema_evolution_(ase) {
  }

  [[nodiscard]] shared_ptr<tiledb::sm::ArraySchemaEvolution>
  array_schema_evolution() const {
    return array_schema_evolution_;
  }
};

#endif  // TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_INTERNAL_H
