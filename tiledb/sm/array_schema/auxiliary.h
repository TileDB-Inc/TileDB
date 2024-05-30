/**
 * @file   auxiliary.h
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
 * This file defines auxiliary functions to support class ArraySchema.
 *
 * This file exists solely as an intermediary solution to resolve build issues
 * with `capi_context_stub` when migrating `store_array_schema` out of
 * `StorageManagerCanonical`. At present, there is an interdependency chain
 * with `generic_tile_io` which must be resolved before this function can be
 * placed into `class ArraySchema`. As such, this file is _intentionally_ left
 * out of the `array_schema` object library and _must_ remain that way. Once
 * these build issues are resolved, this file should be removed and replaced by
 * a member function, `ArraySchema::store`.
 */

#ifndef TILEDB_AUXILIARY_ARRAY_SCHEMA_H
#define TILEDB_AUXILIARY_ARRAY_SCHEMA_H

#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ********************************* */
/*                API                */
/* ********************************* */

/**
 * Stores an array schema into persistent storage.
 *
 * @param resources The context resources.
 * @param array_schema The array schema to be stored.
 * @param encryption_key The encryption key to use.
 * @return Status
 */
Status store_array_schema(
    ContextResources& resources,
    const shared_ptr<ArraySchema>& array_schema,
    const EncryptionKey& encryption_key);

}  // namespace tiledb::sm

#endif  // TILEDB_AUXILIARY_ARRAY_SCHEMA_H
