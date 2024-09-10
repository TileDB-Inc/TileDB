/**
 * @file   array_schema_operations.h
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
 * This file defines I/O operations which support class ArraySchema.
 */

#ifndef TILEDB_ARRAY_SCHEMA_OPERATIONS_H
#define TILEDB_ARRAY_SCHEMA_OPERATIONS_H

#include "tiledb/common/common.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb::sm {

class ArraySchema;
class ContextResources;
class EncryptionKey;

/* ********************************* */
/*                API                */
/* ********************************* */

/**
 * Serializes the array schema object into a buffer.
 *
 * @param serializer The object the array schema is serialized into.
 * @param array_schema The array schema to be serialized.
 */
void serialize_array_schema(
    Serializer& serializer, const ArraySchema& array_schema);

/**
 * Stores an array schema into persistent storage.
 *
 * @section Maturity Notes
 * This function currently implements defective behavior.
 * Storing an array schema that does not have a URI attached to it should
 * _not_ succeed. Users should be aware of this behavior and avoid storage of
 * schemas with empty URIs.
 * This defect is scheduled for fix asap, but must be documented in the interim.
 *
 * @param resources The context resources.
 * @param array_schema The array schema to be stored.
 * @param encryption_key The encryption key to use.
 */
void store_array_schema(
    ContextResources& resources,
    const shared_ptr<ArraySchema>& array_schema,
    const EncryptionKey& encryption_key);

}  // namespace tiledb::sm

#endif  // TILEDB_ARRAY_SCHEMA_OPERATIONS_H
