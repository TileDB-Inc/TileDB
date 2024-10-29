/**
 * @file   array_operations.h
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
 * This file defines I/O operations which support class Array.
 *
 */

#ifndef TILEDB_ARRAY_OPERATIONS_H
#define TILEDB_ARRAY_OPERATIONS_H

#include <vector>

#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb::sm {

class ArraySchema;
class Context;
class ContextResources;
class OpenedArray;
class QueryCondition;
class UpdateValue;

/**
 * Loads the delete and update conditions from storage.
 *
 * @param resources The context resources.
 * @param opened_array The opened array whose conditions are getting loaded.
 * @return vector of the conditions, vector of the update values.
 */
tuple<std::vector<QueryCondition>, std::vector<std::vector<UpdateValue>>>
load_delete_and_update_conditions(
    ContextResources& resources, const OpenedArray& opened_array);

/**
 * Loads an enumeration into a schema.
 * Used to implement `tiledb_array_schema_get_enumeration*` APIs.
 *
 * @param ctx
 * @param enmr_name the requested enumeration
 * @param schema the target schema
 */
void load_enumeration_into_schema(
    Context& ctx, const std::string& enmr_name, ArraySchema& array_schema);

}  // namespace tiledb::sm

#endif  // TILEDB_ARRAY_OPERATIONS_H
