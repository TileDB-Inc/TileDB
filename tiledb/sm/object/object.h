/**
 * @file   object.h
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
 This file defines functions to work with TileDB objects (arrays and groups).
 */

#include <list>
#include <string>

#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/uri.h"

namespace tiledb::sm {

class ContextResources;

/** Removes a TileDB object (group, array). */
void object_remove(ContextResources& resources, const char* path);

/**
 * Renames a TileDB object (group, array). If
 * `new_path` exists, `new_path` will be overwritten.
 */
void object_move(
    ContextResources& resources, const char* old_path, const char* new_path);

/**
 * Checks if the input URI represents an array.
 *
 * @param uri The URI to be checked.
 * @return bool
 */
bool is_array(ContextResources& resources, const URI& uri);

/**
 * Checks if the input URI represents a group.
 *
 * @param uri The URI to be checked.
 * @return bool
 */
bool is_group(ContextResources& resources, const URI& uri);

/**
 * Returns the tiledb object type
 *
 * @param uri Path to TileDB object resource
 * @return ObjectType
 */
ObjectType object_type(ContextResources& resources, const URI& uri);
}  // namespace tiledb::sm
