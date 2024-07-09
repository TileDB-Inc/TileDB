/**
 * @file object_iter.h
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
 * This file defines object iterator functions.
 */

#ifndef TILEDB_OBJECT_ITER_H
#define TILEDB_OBJECT_ITER_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;

namespace tiledb::sm {

/** Enables iteration over TileDB objects in a path. */
struct ObjectIter {
 public:
  /**
   * There is a one-to-one correspondence between `expanded_` and `objs_`.
   * An `expanded_` value is `true` if the corresponding `objs_` path
   * has been expanded to the paths it contains in a post ored traversal.
   * This is not used in a preorder traversal.
   */
  std::list<bool> expanded_;

  /** The next URI in string format. */
  std::string next_;

  /** The next objects to be visited. */
  std::list<URI> objs_;

  /** The traversal order of the iterator. */
  WalkOrder order_;

  /** `True` if the iterator will recursively visit the directory tree. */
  bool recursive_;
};

/**
 * Creates a new object iterator for the input path. The iteration
 * in this case will be recursive in the entire directory tree rooted
 * at `path`.
 *
 * @param resources The context resources.
 * @param path The path the iterator will target at.
 * @param order The traversal order of the iterator.
 * @return The created object iterator.
 */
ObjectIter* object_iter_begin(
    ContextResources& resources, const char* path, WalkOrder order);

/**
 * Creates a new object iterator for the input path. The iteration will
 * not be recursive, and only the children of `path` will be visited.
 *
 * @param resources The context resources.
 * @param path The path the iterator will target at.
 * @return The created object iterator.
 */
ObjectIter* object_iter_begin(ContextResources& resources, const char* path);

/** Frees the object iterator. */
void object_iter_free(ObjectIter* obj_iter);

/**
 * Retrieves the next object path and type.
 *
 * @param resources The context resources.
 * @param obj_iter The object iterator.
 * @param path The object path that is retrieved.
 * @param type The object type that is retrieved.
 * @param has_next True if an object path was retrieved and false otherwise.
 * @return Status
 */
Status object_iter_next(
    ContextResources& resources,
    ObjectIter* obj_iter,
    const char** path,
    ObjectType* type,
    bool* has_next);

/**
 * Retrieves the next object in the post-order traversal.
 *
 * @param resources The context resources.
 * @param obj_iter The object iterator.
 * @param path The object path that is retrieved.
 * @param type The object type that is retrieved.
 * @param has_next True if an object path was retrieved and false otherwise.
 * @return Status
 */
Status object_iter_next_postorder(
    ContextResources& resources,
    ObjectIter* obj_iter,
    const char** path,
    ObjectType* type,
    bool* has_next);

/**
 * Retrieves the next object in the post-order traversal.
 *
 * @param resources The context resources.
 * @param obj_iter The object iterator.
 * @param path The object path that is retrieved.
 * @param type The object type that is retrieved.
 * @param has_next True if an object path was retrieved and false otherwise.
 * @return Status
 */
Status object_iter_next_preorder(
    ContextResources& resources,
    ObjectIter* obj_iter,
    const char** path,
    ObjectType* type,
    bool* has_next);

}  // namespace tiledb::sm

#endif  // TILEDB_OBJECT_ITER_H
