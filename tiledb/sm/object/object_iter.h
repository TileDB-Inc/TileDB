/**
 * @file   object_iter.h
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
 This file defines the class ObjectIter.
 */

#include <list>
#include <string>

#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/uri.h"

namespace tiledb::sm {

class ContextResources;

/** Enables iteration over TileDB objects in a path. */
class ObjectIter {
  /**
   * Creates a new object iterator for the input path.
   *
   * @param resources The context resources to use.
   * @param path The path the iterator will target at.
   * @param order The traversal order of the iterator, or nullopt if the
   * traversal will not be recursive.
   */
 public:
  ObjectIter(
      ContextResources& resources,
      const char* path,
      std::optional<WalkOrder> order = {});

  /**
   * Visits the next item.
   *
   * @returns The next item's path and type, or nullopt if there are no more
   * items.
   */
  std::optional<std::tuple<std::string, ObjectType>> next() {
    // Handle case there is no next
    if (objs_.empty()) {
      return std::nullopt;
    }

    // Retrieve next object
    if (order_ == WalkOrder::PREORDER) {
      return next_preorder();
    } else {
      return next_postorder();
    }
  }

 private:
  std::tuple<std::string, ObjectType> next_preorder();

  std::tuple<std::string, ObjectType> next_postorder();

  /** The context resources to use. */
  ContextResources& resources_;
  /**
   * There is a one-to-one correspondence between `expanded_` and `objs_`.
   * An `expanded_` value is `true` if the corresponding `objs_` path
   * has been expanded to the paths it contains in a post ored traversal.
   * This is not used in a preorder traversal.
   */
  std::list<bool> expanded_;
  /** The next objects to be visited. */
  std::list<URI> objs_;
  /** The traversal order of the iterator. */
  WalkOrder order_;
  /** `True` if the iterator will recursively visit the directory tree. */
  bool recursive_;
};

}  // namespace tiledb::sm
