/**
 * @file   object_iter.cc
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
 This file implements the class ObjectIter.
 */

#include "tiledb/sm/object/object_iter.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/object/object.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/context_resources.h"

namespace tiledb::sm {

class ObjectIterException : public StatusException {
 public:
  explicit ObjectIterException(const std::string& message)
      : StatusException("ObjectIter", message) {
  }
};

ObjectIter::ObjectIter(
    ContextResources& resources,
    const char* path,
    std::optional<WalkOrder> order)
    : resources_(resources)
    , order_(order.value_or(WalkOrder::PREORDER))
    , recursive_(order.has_value()) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    throw ObjectIterException(
        "Cannot create object iterator; Invalid input path");
  }

  // Get all contents of path
  std::vector<URI> uris;
  throw_if_not_ok(resources_.vfs().ls(path_uri, &uris));

  // Include the uris that are TileDB objects in the iterator state
  for (auto& uri : uris) {
    ObjectType obj_type = object_type(resources_, uri);
    if (obj_type != ObjectType::INVALID) {
      objs_.push_back(uri);
      if (recursive_ && order_ == WalkOrder::POSTORDER)
        expanded_.push_back(false);
    }
  }
}

std::tuple<std::string, ObjectType> ObjectIter::next_postorder() {
  // Get all contents of the next URI recursively till the bottom,
  // if the front of the list has not been expanded
  if (expanded_.front() == false) {
    uint64_t obj_num;
    do {
      obj_num = objs_.size();
      std::vector<URI> uris;
      throw_if_not_ok(resources_.vfs().ls(objs_.front(), &uris));
      expanded_.front() = true;

      // Push the new TileDB objects in the front of the iterator's list
      for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
        ObjectType obj_type = object_type(resources_, *it);
        if (obj_type != ObjectType::INVALID) {
          objs_.push_front(*it);
          expanded_.push_front(false);
        }
      }
    } while (obj_num != objs_.size());
  }

  // Prepare the values to be returned
  URI front_uri = objs_.front();

  // Pop the front (next URI) of the iterator's object list
  objs_.pop_front();
  expanded_.pop_front();

  return {front_uri.to_string(), object_type(resources_, front_uri)};
}

std::tuple<std::string, ObjectType> ObjectIter::next_preorder() {
  // Prepare the values to be returned
  URI front_uri = objs_.front();

  // Pop the front (next URI) of the iterator's object list
  objs_.pop_front();

  // Perform recursion if needed
  if (recursive_) {
    // Get all contents of the next URI
    std::vector<URI> uris;
    throw_if_not_ok(resources_.vfs().ls(front_uri, &uris));

    // Push the new TileDB objects in the front of the iterator's list
    for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
      ObjectType obj_type = object_type(resources_, *it);
      if (obj_type != ObjectType::INVALID)
        objs_.push_front(*it);
    }
  }

  return {front_uri.to_string(), object_type(resources_, front_uri)};
}

}  // namespace tiledb::sm
