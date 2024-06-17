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
 * This file implements object iterator functions.
 */

#include "tiledb/sm/object/object_iter.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/object/object.h"

namespace tiledb::sm {

class ObjectIterException : public StatusException {
 public:
  explicit ObjectIterException(const std::string& message)
      : StatusException("ObjectIter", message) {
  }
};

ObjectIter* object_iter_begin(
    ContextResources& resources, const char* path, WalkOrder order) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    throw ObjectIterException(
        "Cannot create object iterator; Invalid input path");
  }

  // Get all contents of path
  std::vector<URI> uris;
  throw_if_not_ok(resources.vfs().ls(path_uri, &uris));

  // Create a new object iterator
  ObjectIter* obj_iter = tdb_new(ObjectIter);
  obj_iter->order_ = order;
  obj_iter->recursive_ = true;

  // Include the uris that are TileDB objects in the iterator state
  ObjectType obj_type;
  for (auto& uri : uris) {
    try {
      obj_type = object_type(resources, uri);
      if (obj_type != ObjectType::INVALID) {
        obj_iter->objs_.push_back(uri);
        if (order == WalkOrder::POSTORDER)
          obj_iter->expanded_.push_back(false);
      }
    } catch (...) {
      tdb_delete(obj_iter);
      throw;
    }
  }

  return obj_iter;
}

ObjectIter* object_iter_begin(ContextResources& resources, const char* path) {
  // Sanity check
  URI path_uri(path);
  if (path_uri.is_invalid()) {
    throw ObjectIterException(
        "Cannot create object iterator; Invalid input path");
  }

  // Get all contents of path
  std::vector<URI> uris;
  throw_if_not_ok(resources.vfs().ls(path_uri, &uris));

  // Create a new object iterator
  ObjectIter* obj_iter = tdb_new(ObjectIter);
  obj_iter->order_ = WalkOrder::PREORDER;
  obj_iter->recursive_ = false;

  // Include the uris that are TileDB objects in the iterator state
  ObjectType obj_type;
  for (auto& uri : uris) {
    try {
      obj_type = object_type(resources, uri);
      if (obj_type != ObjectType::INVALID) {
        obj_iter->objs_.push_back(uri);
      }
    } catch (...) {
      tdb_delete(obj_iter);
      throw;
    }
  }

  return obj_iter;
}

void object_iter_free(ObjectIter* obj_iter) {
  tdb_delete(obj_iter);
}

Status object_iter_next(
    ContextResources& resources,
    ObjectIter* obj_iter,
    const char** path,
    ObjectType* type,
    bool* has_next) {
  // Handle case there is no next
  if (obj_iter->objs_.empty()) {
    *has_next = false;
    return Status::Ok();
  }

  // Retrieve next object
  switch (obj_iter->order_) {
    case WalkOrder::PREORDER:
      RETURN_NOT_OK(
          object_iter_next_preorder(resources, obj_iter, path, type, has_next));
      break;
    case WalkOrder::POSTORDER:
      RETURN_NOT_OK(object_iter_next_postorder(
          resources, obj_iter, path, type, has_next));
      break;
  }

  return Status::Ok();
}

Status object_iter_next_postorder(
    ContextResources& resources,
    ObjectIter* obj_iter,
    const char** path,
    ObjectType* type,
    bool* has_next) {
  // Get all contents of the next URI recursively till the bottom,
  // if the front of the list has not been expanded
  if (obj_iter->expanded_.front() == false) {
    uint64_t obj_num;
    do {
      obj_num = obj_iter->objs_.size();
      std::vector<URI> uris;
      throw_if_not_ok(resources.vfs().ls(obj_iter->objs_.front(), &uris));
      obj_iter->expanded_.front() = true;

      // Push the new TileDB objects in the front of the iterator's list
      ObjectType obj_type;
      for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
        obj_type = object_type(resources, *it);
        if (obj_type != ObjectType::INVALID) {
          obj_iter->objs_.push_front(*it);
          obj_iter->expanded_.push_front(false);
        }
      }
    } while (obj_num != obj_iter->objs_.size());
  }

  // Prepare the values to be returned
  URI front_uri = obj_iter->objs_.front();
  obj_iter->next_ = front_uri.to_string();
  *type = object_type(resources, front_uri);
  *path = obj_iter->next_.c_str();
  *has_next = true;

  // Pop the front (next URI) of the iterator's object list
  obj_iter->objs_.pop_front();
  obj_iter->expanded_.pop_front();

  return Status::Ok();
}

Status object_iter_next_preorder(
    ContextResources& resources,
    ObjectIter* obj_iter,
    const char** path,
    ObjectType* type,
    bool* has_next) {
  // Prepare the values to be returned
  URI front_uri = obj_iter->objs_.front();
  obj_iter->next_ = front_uri.to_string();
  *type = object_type(resources, front_uri);
  *path = obj_iter->next_.c_str();
  *has_next = true;

  // Pop the front (next URI) of the iterator's object list
  obj_iter->objs_.pop_front();

  // Return if no recursion is needed
  if (!obj_iter->recursive_)
    return Status::Ok();

  // Get all contents of the next URI
  std::vector<URI> uris;
  throw_if_not_ok(resources.vfs().ls(front_uri, &uris));

  // Push the new TileDB objects in the front of the iterator's list
  ObjectType obj_type;
  for (auto it = uris.rbegin(); it != uris.rend(); ++it) {
    obj_type = object_type(resources, *it);
    if (obj_type != ObjectType::INVALID)
      obj_iter->objs_.push_front(*it);
  }

  return Status::Ok();
}

}  // namespace tiledb::sm
