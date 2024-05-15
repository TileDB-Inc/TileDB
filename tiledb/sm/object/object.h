/**
 * @file object.h
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
 * This file defines the Object class.
 */

#ifndef TILEDB_OBJECT_H
#define TILEDB_OBJECT_H

#include "tiledb/common/common.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Object {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Object() = default;

  /** Destructor. */
  ~Object() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(Object);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Object);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Checks if the input URI represents an array.
   *
   * @param resources the context resources.
   * @param uri the URI to be checked.
   * @return bool
   */
  static bool is_array(ContextResources& resources, const URI& uri);

  /**
   * Checks if the input URI represents a group.
   *
   * @param resources the context resources.
   * @param uri the URI to be checked.
   * @param is_group Set to `true` if the URI is a group and `false`
   *     otherwise.
   * @return Status
   */
  static Status is_group(
      ContextResources& resources, const URI& uri, bool* is_group);
};

}  // namespace tiledb::sm

#endif
