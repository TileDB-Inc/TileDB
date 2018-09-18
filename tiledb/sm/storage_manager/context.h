/**
 * @file   context.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class Context.
 */

#ifndef TILEDB_CONTEXT_H
#define TILEDB_CONTEXT_H

#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <mutex>

namespace tiledb {
namespace sm {

/**
 * This class manages the context for the C API, wrapping a
 * storage manager object.
 * */
class Context {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Context();

  /** Destructor. */
  ~Context();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Initializes the context with the input config. */
  Status init(Config* config);

  /** Returns the last error status. */
  Status last_error();

  /** Saves the input status. */
  void save_error(const Status& st);

  /** Returns the storage manager. */
  StorageManager* storage_manager() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The last error occurred. */
  Status last_error_;

  /** A mutex for thread-safety. */
  std::mutex mtx_;

  /** The storage manager. */
  StorageManager* storage_manager_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONTEXT_H
