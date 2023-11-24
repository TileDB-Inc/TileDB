/**
 * @file   temporary_local_directory.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares class TemporaryLocalDirectory.
 */

#ifndef TILEDB_TEMPORARY_LOCAL_DIRECTORY_H
#define TILEDB_TEMPORARY_LOCAL_DIRECTORY_H

#include "tiledb/sm/filesystem/vfs.h"

namespace tiledb::sm {

/**
 * Maintenance of a local, unique directory.
 *
 * This class will create a randomly-seeded directory on the local filesystem
 * with an optional prefix. Upon destruction of the object, the local directory
 * is removed. The path to the unique directory may be accessed for additional
 * reads / writes.
 */
class TemporaryLocalDirectory {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor which will create a unique local directory. */
  TemporaryLocalDirectory(std::string prefix = {});

  /** Destructor which will remove the directory. */
  ~TemporaryLocalDirectory();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Return the path of the unique directory. */
  const std::string& path();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The path of the unique directory. */
  std::string path_;
};
}  // namespace tiledb::sm

#endif  // TILEDB_TEMPORARY_LOCAL_DIRECTORY_H
