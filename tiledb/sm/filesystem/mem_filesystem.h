/**
 * @file   mem_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines an in-memory filesystem
 */

#ifndef TILEDB_MEMORY_FILESYSTEM_H
#define TILEDB_MEMORY_FILESYSTEM_H

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

// TODO: add docstrings everywhere

class MemFilesystem {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  MemFilesystem();

  /** Destructor. */
  ~MemFilesystem();

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  Status create_dir(const std::string& path);

  Status ls(const std::string& path, std::vector<std::string>* paths) const;

  Status read(
      const std::string& path,
      const uint64_t offset,
      void* buffer,
      const uint64_t nbytes) const;

  Status remove(const std::string& path);

  Status touch(const std::string& path);

  Status write(
      const std::string& path, const void* data, const uint64_t nbytes);

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
  /* ********************************* */

  class FSNode;
  class File;
  class Directory;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
  FSNode* root_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  void delete_rec(FSNode* node);

  Status get_node(const std::string& path, FSNode** node) const;

  static std::vector<std::string> tokenize(
      const std::string& path, const char delim = '/');

  Status touch(const std::string& path, FSNode** new_node);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_MEMORY_FILESYSTEM_H