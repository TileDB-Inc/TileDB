/**
 * @file   directory_entry.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements class directory_entry.
 */

#ifndef TILEDB_DIRECTORY_ENTRY_H
#define TILEDB_DIRECTORY_ENTRY_H

#include "path.h"

#include <cstdint>

namespace tiledb {
namespace common {
namespace filesystem {

class directory_entry {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  directory_entry() = default;

  /**
   * Copy constructor defaulted
   *
   * @param dentry directory_entry to copy from
   */
  directory_entry(const directory_entry& dentry) = default;

  /**
   * Move constructor defaulted
   *
   * @param dentry directory_entry to move from
   */
  directory_entry(directory_entry&& dentry) = default;

  /**
   * Constructor
   *
   * @param p The path of the entry
   * @param size The size of the filesystem entry
   */
  directory_entry(const std::string& p, uint64_t size);

  /** Destructor. */
  ~directory_entry() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Copy assignment operator
   *
   * @param dentry directory_entry to move from
   */
  directory_entry& operator=(const directory_entry& dentry) = default;

  /**
   * Move assignment operator
   *
   * @param dentry directory_entry to move from
   */
  directory_entry& operator=(directory_entry&& dentry) = default;

  /**
   * Assigns new content to the entry.
   */
  void assign(const class path& p);

  /**
   * @return The full path the directory_entry refers to
   */
  const class path& path() const;

  /**
   * @return The size of the file to which the directory entry refers
   */
  uint64_t file_size() const;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The URI of the filesystem entry */
  class path path_;

  /** The size of a filesystem entry */
  uint64_t size_;
};

}  // namespace filesystem
}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_DIRECTORY_ENTRY_H
