/**
 * @file   filestat.h
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
 * This file implements class FileStat.
 */

#ifndef TILEDB_FILESTAT_H
#define TILEDB_FILESTAT_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/uri.h"

#include <string>

namespace tiledb {
namespace sm {

class FileStat {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  FileStat();

  /**
   * Constructor.
   *
   * @param path URI that identifies a filesystem entry.
   */
  explicit FileStat(const URI& path);

  /**
   * Constructor.
   *
   * @param path URI that identifies a filesystem entry.
   * @param size The size in bytes stored for this entry.
   */
  explicit FileStat(const URI& path, optional<uint64_t> size);

  /** Destructor. */
  ~FileStat();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** @return The URI identifying the filesystem entry. */
  URI path() const;

  /** @return The size in bytes of the filesystem entry. */
  optional<uint64_t> size() const;

  /** For comparing entries alphanumerically. */
  bool operator==(const FileStat& uri) const;

  /** For comparing entries  alphanumerically. */
  bool operator!=(const FileStat& uri) const;

  /** For comparing entries  alphanumerically. */
  bool operator<(const FileStat& uri) const;

  /** For comparing entries  alphanumerically. */
  bool operator>(const FileStat& uri) const;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The URI of the filesystem entry */
  URI path_;

  /** The size of a filesystem entry in bytes */
  optional<uint64_t> size_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILESTAT_H
