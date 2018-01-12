/**
 * @file   tiledb_cpp_api_vfs.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB VFS object.
 */

#ifndef TILEDB_CPP_API_VFS_H
#define TILEDB_CPP_API_VFS_H

#include "tiledb.h"
#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_deleter.h"

#include <memory>

namespace tdb {

/**
 * Implements a virtual filesystem that enables performing directory/file
 * operations with a unified API on different filesystems, such as local
 * posix/windows, HDFS, AWS S3, etc.
 */
class VFS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param ctx A TileDB context.
   */
  explicit VFS(const Context& ctx);

  /**
   * Constructor.
   *
   * @param ctx TileDB context.
   * @param config TileDB config.
   */
  VFS(const Context& ctx, const Config& config);
  VFS(const VFS&)  = default;
  VFS(VFS&&)  = default;
  VFS &operator=(const VFS &) = default;
  VFS &operator=(VFS&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Creates an object-store bucket with the input URI. */
  void create_bucket(const std::string& uri) const;

  /** Deletes an object-store bucket with the input URI. */
  void remove_bucket(const std::string& uri) const;

  /** Checks if an object-store bucket with the input URI exists. */
  bool is_bucket(const std::string& uri) const;

  /** Creates a directory with the input URI. */
  void create_dir(const std::string& uri) const;

  /** Checks if a directory with the input URI exists. */
  bool is_dir(const std::string& uri) const;

  /** Removes a directory (recursively) with the input URI. */
  void remove_dir(const std::string& uri) const;

  /** Checks if a file with the input URI exists. */
  bool is_file(const std::string& uri) const;

  /** Deletes a file with the input URI. */
  void remove_file(const std::string& uri) const;

  /** Retrieves the size of a file with the input URI. */
  uint64_t file_size(const std::string& uri) const;

  /** Renames a TileDB path from an old URI to a new URI. */
  void move(const std::string& old_uri, const std::string& new_uri) const;

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset in the file where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   */
  void read(
      const std::string& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes) const;

  /**
   * Writes the contents of a buffer into a file. Note that this
   * function only **appends** data at the end of the file. If the
   * file does not exist, it will be created.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param nbytes Number of bytes to write.
   */
  void write(const std::string& uri, const void* buffer, uint64_t nbytes) const;

  /**
   * Syncs (flushes) a file with the input URI. This is important to call
   * before starting to read from the file.
   *
   * @note Specifically for S3, this function **finalizes** the file, in the
   *     sense that from this point and onwards it becomes immutable. Any
   *     attempt to write to this file again will result in **overwriting**
   *     the old data.
   */
  void sync(const std::string& uri) const;

  /** Checks if a given storage filesystem backend is supported. */
  bool supports_fs(tiledb_filesystem_t fs) const;

  /** Touches a file with the input URI, i.e., creates a new empty file. */
  void touch(const std::string& uri) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** A deleter wrapper. */
  impl::Deleter deleter_;

  /** The TileDB C VFS object. */
  std::shared_ptr<tiledb_vfs_t> vfs_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Creates a TileDB C VFS object, using the input config. */
  void create_vfs(tiledb_config_t* config);
};

}  // namespace tdb

#endif  // TILEDB_CPP_API_VFS_H
