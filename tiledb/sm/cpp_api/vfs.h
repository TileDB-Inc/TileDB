/**
 * @file   tiledb_cpp_api_vfs.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for the TileDB VFS object.
 */

#ifndef TILEDB_CPP_API_VFS_H
#define TILEDB_CPP_API_VFS_H

#include "context.h"
#include "deleter.h"
#include "tiledb.h"

#include <memory>

namespace tiledb {
namespace impl {
class VFSFilebuf;
}

/**
 * Implements a virtual filesystem that enables performing directory/file
 * operations with a unified API on different filesystems, such as local
 * posix/windows, HDFS, AWS S3, etc.
 */
class TILEDB_EXPORT VFS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  using filebuf = impl::VFSFilebuf;

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
  VFS(const VFS&) = default;
  VFS(VFS&&) = default;
  VFS& operator=(const VFS&) = default;
  VFS& operator=(VFS&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Creates an object-store bucket with the input URI. */
  void create_bucket(const std::string& uri) const;

  /** Deletes an object-store bucket with the input URI. */
  void remove_bucket(const std::string& uri) const;

  /** Checks if an object-store bucket with the input URI exists. */
  bool is_bucket(const std::string& uri) const;

  /** Empty a bucket **/
  void empty_bucket(const std::string& bucket) const;

  /** Check if a bucket is empty **/
  bool is_empty_bucket(const std::string& bucket) const;

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

  /** Renames a TileDB file from an old URI to a new URI. */
  void move_file(const std::string& old_uri, const std::string& new_uri) const;

  /** Renames a TileDB directory from an old URI to a new URI. */
  void move_dir(const std::string& old_uri, const std::string& new_uri) const;

  /** Touches a file with the input URI, i.e., creates a new empty file. */
  void touch(const std::string& uri) const;

  /** Get the underlying context **/
  const Context& context() const;

  /** Get the underlying tiledb object **/
  std::shared_ptr<tiledb_vfs_t> ptr() const;

  /** Get the config **/
  Config config() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** TileDB context. */
  std::reference_wrapper<const Context> ctx_;

  /** Config **/
  Config config_;

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

}  // namespace tiledb

#endif  // TILEDB_CPP_API_VFS_H
