/**
 * @file   win.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file includes declarations of Windows filesystem functions.
 */

#ifndef TILEDB_WIN_FILESYSTEM_H
#define TILEDB_WIN_FILESYSTEM_H
#ifdef _WIN32

#include <sys/types.h>

#include <string>
#include <vector>

#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem.h"

using namespace tiledb::common;

namespace tiledb::sm::filesystem {

/** Typedef this here so we don't have to include Windows.h */
typedef void* HANDLE;

class Win : public Filesystem {
 public:
  explicit Win(ContextResources& resources);

  ~Win() override = default;

  /**
   * Check if a URI refers to a directory.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a directory or not.
   */
  virtual bool is_dir(const URI& uri) const override;

  /**
   * Check if a URI refers to a file.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a file or not.
   */
  virtual bool is_file(const URI& uri) const override;

  /**
   * Create a directory.
   *
   * @param uri The URI of the directory.
   */
  virtual void create_dir(const URI& uri) override;

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  virtual std::vector<FilesystemEntry>
  ls(const URI& parent) const override;

  /**
   * Copy a directory.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  virtual void copy_dir(const URI& src_uri, const URI& tgt_uri) override;

  /**
   * Recursively remove a directory.
   *
   * @param uri The uri of the directory to be removed.
   */
  virtual void remove_dir(const URI& uri) override;

  /**
   * Create an empty file.
   *
   * @param uri The URI of the file.
   */
  virtual void touch(const URI& uri) override;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @return uint64_t The size of the file in bytes.
   */
  virtual uint64_t file_size(const URI& uri) const override;

  /**
   * Write the contents of a buffer to a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   */
  virtual void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size) override;

  /**
   * Read from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   */
  virtual void read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes) override;

  /**
   * Syncs (flushes) a file.
   *
   * @param uri The URI of the file.
   */
  virtual void sync(const URI& uri) override;

  /**
   * Copy a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  virtual void copy_file(const URI& src_uri, const URI& tgt_uri) override;

  /**
   * Rename a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  virtual void move_file(const URI& src_uri, const URI& tgt_uri) override;

  /**
   * Delete a file.
   *
   * @param uri The URI of the file to remove.
   */
  virtual void remove_file(const URI& uri) override;
};

}  // namespace sm
}  // namespace tiledb

#endif  // _WIN32

#endif  // TILEDB_WIN_FILESYSTEM_H
