/**
 * @file   failing_fs.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file defines the FailingFS class.
 */

#ifndef TILEDB_FAILINGFS_H
#define TILEDB_FAILINGFS_H

#include "tiledb/common/assert.h"
#include "tiledb/sm/filesystem/filesystem_base.h"

namespace tiledb::sm {
/**
 * Implements FilesystemBase by always throwing FilesystemException.
 */
class FailingFS : public FilesystemBase {
 public:
  /**
   * Constructor.
   *
   * @param uri_scheme The URI scheme this object will claim to support.
   * @param message The message of the exceptions this object's operations will
   * throw.
   */
  FailingFS(const std::string& uri_scheme, const std::string& message = "")
      : uri_scheme_(uri_scheme)
      , message_(message) {
    iassert(!uri_scheme.empty());
  }

  bool supports_uri(const URI& uri) const override {
    return uri.backend_name() == uri_scheme_;
  }

  virtual void create_dir(const URI&) const override {
    throw get_exception();
  }

  virtual void touch(const URI&) const override {
    throw get_exception();
  }

  virtual bool is_dir(const URI&) const override {
    throw get_exception();
  }

  virtual bool is_file(const URI&) const override {
    throw get_exception();
  }

  virtual void remove_dir(const URI&) const override {
    throw get_exception();
  }

  virtual void remove_file(const URI&) const override {
    throw get_exception();
  }

  virtual uint64_t file_size(const URI&) const override {
    throw get_exception();
  }

  virtual std::vector<tiledb::common::filesystem::directory_entry>
  ls_with_sizes(const URI&, bool) const override {
    throw get_exception();
  }

  virtual LsObjects ls_filtered(const URI&, ResultFilter, bool) const override {
    throw get_exception();
  }

  virtual LsObjects ls_filtered_v2(
      const URI&, ResultFilterV2, bool) const override {
    throw get_exception();
  }

  virtual void move_file(const URI&, const URI&) const override {
    throw get_exception();
  }

  virtual void move_dir(const URI&, const URI&) const override {
    throw get_exception();
  }

  virtual void copy_file(const URI&, const URI&) override {
    throw get_exception();
  }

  virtual void copy_dir(const URI&, const URI&) override {
    throw get_exception();
  }

  virtual uint64_t read(const URI&, uint64_t, void*, uint64_t) override {
    throw get_exception();
  }

  virtual void flush(const URI&, bool) override {
    throw get_exception();
  }

  void sync(const URI&) const override {
    throw get_exception();
  }

  virtual void write(const URI&, const void*, uint64_t, bool) override {
    throw get_exception();
  }

  virtual bool is_bucket(const URI&) const override {
    throw get_exception();
  }

  virtual bool is_empty_bucket(const URI&) const override {
    throw get_exception();
  }

  virtual void create_bucket(const URI&) const override {
    throw get_exception();
  }

  virtual void remove_bucket(const URI&) const override {
    throw get_exception();
  }

  virtual void empty_bucket(const URI&) const override {
    throw get_exception();
  }

 private:
  const std::string uri_scheme_, message_;

  FilesystemException get_exception() const {
    return FilesystemException(message_);
  }
};
}  // namespace tiledb::sm
#endif  // TILEDB_FAILINGFS_H
