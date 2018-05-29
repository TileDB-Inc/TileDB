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

#include <functional>
#include <locale>
#include <memory>
#include <streambuf>

namespace tiledb {

class VFS;

namespace impl {

/**
 * @brief
 * Stream buffer for a Tiledb VFS.
 *
 * @details
 * This is unbuffered; each write is directly dispatched to TileDB. As such
 * it is recommended to issue fewer, larger, writes.
 *
 * **Example:**
 *
 * @code{.cpp}
 *   tiledb::Context ctx;
 *   tiledb::VFS vfs(ctx);
 *   tiledb::VFSFilebuf buf(vfs);
 *   buf.open("file.txt", std::ios::out | std::ios::app);
 *   std::ostream os(&buf);
 *   os << "abcdefghijklmnopqrstuvwxyz";
 * @endcode
 */
class VFSFilebuf : public std::streambuf {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param vfs Tiledb VFS
   */
  explicit VFSFilebuf(const VFS& vfs);
  VFSFilebuf(const VFSFilebuf& buf) = default;
  VFSFilebuf(VFSFilebuf&& buf) = default;
  VFSFilebuf& operator=(const VFSFilebuf&) = default;
  VFSFilebuf& operator=(VFSFilebuf&& o) = default;
  ~VFSFilebuf() override {
    close();
  }

  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  /**
   * Open a file.
   *
   * @param uri
   * @param openmode Must be either std::ios::in or std::ios::out
   */
  VFSFilebuf* open(
      const std::string& uri, std::ios::openmode openmode = std::ios::in);

  /** Check if a file is open **/
  bool is_open() const {
    return uri_ != "";
  }

  /** Close a file. **/
  VFSFilebuf* close();

  /** Current opened URI. **/
  const std::string& get_uri() const {
    return uri_;
  }

 protected:
  /* ********************************* */
  /*       PROTECTED POSITIONING       */
  /* ********************************* */

  /**
   * Seek a position relative to another.
   *
   * @param offset Offset from seekdir
   * @param seekdir beg, curr, or end
   * @param openmode File openmode, accepts app and/or out
   */
  std::streambuf::pos_type seekoff(
      off_type offset,
      std::ios::seekdir seekdir,
      std::ios::openmode openmode) override {
    // No seek in write mode
    if (openmode & std::ios::app || openmode & std::ios::out) {
      return std::streampos(std::streamoff(-1));
    }

    uint64_t fsize = file_size();

    // Note logic is to get rid of signed/unsigned comparison errors
    switch (seekdir) {
      case std::ios::beg: {
        if (offset < 0 || static_cast<uint64_t>(offset) > fsize) {
          return std::streampos(std::streamoff(-1));
        }
        offset_ = static_cast<uint64_t>(offset);
        break;
      }
      case std::ios::cur: {
        if (offset + offset_ > fsize ||
            (offset < 0 && static_cast<uint64_t>(std::abs(offset)) > offset_)) {
          return std::streampos(std::streamoff(-1));
        }
        offset_ = static_cast<uint64_t>(offset + offset_);
        break;
      }
      case std::ios::end: {
        if (offset + fsize > fsize ||
            (offset < 0 && static_cast<uint64_t>(std::abs(offset)) > fsize)) {
          return std::streampos(std::streamoff(-1));
        }
        offset_ = static_cast<uint64_t>(offset + fsize);
        break;
      }
      default:
        return std::streampos(std::streamoff(-1));
    }
    // This returns a static constant
    return std::streampos(offset);
  }

  /**
   * Seek a byte position in the file.
   *
   * @param pos
   * @param openmode in, and/or app (append)
   */
  pos_type seekpos(pos_type pos, std::ios::openmode openmode) override {
    return seekoff((off_type)pos, std::ios::beg, openmode);
  }

  /* ********************************* */
  /*           PROTECTED GET           */
  /* ********************************* */

  /**
   * @return Number of bytes left between current pos and end of file
   */
  std::streamsize showmanyc() override {
    return std::streamsize(file_size() - offset_);
  }

  /**
   * Get N characters at current position.
   *
   * @param s Data pointer
   * @param n Number of bytes to get
   * @return Number of bytes read
   */
  std::streamsize xsgetn(char_type* s, std::streamsize n) override;

  /**
   * Get a character in the file. Note this function should rarely be used
   * on non-local URI's, since each will dispatch a request.
   *
   * @return character at current pos
   */
  int_type underflow() override {
    char_type c;
    if (xsgetn(&c, sizeof(c)) != traits_type::eof()) {
      --offset_;
      return traits_type::to_int_type(c);
    }
    return traits_type::eof();
  }

  /**
   * Get a character in the file and advance position.
   *
   * @return character at current pos
   */
  int_type uflow() override {
    char_type c;
    if (xsgetn(&c, sizeof(c)) != traits_type::eof()) {
      return traits_type::to_int_type(c);
    }
    return traits_type::eof();
  }

  /* ********************************* */
  /*           PROTECTED PUT           */
  /* ********************************* */

  /**
   * Put N characters at the end of the file.
   *
   * @param s Data pointer
   * @param n Number of bytes to write
   * @return Number of bytes written
   */
  std::streamsize xsputn(const char_type* s, std::streamsize n) override;

  /**
   * Put a character in the file. Note this function should rarely be used
   * on non-local URI's, since each will dispatch a request.
   *
   * @param c character to put
   * @return character that was put
   */
  int_type overflow(int_type c) override {
    if (c != traits_type::eof()) {
      char_type ch = traits_type::to_char_type(c);
      if (xsputn(&ch, sizeof(ch)) != traits_type::eof())
        return traits_type::to_int_type(ch);
    }
    return traits_type::eof();
  }

 private:
  /* ********************************* */
  /*              PRIVATE              */
  /* ********************************* */

  /** Get the file size of the file in bytes.
   * Return 0 if file does not exist. **/
  uint64_t file_size() const;

  /** Underlying VFS **/
  std::reference_wrapper<const VFS> vfs_;

  /** File handle **/
  std::shared_ptr<tiledb_vfs_fh_t> fh_;

  /** Deleter for fh_ **/
  impl::Deleter deleter_;

  /** File URI **/
  std::string uri_ = "";

  /** Current offset from the file beginning **/
  uint64_t offset_ = 0;
};

}  // namespace impl

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

  using filebuf = impl::VFSFilebuf;

  /**
   * Constructor.
   *
   * @param ctx A TileDB context.
   */
  explicit VFS(const Context& ctx)
      : ctx_(ctx) {
    create_vfs(nullptr);
  }

  /**
   * Constructor.
   *
   * @param ctx TileDB context.
   * @param config TileDB config.
   */
  VFS(const Context& ctx, const Config& config)
      : ctx_(ctx)
      , config_(config) {
    create_vfs(config.ptr().get());
  }

  VFS(const VFS&) = default;
  VFS(VFS&&) = default;
  VFS& operator=(const VFS&) = default;
  VFS& operator=(VFS&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Creates an object-store bucket with the input URI. */
  void create_bucket(const std::string& uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_create_bucket(ctx, vfs_.get(), uri.c_str()));
  }

  /** Deletes an object-store bucket with the input URI. */
  void remove_bucket(const std::string& uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_remove_bucket(ctx, vfs_.get(), uri.c_str()));
  }

  /** Checks if an object-store bucket with the input URI exists. */
  bool is_bucket(const std::string& uri) const {
    auto& ctx = ctx_.get();
    int ret;
    ctx.handle_error(tiledb_vfs_is_bucket(ctx, vfs_.get(), uri.c_str(), &ret));
    return (bool)ret;
  }

  /** Empty a bucket **/
  void empty_bucket(const std::string& bucket) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_empty_bucket(ctx, vfs_.get(), bucket.c_str()));
  }

  /** Check if a bucket is empty **/
  bool is_empty_bucket(const std::string& bucket) const {
    auto& ctx = ctx_.get();
    int empty;
    ctx.handle_error(
        tiledb_vfs_is_empty_bucket(ctx, vfs_.get(), bucket.c_str(), &empty));
    return empty == 0;
  }

  /** Creates a directory with the input URI. */
  void create_dir(const std::string& uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_create_dir(ctx, vfs_.get(), uri.c_str()));
  }

  /** Checks if a directory with the input URI exists. */
  bool is_dir(const std::string& uri) const {
    auto& ctx = ctx_.get();
    int ret;
    ctx.handle_error(tiledb_vfs_is_dir(ctx, vfs_.get(), uri.c_str(), &ret));
    return (bool)ret;
  }

  /** Removes a directory (recursively) with the input URI. */
  void remove_dir(const std::string& uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_remove_dir(ctx, vfs_.get(), uri.c_str()));
  }

  /** Checks if a file with the input URI exists. */
  bool is_file(const std::string& uri) const {
    auto& ctx = ctx_.get();
    int ret;
    ctx.handle_error(tiledb_vfs_is_file(ctx, vfs_.get(), uri.c_str(), &ret));
    return (bool)ret;
  }

  /** Deletes a file with the input URI. */
  void remove_file(const std::string& uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_remove_file(ctx, vfs_.get(), uri.c_str()));
  }

  /** Retrieves the size of a file with the input URI. */
  uint64_t file_size(const std::string& uri) const {
    uint64_t ret;
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_file_size(ctx, vfs_.get(), uri.c_str(), &ret));
    return ret;
  }

  /** Renames a TileDB file from an old URI to a new URI. */
  void move_file(const std::string& old_uri, const std::string& new_uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_move_file(
        ctx, vfs_.get(), old_uri.c_str(), new_uri.c_str()));
  }

  /** Renames a TileDB directory from an old URI to a new URI. */
  void move_dir(const std::string& old_uri, const std::string& new_uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(
        tiledb_vfs_move_dir(ctx, vfs_.get(), old_uri.c_str(), new_uri.c_str()));
  }

  /** Touches a file with the input URI, i.e., creates a new empty file. */
  void touch(const std::string& uri) const {
    auto& ctx = ctx_.get();
    ctx.handle_error(tiledb_vfs_touch(ctx, vfs_.get(), uri.c_str()));
  }

  /** Get the underlying context **/
  const Context& context() const {
    return ctx_.get();
  }

  /** Get the underlying tiledb object **/
  std::shared_ptr<tiledb_vfs_t> ptr() const {
    return vfs_;
  }

  /** Get the config **/
  Config config() const {
    return config_;
  }

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
  void create_vfs(tiledb_config_t* config) {
    tiledb_vfs_t* vfs;
    int rc = tiledb_vfs_alloc(ctx_.get(), &vfs, config);
    if (rc != TILEDB_OK)
      throw std::runtime_error(
          "[TileDB::C++API] Error: Failed to create VFS object");

    vfs_ = std::shared_ptr<tiledb_vfs_t>(vfs, deleter_);
  }
};

namespace impl {

inline VFSFilebuf::VFSFilebuf(const VFS& vfs)
    : vfs_(vfs) {
}

inline VFSFilebuf* VFSFilebuf::open(
    const std::string& uri, std::ios::openmode openmode) {
  close();
  uri_ = uri;

  tiledb_vfs_mode_t mode;

  if (openmode == std::ios::out) {
    mode = TILEDB_VFS_WRITE;
  } else if (openmode == std::ios::app) {
    mode = TILEDB_VFS_APPEND;
  } else if (openmode == std::ios::in) {
    mode = TILEDB_VFS_READ;
  } else
    return nullptr;

  tiledb_vfs_fh_t* fh;
  auto& ctx = vfs_.get().context();
  if (tiledb_vfs_open(ctx, vfs_.get().ptr().get(), uri.c_str(), mode, &fh) !=
      TILEDB_OK) {
    return nullptr;
  }
  fh_ = std::shared_ptr<tiledb_vfs_fh_t>(fh, deleter_);

  if (mode == TILEDB_VFS_APPEND && vfs_.get().is_file(uri)) {
    seekoff(0, std::ios::end, openmode);
  }

  return this;
}

inline VFSFilebuf* VFSFilebuf::close() {
  if (is_open()) {
    auto& ctx = vfs_.get().context();
    ctx.handle_error(tiledb_vfs_close(ctx, fh_.get()));
  }
  uri_ = "";
  fh_ = nullptr;
  offset_ = 0;
  return this;
}

inline std::streamsize VFSFilebuf::xsgetn(char_type* s, std::streamsize n) {
  uint64_t fsize = file_size();
  std::streamsize readlen = n;
  if (offset_ + n >= fsize) {
    readlen = fsize - offset_;
  }
  if (readlen == 0)
    return traits_type::eof();
  auto& ctx = vfs_.get().context();
  if (tiledb_vfs_read(
          ctx, fh_.get(), offset_, s, static_cast<uint64_t>(readlen)) !=
      TILEDB_OK) {
    return traits_type::eof();
  }

  offset_ += readlen;
  return readlen;
}

inline std::streamsize VFSFilebuf::xsputn(
    const char_type* s, std::streamsize n) {
  if (offset_ != 0 && offset_ != file_size())
    return traits_type::eof();
  auto& ctx = vfs_.get().context();
  if (tiledb_vfs_write(ctx, fh_.get(), s, static_cast<uint64_t>(n)) !=
      TILEDB_OK) {
    return traits_type::eof();
  }
  offset_ += n;
  return n;
}

inline uint64_t VFSFilebuf::file_size() const {
  if (!vfs_.get().is_file(uri_))
    return 0;
  try {
    return vfs_.get().file_size(uri_);
  } catch (TileDBError& e) {
    return 0;
  }
}

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_CPP_API_VFS_H
