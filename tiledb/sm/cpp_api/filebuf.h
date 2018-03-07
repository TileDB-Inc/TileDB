/**
 * @file   tiledb_cpp_api_vfs_filebuf.h
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
 * File buffer for the TileDB VFS.
 */

#ifndef TILEDB_CPP_API_VFS_FILEBUF_H
#define TILEDB_CPP_API_VFS_FILEBUF_H

#include "deleter.h"
#include "vfs.h"

#include <functional>
#include <locale>
#include <memory>
#include <streambuf>

namespace tiledb {
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
class TILEDB_EXPORT VFSFilebuf : public std::streambuf {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param vfs Tiledb VFS
   */
  explicit VFSFilebuf(const VFS& vfs)
      : vfs_(vfs)
      , deleter_(vfs.context()) {
  }
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
      std::ios::openmode openmode) override;

  /**
   * Seek a byte position in the file.
   *
   * @param pos
   * @param openmode in, and/or app (append)
   */
  pos_type seekpos(pos_type pos, std::ios::openmode openmode) override;

  /* ********************************* */
  /*           PROTECTED GET           */
  /* ********************************* */

  /**
   * @return Number of bytes left between current pos and end of file
   */
  std::streamsize showmanyc() override;

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
  int_type underflow() override;

  /**
   * Get a character in the file and advance position.
   *
   * @return character at current pos
   */
  int_type uflow() override;

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
  int_type overflow(int_type c) override;

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
}  // namespace tiledb

#endif  // TILEDB_CPP_API_VFS_FILEBUF_H
