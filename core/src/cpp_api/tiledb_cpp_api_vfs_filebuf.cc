/**
 * @file   tiledb_cpp_api_vfs_streambuf.cc
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
 * streambuf for the tiledb VFS.
 */

#include "tiledb_cpp_api_vfs_filebuf.h"

namespace tdb {

  /* ********************************* */
  /*            PUBLIC API             */
  /* ********************************* */

  VFSfilebuf *VFSfilebuf::open(const std::string &uri, std::ios::openmode openmode) {
    close();
    if ((openmode & std::ios::out) && !(openmode & std::ios::app)) {
      // Need append mode
      return nullptr;
    }
    if (vfs_.get().is_file(uri)) {
      // File exists
      uri_ = uri;
      seekoff(0, openmode & std::ios::out ? std::ios::end : std::ios::beg, openmode);
    } else if (openmode & std::ios::app) {
      // Make file
      vfs_.get().touch(uri);
      uri_ = uri;
      seekpos(0, openmode);
    } else {
      // File does not exist.
      return nullptr;
    }
    return this;
  }

  VFSfilebuf *VFSfilebuf::close() {
    if (is_open()) {
      sync();
      uri_ = "";
      return this;
    }
    return nullptr;
  }

  /* ********************************* */
  /*       PROTECTED POSITIONING       */
  /* ********************************* */

std::streambuf::pos_type VFSfilebuf::seekoff(
    long offset,
    std::ios::seekdir seekdir,
    std::ios::openmode openmode) {
  uint64_t fsize = file_size();
  (void)openmode;  // unused arg error
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

std::streambuf::pos_type VFSfilebuf::seekpos(pos_type pos, std::ios::openmode openmode) {
  (void)openmode;
  uint64_t fsize = file_size();
  if (pos < 0 || static_cast<uint64_t>(pos) > fsize) {
    return std::streampos(std::streamoff(-1));
  }
  offset_ = static_cast<uint64_t>(pos);
  // This returns a static constant
  return std::streampos(pos);
}

std::streambuf::int_type VFSfilebuf::sync() {
  vfs_.get().sync(uri_);
  return traits_type::to_int_type(0);
}

  /* ********************************* */
  /*           PROTECTED GET           */
  /* ********************************* */

std::streamsize VFSfilebuf::showmanyc() {
  return std::streamsize(file_size() - offset_);
}

std::streamsize VFSfilebuf::xsgetn(char_type *s, std::streamsize n) {
  uint64_t fsize = file_size();
  std::streamsize readlen = n;
  if (offset_ + n >= fsize) {
    readlen = fsize - offset_;
  }
  if (readlen == 0)
    return traits_type::eof();
  vfs_.get().read(uri_,offset_, s, static_cast<uint64_t>(readlen));
  offset_ += readlen;
  return readlen;
}

std::streambuf::int_type VFSfilebuf::underflow() {
  char_type c;
  if (xsgetn(&c, sizeof(c)) != traits_type::eof()) {
    --offset_;
    return traits_type::to_int_type(c);
  }
  return traits_type::eof();
}

std::streambuf::int_type VFSfilebuf::uflow() {
  char_type c;
  if (xsgetn(&c, sizeof(c)) != traits_type::eof()) {
    return traits_type::to_int_type(c);
  }
  return traits_type::eof();
}

  /* ********************************* */
  /*           PROTECTED PUT           */
  /* ********************************* */

std::streamsize VFSfilebuf::xsputn(const char_type *s, std::streamsize n) {
  if (offset_ != file_size())
    return traits_type::eof();
  vfs_.get().write(uri_, s, static_cast<uint64_t>(n));
  offset_ += n;
  return n;
}

std::streambuf::int_type VFSfilebuf::overflow(int_type c) {
  if (c != traits_type::eof()) {
    char_type ch = traits_type::to_char_type(c);
    if (xsputn(&ch, sizeof(ch)) != traits_type::eof())
      return traits_type::to_int_type(ch);
  }
  return traits_type::eof();
}

  /* ********************************* */
  /*              PRIVATE              */
  /* ********************************* */

uint64_t VFSfilebuf::file_size() const {
  return vfs_.get().file_size(uri_);
}

}  // namespace tdb