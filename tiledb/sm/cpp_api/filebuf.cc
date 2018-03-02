/**
 * @file   tiledb_cpp_api_vfs_filebuf.cc
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
 * filebuf for the tiledb VFS.
 */

#include "filebuf.h"
#include "exception.h"

namespace tiledb {
namespace impl {

/* ********************************* */
/*            PUBLIC API             */
/* ********************************* */

VFSFilebuf* VFSFilebuf::open(
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

VFSFilebuf* VFSFilebuf::close() {
  if (is_open()) {
    auto& ctx = vfs_.get().context();
    ctx.handle_error(tiledb_vfs_close(ctx, fh_.get()));
  }
  uri_ = "";
  fh_ = nullptr;
  offset_ = 0;
  return this;
}

/* ********************************* */
/*       PROTECTED POSITIONING       */
/* ********************************* */

std::streambuf::pos_type VFSFilebuf::seekoff(
    off_type offset, std::ios::seekdir seekdir, std::ios::openmode openmode) {
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

std::streambuf::pos_type VFSFilebuf::seekpos(
    pos_type pos, std::ios::openmode openmode) {
  return seekoff((off_type)pos, std::ios::beg, openmode);
}

/* ********************************* */
/*           PROTECTED GET           */
/* ********************************* */

std::streamsize VFSFilebuf::showmanyc() {
  return std::streamsize(file_size() - offset_);
}

std::streamsize VFSFilebuf::xsgetn(char_type* s, std::streamsize n) {
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

std::streambuf::int_type VFSFilebuf::underflow() {
  char_type c;
  if (xsgetn(&c, sizeof(c)) != traits_type::eof()) {
    --offset_;
    return traits_type::to_int_type(c);
  }
  return traits_type::eof();
}

std::streambuf::int_type VFSFilebuf::uflow() {
  char_type c;
  if (xsgetn(&c, sizeof(c)) != traits_type::eof()) {
    return traits_type::to_int_type(c);
  }
  return traits_type::eof();
}

/* ********************************* */
/*           PROTECTED PUT           */
/* ********************************* */

std::streamsize VFSFilebuf::xsputn(const char_type* s, std::streamsize n) {
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

std::streambuf::int_type VFSFilebuf::overflow(int_type c) {
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

uint64_t VFSFilebuf::file_size() const {
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
