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

#include "tiledb_cpp_api_vfs_streambuf.h"

tdb::impl::VFSStreambuf::VFSStreambuf(const tdb::Context &ctx, const std::string &uri,
                                      std::shared_ptr<tiledb_config_t> config)
: ctx_(ctx), deleter_(ctx), uri_(uri) {
  tiledb_vfs_t *vfs;
  ctx.handle_error(tiledb_vfs_create(ctx, &vfs, config.get()));
  vfs_ = std::shared_ptr<tiledb_vfs_t>(vfs, deleter_);
}

pos_type
tdb::impl::VFSStreambuf::seekoff(long offset, std::ios_base::seekdir seekdir, std::ios_base::openmode openmode) {
  if (openmode & std::ios_base::in) {
    throw std::runtime_error("Tiledb VFS does not support in openmode, only out and app.");
  }
  uint64_t fsize = _file_size();
  // Note convoluted logic is to get rid of signed/unsigned comparison errors
  switch (seekdir) {
    case std::ios_base::beg: {
      if (offset < 0 || static_cast<uint64_t>(offset) > fsize) {
        throw std::invalid_argument("Invalid offset.");
      }
      offset_ = static_cast<uint64_t>(offset);
      break;
    }
    case std::ios_base::cur: {
      if (offset + offset_ > fsize || (offset < 0 && static_cast<uint64_t>(std::abs(offset)) > offset_)) {
        throw std::invalid_argument("Invalid offset.");
      }
      offset_ = static_cast<uint64_t>(offset + offset_);
      break;
    }
    case std::ios_base::end: {
      if (offset + fsize > fsize || (offset < 0 && static_cast<uint64_t>(std::abs(offset)) > fsize)) {
        throw std::invalid_argument("Invalid offset.");
      }
      offset_ = static_cast<uint64_t>(offset + fsize);
      break;
    }
    default: throw std::invalid_argument("Invalid offset.");
  }
  // This returns a static constant
  return std::streambuf::seekoff(offset, seekdir, openmode);
}

pos_type tdb::impl::VFSStreambuf::seekpos(std::fpos<mbstate_t> pos, std::ios_base::openmode openmode) {
  if (openmode & std::ios_base::in) {
    throw std::runtime_error("Tiledb VFS does not support in openmode, only out and app.");
  }
  uint64_t fsize = _file_size();
  if (pos < 0 || static_cast<uint64_t>(pos) > fsize) {
    throw std::invalid_argument("Invalid pos.");
  }
  offset_ = static_cast<uint64_t>(pos);
  // This returns a static constant
  return std::streambuf::seekpos(pos, openmode);
}

int_type tdb::impl::VFSStreambuf::sync() {
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_sync(ctx, vfs_.get(), uri_.c_str()));
  return 0;
}

std::streamsize tdb::impl::VFSStreambuf::showmanyc() {
  return _file_size() - offset_;
}

std::streamsize tdb::impl::VFSStreambuf::xsgetn(char *s, std::streamsize n) {
  auto &ctx = ctx_.get();
  uint64_t fsize = _file_size();
  if (offset_ + n >= fsize) {
    n = fsize - offset_;
  }
  if (n == 0) return traits_type::eof();
  ctx.handle_error(tiledb_vfs_read(ctx, vfs_.get(), uri_.c_str(), offset_, s, static_cast<uint64_t>(n)));
  offset_ += n;
  return n;
}

int_type tdb::impl::VFSStreambuf::underflow() {
  char_type c;
  xsgetn(&c, 1);
  return c;
}

std::streamsize tdb::impl::VFSStreambuf::xsputn(const char *s, std::streamsize n) {
  if (offset_ != _file_size()) throw std::runtime_error("VFS can only append to file.");
  auto &ctx = ctx_.get();
  ctx.handle_error(tiledb_vfs_write(ctx, vfs_.get(), uri_.c_str(), s, static_cast<uint64_t>(n)));
  return n;
}

int_type tdb::impl::VFSStreambuf::overflow(int c) {
  char_type ch = traits_type::to_char_type(c);
  xsputn(&ch, 1);
  return traits_type::to_int_type(ch);
}

uint64_t tdb::impl::VFSStreambuf::_file_size() const {
  auto &ctx = ctx_.get();
  uint64_t fsize;
  ctx.handle_error(tiledb_vfs_file_size(ctx, vfs_.get(), uri_.c_str(), &fsize));
  return fsize;
}
