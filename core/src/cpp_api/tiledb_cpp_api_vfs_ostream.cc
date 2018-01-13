/**
 * @file   tiledb_cpp_api_vfs_ostream.cc
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
 * Write a file with the VFS.
 */

#include "tiledb_cpp_api_vfs_ostream.h"

void tdb::VFSostream::open(const std::string &fname, std::ios_base::openmode openmode) {
  close();
  if ((openmode & std::ios::app) == 0) {
    throw std::runtime_error("VFS ostream must be opened in app mode.");
  }
  if (vfs_.get().is_file(fname)) {
    sbuf_.set_uri(fname);
    sbuf_.pubseekoff(0, std::ios_base::end);
  } else {
    vfs_.get().touch(fname);
    sbuf_.set_uri(fname);
    sbuf_.pubseekpos(0);
  }
  openmode_ = openmode;
}

bool tdb::VFSostream::is_open() const {
  return sbuf_.get_uri().size() != 0;
}

void tdb::VFSostream::close() {
  if (is_open()) {
    sbuf_.pubsync();
    sbuf_.set_uri("");
  }
}

tdb::VFSostream &tdb::VFSostream::write(const char *s, uint64_t size) {
  sbuf_.sputn(s, size);
  return *this;
}

tdb::VFSostream &tdb::VFSostream::write(const std::string &s) {
  return write(s.c_str(), s.size());
}

tdb::VFSostream &tdb::VFSostream::operator<<(const char *s) {
  std::string str(s);
  return write(s);
}

tdb::VFSostream &tdb::VFSostream::operator<<(const std::string &s) {
  return write(s);
}


