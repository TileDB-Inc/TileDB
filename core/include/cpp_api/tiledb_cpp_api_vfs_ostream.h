/**
 * @file   tiledb_cpp_api_vfs_ostream.h
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

#ifndef TILEDB_TILEDB_CPP_API_VFS_OSTREAM_H
#define TILEDB_TILEDB_CPP_API_VFS_OSTREAM_H

#include "tiledb_cpp_api_vfs.h"
#include "tiledb_cpp_api_vfs_streambuf.h"

#include <sstream>
#include <type_traits>

namespace tdb {
  class VFSostream {
  public:
    /**
     * Constructor
     *
     * @param vfs VFS object
     */
    explicit VFSostream(const VFS &vfs) : vfs_(vfs), sbuf_(vfs.context()) {}

    /**
     * Constructor. Open a file.
     *
     * @param vfs VFS object
     * @param fname URI to open
     * @param openmode open mode, can either be std::ios::app or (std::ios::app | std::ios::binary)
     */
    VFSostream(const VFS &vfs, const std::string &fname, std::ios::openmode openmode=std::ios::app) : VFSostream(vfs) {
      open(fname, openmode);
    }
    VFSostream(const VFSostream&) = default;
    VFSostream(VFSostream&&) = default;
    VFSostream &operator=(const VFSostream&) = default;
    VFSostream &operator=(VFSostream&&) = default;
    ~VFSostream() {
      close();
    }

    /**
     * Open a file.
     *
     * @param fname URI to file.
     * @param openmode open mode, can either be std::ios::app or (std::ios::app | std::ios::binary)
     */
    void open(const std::string &fname, std::ios::openmode openmode=std::ios::app);

    /** Check if a file is open **/
    bool is_open() const;

    /** Sync any writes and close the file **/
    void close();

    /** Write an arbitrary buffer. **/
    VFSostream &write(const char* s, uint64_t size);

    /** Write a string. **/
    VFSostream &write(const std::string &s);

    /** Write a string **/
    VFSostream &operator<<(const std::string &s);

    VFSostream &operator<<(const char *s);

    /** Write a vector of POD data **/
    template<typename T>
    typename std::enable_if<std::is_pod<T>::value, VFSostream&>::type
    operator<<(const std::vector<T> &v) {
      if (openmode_ & std::ios::binary) {
        return write((const char *) v.data(), v.size() * sizeof(T));
      } else {
        std::string sbuf, s;
        for (const auto &d : v) {
          _to_string(d, s);
          sbuf += s;
        }
        return write(sbuf);
      }
    };

    /** Write a variable value, or a POD value in binary mode. **/
    template<typename T>
    typename std::enable_if<std::is_pod<T>::value, VFSostream&>::type
    operator<<(const T val) {
      if (openmode_ & std::ios::binary) {
        return write((const char*) &val, sizeof(T));
      } else {
        std::string s;
        _to_string(val, s);
        return operator<<(s);
      }
    }

    /** Flush the stream to file **/
    void flush() {
      sbuf_.pubsync();
    }

  private:

    /** Underlying VFS **/
    std::reference_wrapper<const VFS> vfs_;

    /** Stream buffer **/
    impl::VFSstreambuf sbuf_;

    /** File open mode. Should always be std::ios::app, binary is optional **/
    std::ios::openmode openmode_;

    /** Wrapper around std::to_string to throw an error when trying to write a POD type in ASCII **/
    template<typename T>
    typename std::enable_if<std::is_pod<T>::value && !std::is_fundamental<T>::value, void>::type
    _to_string(const T &val, std::string &o) {
      (void) val;
      (void) o;
      throw std::runtime_error("Can only serialize POD data in binary mode.");
    };

    /** Wrapper around std::to_string to throw an error when trying to write a POD type in ASCII **/
    template<typename T>
    typename std::enable_if<std::is_fundamental<T>::value, void>::type
    _to_string(const T &val, std::string &o) {
      o = std::to_string(val);
    };

    /** Wrapper around std::to_string to throw an error when trying to write a POD type in ASCII **/
    void _to_string(const char &val, std::string &o) {
      o = val;
    };

  };
}

#endif //TILEDB_TILEDB_CPP_API_VFS_OSTREAM_H
