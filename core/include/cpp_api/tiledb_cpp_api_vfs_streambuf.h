/**
 * @file   tiledb_cpp_api_vfs_streambuf.h
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
 * stream buffer for the tiledb VFS.
 */

#ifndef TILEDB_TILEDB_CPP_API_VFS_STREAMBUF_H
#define TILEDB_TILEDB_CPP_API_VFS_STREAMBUF_H

#include "tiledb_cpp_api_context.h"
#include "tiledb_cpp_api_deleter.h"

#include <streambuf>
#include <locale>
#include <functional>
#include <memory>

namespace tdb {
  namespace impl {

    /**
     * @brief
     * Stream buffer for a Tiledb VFS. The buffer should be used to make a vfsstream.
     *
     * @details
     * This is unbuffered; each write is directly dispatched to TileDB. As such
     * it is recommended to fewer, larger, writes.
     *
     * @code{.cpp}
     *   tdb::Context ctx;
     *   tdb::impl::VFSStreambuf buf(ctx, "vfs.test");
     *   std::ostream os(&buf);
     *   os << "abcdefghijklmnopqrstuvwxyz";
     * @endcode
     */
    class VFSstreambuf : public std::streambuf {
    public:
      /* ********************************* */
      /*     CONSTRUCTORS & DESTRUCTORS    */
      /* ********************************* */

      /**
       * Constructor.
       *
       * @param ctx tiledb Context
       * @param uri URI of file
       * @param config configuration. Default none.
       */
      // TODO get the config directly from context ptr once exposed in C API
      explicit VFSstreambuf(const Context &ctx, std::shared_ptr<tiledb_config_t> config=nullptr);
      VFSstreambuf(const VFSstreambuf &buf) = default;
      VFSstreambuf(VFSstreambuf &&buf) = default;
      VFSstreambuf &operator=(const VFSstreambuf &) = default;
      VFSstreambuf &operator=(VFSstreambuf &&o) = default;

      /**
       * Set the file URI.
       *
       * @param uri Filename
       */
      void set_uri(const std::string &uri);

      /**
       * @return Current opened URI.
       */
      const std::string &get_uri() const;

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
      pos_type seekoff(off_type offset, std::ios_base::seekdir seekdir,
                       std::ios_base::openmode openmode = std::ios_base::app | std::ios_base::in) override;


      /**
       * Seek a byte position in the file.
       *
       * @param pos
       * @param openmode in, and/or app (append)
       */
      pos_type seekpos(pos_type pos, std::ios_base::openmode openmode) override;



      /** Sync all writes to the file **/
      int_type sync() override;

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
      std::streamsize xsgetn(char_type *s, std::streamsize n) override;

      /**
       * Get a character in the file. Note this function should rarely be used
       * on non-local URI's, since each will dispatch a request.
       *
       * @return character at current pos
       */
      int_type underflow() override;

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
      std::streamsize xsputn(const char_type *s, std::streamsize n) override;

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

      /** Get the file size of the file in bytes **/
      uint64_t _file_size() const;

      /** Underlying Context **/
      std::reference_wrapper<const Context> ctx_;

      /** Underlying vfs object **/
      std::shared_ptr<tiledb_vfs_t> vfs_;

      /** Deleter for vfs_ **/
      const Deleter deleter_;

      /** File URI **/
      std::string uri_;

      /** Current offset from the file beginning **/
      uint64_t offset_ = 0;

    };

  }
}

#endif //TILEDB_TILEDB_CPP_API_VFS_STREAMBUF_H
