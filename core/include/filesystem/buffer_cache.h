/**
 * @file   buffer_cache.h
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
 * This file includes declarations for filesystem write buffering functionality.
 */

#ifndef TILEDB_BUFFER_CACHE_H
#define TILEDB_BUFFER_CACHE_H

#include <string>

#include <sys/types.h>
#include <string>
#include <unordered_map>
#include "buffer.h"
#include "uri.h"

namespace tiledb {

/** Specifies the buffer cache */
class BufferCache {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  BufferCache();

  /** Destructor. */
  ~BufferCache();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /**
   * Writes the input buffer to a memory buffer. When the memory buffer reaches
   * BUFFER_SIZE it is written to the underlying filesystem
   *
   * If the file does not  exists it is created.
   * If the file exist then it is appended to.
   *
   * @param uri The URI of the file to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @return Status
   */
  Status write_to_file(
      const URI& uri, const void* buffer, const uint64_t length);

  Status flush_file(const URI& uri);

  uint64_t get_buffer_size();

  void set_buffer_size(uint64_t buffer_size);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /* */
  uint64_t BUFFER_SIZE = 5 * 1024 * 1024;
  /** Map containing all open buffers for files. */
  std::unordered_map<std::string, Buffer> file_buffers;
};

}  // namespace tiledb

#endif /* TILEDB_BUFFER_CACHE_H */
