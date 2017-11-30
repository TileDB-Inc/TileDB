/**
 * @file   buffer_cache.cc
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
 * This file includes implementations of S3 write buffer cache
 */

#include "buffer_cache.h"
#include <iostream>
#include "logger.h"

#ifdef HAVE_S3
#include "s3_filesystem.h"
#endif

namespace tiledb {

BufferCache::BufferCache() {
}

BufferCache::~BufferCache() {
}

uint64_t BufferCache::get_buffer_size() {
  return BUFFER_SIZE;
}

void BufferCache::set_buffer_size(uint64_t buffer_size) {
  BUFFER_SIZE = buffer_size;
}

Status BufferCache::write_to_file(
    const URI& uri, const void* buffer, const uint64_t length) {
#ifdef HAVE_S3
  std::string path = uri.c_str();
  if (file_buffers.find(path) == file_buffers.end()) {
    Buffer file_buffer;
    file_buffers[path] = file_buffer;
  }
  file_buffers[path].write(buffer, length);
  if (file_buffers[path].size() >= BUFFER_SIZE) {
    s3::write_to_file_no_cache(
        uri, file_buffers[path].data(), BUFFER_SIZE);//file_buffers[path].size());
    Buffer* new_buffer = new Buffer();
    new_buffer->write(file_buffers[path].data(BUFFER_SIZE), file_buffers[path].size()-BUFFER_SIZE);
    file_buffers[path].clear();
    file_buffers.erase(path);
    file_buffers[path] = (*new_buffer); 
  }
#endif
  return Status::Ok();
}

Status BufferCache::flush_file(const URI& uri) {
#ifdef HAVE_S3
  std::string path = uri.c_str();
  if (file_buffers.find(path) == file_buffers.end()) {
    return Status::IOError("No buffer found for file.");
  }
  s3::write_to_file_no_cache(
      uri, file_buffers[path].data(), file_buffers[path].size());
  file_buffers[path].clear();
  file_buffers.erase(path);
#endif
  return Status::Ok();
}

}  // namespace tiledb
