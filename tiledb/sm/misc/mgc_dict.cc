/**
 * @file mgc_dict.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 */

#include "mgc_dict.h"

#include "tiledb/sm/compressors/util/gzip_wrappers.h"

#include <stdexcept>
#include <string>

namespace tiledb {
namespace sm {

static const char magic_mgc_compressed_bytes[] = {
#include "magic_mgc_gzipped.bin"
};

shared_ptr<tiledb::sm::ByteVecValue> magic_dict::expanded_buffer_;

void* magic_dict::uncompressed_magic_dict_ = nullptr;

static magic_dict magic_dict_object;

magic_dict::magic_dict() {
  if (uncompressed_magic_dict_)
    return;

  expanded_buffer_ = make_shared<tiledb::sm::ByteVecValue>(HERE());
  if (!gzip_decompress(
           expanded_buffer_,
           reinterpret_cast<const uint8_t*>(&magic_mgc_compressed_bytes[0]))
           .ok()) {
    throw std::runtime_error("gzip_decompress failure!");
  }
  uncompressed_magic_dict_ = expanded_buffer_.get()->data();
}

int magic_dict::magic_load(magic_t magic) {
  void* data[1] = {uncompressed_magic_dict_};
  uint64_t sizes[1] = {expanded_buffer_->size()};
  // zero ok, non-zero error
  return magic_load_buffers(magic, &data[0], sizes, 1);
}

const shared_ptr<tiledb::sm::ByteVecValue> magic_dict::expanded_buffer() {
  return expanded_buffer_;
}

}  // namespace sm
}  // namespace tiledb
