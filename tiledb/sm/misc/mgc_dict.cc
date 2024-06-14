/**
 * @file mgc_dict.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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

#include "magic_mgc.zst.h_"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/compressors/zstd_compressor.h"

#include <iostream>
#include <stdexcept>
#include <string>

namespace tiledb::sm::magic_dict {

void prepare_data(span<uint8_t> expanded_buffer) {
  ConstBuffer input(magic_mgc_compressed_bytes, magic_mgc_compressed_size);
  PreallocatedBuffer output(expanded_buffer.data(), expanded_buffer.size());
  ZStd::ZSTD_Decompress_Context ctx;
  ZStd::decompress(ctx, input, output);
}

int magic_mgc_embedded_load(magic_t magic) {
  auto buffer = expanded_buffer();

  void* data = const_cast<unsigned char*>(buffer.data());
  size_t size = buffer.size();
  // zero ok, non-zero error
  return magic_load_buffers(magic, &data, &size, 1);
}

span<const uint8_t> expanded_buffer() {
  static std::vector<uint8_t> expanded_buffer(magic_mgc_decompressed_size);
  static std::once_flag once_flag;
  // Thread-safe initialization of the expanded data.
  std::call_once(once_flag, [&]() { prepare_data(expanded_buffer); });
  return expanded_buffer;
}

}  // namespace tiledb::sm::magic_dict
