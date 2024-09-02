/**
 * @file compile_filter_main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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

#include "../bzip_compressor.h"
#include "../dd_compressor.h"
#include "../dict_compressor.h"
#include "../gzip_compressor.h"
#include "../lz4_compressor.h"
#include "../rle_compressor.h"
#include "../zstd_compressor.h"

int main() {
  (void)sizeof(tiledb::sm::BZip);
  (void)sizeof(tiledb::sm::DoubleDelta);
  (void)sizeof(tiledb::sm::DictEncoding);
  (void)sizeof(tiledb::sm::GZip);
  (void)sizeof(tiledb::sm::LZ4);
  (void)sizeof(tiledb::sm::RLE);
  (void)sizeof(tiledb::sm::ZStd);
  return 0;
}
