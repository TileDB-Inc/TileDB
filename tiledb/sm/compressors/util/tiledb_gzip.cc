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

#include "tiledb/common/common.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/misc/types.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <memory>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

int main(int argc, char* argv[]) {
  (void)sizeof(tiledb::sm::BZip);
  (void)sizeof(tiledb::sm::DoubleDelta);
  (void)sizeof(tiledb::sm::DictEncoding);
  (void)sizeof(tiledb::sm::GZip);
  (void)sizeof(tiledb::sm::LZ4);
  (void)sizeof(tiledb::sm::RLE);
  (void)sizeof(tiledb::sm::ZStd);

  const int seg_sz = 4096;
  char fbuf[seg_sz];
  auto filter_stg = make_shared<tiledb::sm::FilterStorage>(HERE());
  auto inbuf = make_shared<tiledb::sm::Buffer>(HERE());
  // auto zipped_buf = make_shared<tiledb::sm::Buffer>(HERE());
  auto zipped_buf = make_shared<tiledb::sm::FilterBuffer>(HERE(), &*filter_stg);
  uint64_t nread;
  uint64_t cntread = 0;

  auto infile = stdin;
  auto outfile = stdout;

  // note: If stdout used ( '>' ), compressed data subject to being
  // intermixed with any application output, corrupting the compressed
  // output stream.

  if (argc > 1) {
    outfile = fopen(argv[1], "w+b");
    if (!outfile) {
      fprintf(stderr, "Unable to create file %s\n", argv[1]);
      exit(-2);
    }
  }

#ifdef _WIN32
  // need to be sure in/out are in binay mode, windows default won't be!!!
  if ((-1 == _setmode(fileno(stdin), _O_BINARY)) ||
      (-1 == _setmode(fileno(stdout), _O_BINARY))) {
    fprintf(stderr, "failure setting stdin/stdout to binary mode!");
    exit(-1);
  }
#endif

  do {
    nread = fread(fbuf, 1, sizeof(fbuf), infile);
    if (nread) {
      inbuf->write(fbuf, nread);
      cntread += nread;
    }
  } while (nread == sizeof(fbuf));

  __debugbreak();

  // tiledb::sm::GZip::compress(9, &*inbuf, &*zipped_buf);
  tiledb::sm::ConstBuffer const_inbuf(&*inbuf);
  // zipped_buf->set_size(inbuf->size());
  // zipped_buf->ensure_alloced_size(inbuf->size());
  // zipped_buf->prepend_buffer(inbuf->size());
  // Ensure space in output buffer for worst case.
  if (!zipped_buf->prepend_buffer(inbuf->size()).ok()) {
    printf("output buffer allocation error!\n");
    exit(-3);
  }
  tiledb::sm::Buffer* out_buffer_ptr = zipped_buf->buffer_ptr(0);
  assert(out_buffer_ptr != nullptr);
  out_buffer_ptr->reset_offset();
  // tiledb::sm::GZip::compress(9, &const_inbuf, &*zipped_buf);
  if (!tiledb::sm::GZip::compress(9, &const_inbuf, out_buffer_ptr).ok()) {
    printf("Error compressing data!\n");
    exit(-4);
  }
  const char* prefix = "R\"tiledb_gzipped_mgc(";
  const char* postfix = ")tiledb_gzipped_mgc";
  if (fwrite(prefix, 1, strlen(prefix), outfile) != strlen(prefix)) {
    printf("error writing prefix\n");
    exit(1);
  }
  if (fwrite(&cntread, 1, sizeof(cntread), outfile) != sizeof(cntread)) {
    printf("error writing original bytecnt\n");
    exit(2);
  }
  uint64_t compressed_size = zipped_buf->size();
  if (fwrite(&compressed_size, 1, sizeof(compressed_size), outfile) !=
      sizeof(compressed_size)) {
    printf("error writing compressed bytecnt\n");
    exit(3);
  }
  // now output the compressed data
  auto nremaining = zipped_buf->size();
  while (nremaining) {
    auto ntowrite = nremaining > seg_sz ? seg_sz : nremaining;
    zipped_buf->read(fbuf, ntowrite);
    if (fwrite(&ntowrite, 1, ntowrite, outfile) != ntowrite) {
      printf("error writing compressed data");
      exit(4);
    }
    nremaining -= ntowrite;
  }
  if (fwrite(postfix, 1, strlen(postfix), outfile) != strlen(postfix)) {
    printf("error writing postfix\n");
    exit(5);
  }

  // auto ungzip_buf = make_shared<tiledb::sm::FilterBuffer>(HERE(),
  // &*filter_stg);
  //  auto ungzip_buf = make_shared<tiledb::sm::Buffer>(HERE());
  // ungzip_buf->prepend_buffer(inbuf->size()); // want uncompressed size
  // tiledb::sm::PreallocatedBuffer
  // pa_ungzip_buf(ungzip_buf->buffer_ptr(0)->cur_data(), inbuf->size()); TBD:
  // does PreallocatedBuffer actualy -do- the allocation? NO! auto
  // uncompressed_buf = make_shared<uint8_t>(HERE());
  auto uncompressed_size = inbuf->size();
  auto uncompressed_buf =
      // make_shared<uint8_t>(HERE(), uint8_t[uncompressed_size]);
      make_shared<tiledb::sm::ByteVecValue>(HERE(), uncompressed_size);

  tiledb::sm::PreallocatedBuffer pa_gunzip_outbuf(
      //      ungzip_buf->cur_data(), inbuf->size());
      uncompressed_buf->data(),
      uncompressed_size);  // the uncompressed size
  // out_buffer_ptr->reset_offset();

  //  auto compressed_size = zipped_buf->size();
  auto comp_data = zipped_buf->buffer_ptr(0)->cur_data();
  zipped_buf->set_offset(0);
  tiledb::sm::ConstBuffer gzipped_input_buffer(comp_data, compressed_size);
#if 0
  tiledb::sm::ConstBuffer gzipped_input_buffer(nullptr, 0);
#if 01
  zipped_buf->set_offset(0);
  if (!zipped_buf->get_const_buffer(zipped_buf->size(), &gzipped_input_buffer)
           .ok()) {
    printf("error decompression check!\n");
    exit(7);

  }
#endif
#endif

#if 01
  if (!tiledb::sm::GZip::decompress(&gzipped_input_buffer, &pa_gunzip_outbuf)
           .ok()) {
    printf("Error decompressing data!\n");
    exit(-4);
  }
#endif

#if 01
  if (memcmp(
          // pa_gunzip_outbuf.cur_data(), inbuf->cur_data(), inbuf->size())) {
          pa_gunzip_outbuf.data(),
          inbuf->data(),
          inbuf->size())) {
    printf("Error uncompress data != original data!\n");
    exit(9);
  }
#endif

  return 0;
}
