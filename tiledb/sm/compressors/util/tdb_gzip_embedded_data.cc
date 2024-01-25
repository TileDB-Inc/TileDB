/**
 * @file compile_filter_main.cc
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

#include "../bzip_compressor.h"
#include "../dd_compressor.h"
#include "../dict_compressor.h"
#include "../gzip_compressor.h"
#include "../lz4_compressor.h"
#include "../rle_compressor.h"
#include "../zstd_compressor.h"

#include "tiledb/common/common.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/misc/types.h"

#include "gzip_wrappers.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

int main(int argc, char* argv[]) {
  (void)sizeof(tiledb::sm::GZip);

  const int seg_sz = 4096;
  char fbuf[seg_sz];
  auto filter_stg = make_shared<tiledb::sm::FilterStorage>(HERE());
  auto inbuf = make_shared<tiledb::sm::Buffer>(HERE());
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
  auto closefile = [&]() { fclose(outfile); };
  tiledb::common::ScopedExecutor onexit1(closefile);

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
      throw_if_not_ok(inbuf->write(fbuf, nread));
      cntread += nread;
    }
  } while (nread == sizeof(fbuf));

  tiledb::sm::ConstBuffer const_inbuf(&*inbuf);
  // Ensure space in output buffer for worst case.
  if (!zipped_buf->prepend_buffer(inbuf->size()).ok()) {
    printf("output buffer allocation error!\n");
    exit(-3);
  }
  tiledb::sm::Buffer* out_buffer_ptr = zipped_buf->buffer_ptr(0);
  assert(out_buffer_ptr != nullptr);
  out_buffer_ptr->reset_offset();
  if (!tiledb::sm::GZip::compress(9, &const_inbuf, out_buffer_ptr).ok()) {
    printf("Error compressing data!\n");
    exit(-4);
  }
  std::cerr << "sizes input " << inbuf->size() << ", compressed "
            << out_buffer_ptr->size() << std::endl;
  if (0)  // leave available for future possible diagnostic need
  {
    // save compressed data to allow examination from filesystem
    auto outgzip = fopen("magic-mgc.tdbgzip", "wb");
    if (!outgzip) {
      fprintf(stderr, "Unable to create magic-mgc.tdbgzip!\n");
      exit(-21);
    }
    if (fwrite(out_buffer_ptr->data(0), 1, out_buffer_ptr->size(), outgzip) !=
        out_buffer_ptr->size()) {
      fprintf(stderr, "write failure magic-mgc.tdbgzip!\n");
      exit(-22);
    }
    fclose(outgzip);
  }
  auto tdb_gzip_buf = make_shared<tiledb::sm::Buffer>(HERE());

  unsigned maxnperline = 128;
  auto addbytevals = [&](void* _pbytes, uint64_t nbytes) {
    uint8_t* pbytes = reinterpret_cast<uint8_t*>(_pbytes);
    static unsigned cntout = 0;
    for (auto i = 0u; i < nbytes; ++i) {
      fprintf(outfile, "'\\x%02x',", pbytes[i]);
      if (++cntout > maxnperline) {
        cntout = 0;
        fprintf(outfile, "\n");
      }
    }
  };
  addbytevals(&cntread, sizeof(cntread));
  throw_if_not_ok(tdb_gzip_buf->write(&cntread, sizeof(cntread)));
  uint64_t compressed_size = zipped_buf->size();
  addbytevals(&compressed_size, sizeof(compressed_size));
  throw_if_not_ok(
      tdb_gzip_buf->write(&compressed_size, sizeof(compressed_size)));
  auto nremaining = zipped_buf->size();
  // vs19 complained...
  // A) could not find raw string literal terminator
  //    for almost identical error, though not quite same cause, can see
  // https://developercommunity.visualstudio.com/t/bug-in-raw-string-implementation-converning-eof-02/254730)
  // vs19 16.11.3 was in use with tiledb failed #include of raw literal file
  // B) that (all data as) one string was to large a string
  // C) many shorter strings expecting concatention were causing
  //    compiler heap memory errors (100+GB)
  // D) caused warnings trying to do direct 0xXX numeric values
  // end result format is comma delimited '\xXX' characters with periodic line
  // splits
  while (nremaining) {
    auto ntowrite = nremaining > seg_sz ? seg_sz : nremaining;
    // TBD: error from ->read()?
    if (!zipped_buf->read(fbuf, ntowrite).ok()) {
      printf("ERROR reading from compressed data.\n");
      exit(-7);
    }
    if (!tdb_gzip_buf->write(fbuf, ntowrite).ok()) {
      printf("ERROR writing compressed format buffer.");
      exit(-11);
    }
    addbytevals(fbuf, ntowrite);
    nremaining -= ntowrite;
  }

  // brief sanity check that wrapper compression matches unwrapped compression
  shared_ptr<tiledb::sm::Buffer> out_gzipped_buf =
      make_shared<tiledb::sm::Buffer>(HERE());
  throw_if_not_ok(
      gzip_compress(out_gzipped_buf, inbuf->data(0), inbuf->size()));
  if (out_gzipped_buf->size() != tdb_gzip_buf->size()) {
    printf(
        "Error, compressed data sizes mismatch! %" PRIu64 ", %" PRIu64 "u\n",
        out_gzipped_buf->size(),
        tdb_gzip_buf->size());
    exit(-13);
  }
  if (memcmp(
          out_gzipped_buf->data(0),
          tdb_gzip_buf->data(0),
          out_gzipped_buf->size())  // tdb_gzip_buf->size())
  ) {
    printf("Error, compressed data mismatch!\n");
    exit(-17);
  }

  shared_ptr<tiledb::sm::ByteVecValue> expanded_buffer =
      make_shared<tiledb::sm::ByteVecValue>(HERE());

  // brief sanity check the decompressed()d data matches original
  tdb_gzip_buf->set_offset(0);
  throw_if_not_ok(gzip_decompress(
      expanded_buffer, static_cast<uint8_t*>(tdb_gzip_buf->data())));
  if (expanded_buffer->size() != inbuf->size()) {
    fprintf(stderr, "re-expanded size different from original size!\n");
    exit(-29);
  }
  if (memcmp(expanded_buffer->data(), inbuf->data(), inbuf->size())) {
    printf("Error uncompress data != original data!\n");
    exit(-21);
  }

  return 0;
}
