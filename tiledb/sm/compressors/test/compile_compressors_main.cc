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

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <memory>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

int main() {
  (void)sizeof(tiledb::sm::BZip);
  (void)sizeof(tiledb::sm::DoubleDelta);
  (void)sizeof(tiledb::sm::DictEncoding);
  (void)sizeof(tiledb::sm::GZip);
  (void)sizeof(tiledb::sm::LZ4);
  (void)sizeof(tiledb::sm::RLE);
  (void)sizeof(tiledb::sm::ZStd);

  const int seg_sz = 4096;
  char fbuf[seg_sz];
  auto inbuf = make_shared<tiledb::sm::Buffer>(HERE());
  auto outbuf = make_shared<tiledb::sm::Buffer>(HERE());
  uint64_t nread;
  uint64_t cntread = 0;

#ifdef _WIN32
  // need to be sure in/out are in binay mode, windows default won't be!!!
  if ((-1 == _setmode(fileno(stdin), _O_BINARY)) ||
      (-1 == _setmode(fileno(stdout), _O_BINARY))) {
    fprintf(stderr, "failure setting stdin/stdout to binary mode!");
    exit(-1);
  }
#endif
  auto infile = stdin;
  auto outfile = stdout;

  do {
    nread = fread(fbuf, 1, sizeof(fbuf), infile);
    if (nread && nread != -1) {
      inbuf->write(fbuf, nread);
      cntread += nread;
    }
  } while (nread == sizeof(fbuf));
  // tiledb::sm::GZip::compress(9, &*inbuf, &*outbuf);
  tiledb::sm::ConstBuffer const_inbuf(&*inbuf);
  tiledb::sm::GZip::compress(9, &const_inbuf, &*outbuf);
  const char* prefix = "R\"tiledb_gzipped_mgc(";
  const char* postfix = ")tiledb_gzipped_mgc";
  if (fwrite(prefix, 1, strlen(prefix), stdout) != strlen(prefix)) {
    printf("error writing prefix\n");
    exit(1);
  }
  if (fwrite(&cntread, 1, sizeof(cntread), stdout) != sizeof(cntread)) {
    printf("error writing original bytecnt\n");
    exit(2);
  }
  uint64_t compressed_size = outbuf->size();
  if (fwrite(&compressed_size, 1, sizeof(compressed_size), outfile) !=
      sizeof(compressed_size)) {
    printf("error writing compressed bytecnt\n");
    exit(3);
  }
  // now output the compressed data
  auto nremaining = outbuf->size();
  while (nremaining) {
    auto ntowrite = nremaining > seg_sz ? seg_sz : nremaining;
    outbuf->read(fbuf, ntowrite);
    if (fwrite(&ntowrite, 1, ntowrite, outfile) != ntowrite) {
      printf("error writing compressed data");
      exit(4);
    }
    nremaining -= ntowrite;
  }
  if (fwrite(postfix, 1, strlen(postfix), stdout) != strlen(postfix)) {
    printf("error writing postfix\n");
    exit(5);
  }

  return 0;
}
