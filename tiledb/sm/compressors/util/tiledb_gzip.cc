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

#include "gzip_wrappers.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <memory>
#include <sstream>
#include <iomanip>
#include <iostream>

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
  auto tdb_gzip_buf = make_shared<tiledb::sm::Buffer>(HERE());
#if 01
  unsigned maxnperline = 128;
  auto addbytevals = [&](void *_pbytes, uint64_t nbytes) {
    uint8_t* pbytes = reinterpret_cast<uint8_t*>(_pbytes);
    static unsigned cntout = 0;
    for (auto i=0u; i < nbytes; ++i) {
      //std::cout << "0x" << std::setw(2) << std::hex << std::setfill('0')
      //          << (unsigned)pbytes[i] << ",";
      fprintf(outfile,"'\\x%02x',", pbytes[i]);
      if (++cntout > maxnperline)
        cntout = 0,
        //std::cout << std::endl;
        fprintf(outfile, "\n");
    }
  };
  addbytevals(&cntread, sizeof(cntread));
  //std::cerr << hexstreamvalues.str() << std::endl;
  uint64_t compressed_size = zipped_buf->size();
  addbytevals(&compressed_size, sizeof(compressed_size));
  //writehexstreamvaluesstring(hexstreamvalues);
  //hexstreamvalues.clear();
  auto nremaining = zipped_buf->size();
  // vs19 complained...
  // A) could not find raw string literal terminator
  // B) that (all data as) one string was to large a string
  while (nremaining) {
    auto ntowrite = nremaining > seg_sz ? seg_sz : nremaining;
    // TBD: error from ->read()?
    zipped_buf->read(fbuf, ntowrite);
    // temp code tryingt to elim pseudo EOF chars to see if this is cause of
    // VS19's complaint trying to include file containing them
    // (can also see
    // https://developercommunity.visualstudio.com/t/bug-in-raw-string-implementation-converning-eof-02/254730)
    // (I was running 16.11.3 when I encountered...)
    // for(auto i = 0 ; i < ntowrite; ++i)
    //  if(fbuf[i] == 0x1a) fbuf[i] = 0;
    tdb_gzip_buf->write(fbuf, ntowrite);
    addbytevals(fbuf, ntowrite);
    //writehexstreamvaluesstring(hexstreamvalues);
    //hexstreamvalues.clear();
    nremaining -= ntowrite;
  }
#elif 0
  std::string hexstringtowrite;
  std::stringstream hexstreamvalues;
  auto writehexstringvaluesstring = [&outfile](std::string& s) {
    if (fwrite("\"", 1, 1, outfile) != 1) {
      printf("error writing compressed data");
      exit(4);
    }
    if (fwrite(s.c_str(), 1, s.length(), outfile) !=
        s.length()) {
      printf("error writing compressed data");
      exit(4);
    }
    if (fwrite("\"", 1, 1, outfile) != 1) {
      printf("error writing compressed data");
      exit(4);
    }
    if (fwrite("\n", 1, 1, outfile) != 1) {
      printf("error writing compressed data");
      exit(4);
    }
  };
  auto writehexstringchunks = [&](std::string s) {
    int chunksize = 128;
    while (s.length() > chunksize) {
      std::string subs = s.substr(0, chunksize);
      writehexstringvaluesstring(subs);
      s.erase(0, chunksize);
    }
    writehexstringvaluesstring(s);
  };
  auto writehexstreamvaluesstring = [&](std::stringstream& ss) {
    writehexstringchunks(ss.str());
    ss.clear();
  };
  auto addhexchars =
      [&](std::stringstream& hexstreamvalues, void* _pbytes, unsigned nbytes) {
        auto pbytes = reinterpret_cast<uint8_t*>(_pbytes);
        for (auto i = 0u; i < nbytes; ++i)
          // hexstreamvalues << '\\x' << std::setw(2) << std::hex <<
          // std::setfill('0') << pbytes[i];
          #if 0
          hexstreamvalues << "0x" << std::setfill('0') << std::hex
                          << std::setw(2) << (unsigned)pbytes[i] << ",";
          #else
          hexstreamvalues << "\\x" << std::setfill('0') << std::hex
                          << std::setw(2) << (unsigned)pbytes[i];
        #endif
        //if(hexstreamvalues.str().length() >= 128) {
        //  writehexstringchunks(hexstreamvalues.str());
        //  hexstreamvalues.clear();
        //}
      };
  addhexchars(hexstreamvalues, &cntread, sizeof(cntread));
  std::cerr << hexstreamvalues.str() << std::endl;
  uint64_t compressed_size = zipped_buf->size();
  addhexchars(hexstreamvalues, &compressed_size, sizeof(compressed_size));
  writehexstreamvaluesstring(hexstreamvalues);
  hexstreamvalues.clear();
  auto nremaining = zipped_buf->size();
  // vs19 complained...
  // A) could not find raw string literal terminator
  // B) that (all data as) one string was to large a string
  while (nremaining) {
    auto ntowrite = nremaining > seg_sz ? seg_sz : nremaining;
    // TBD: error from ->read()?
    zipped_buf->read(fbuf, ntowrite);
    // temp code tryingt to elim pseudo EOF chars to see if this is cause of
    // VS19's complaint trying to include file containing them
    // (can also see https://developercommunity.visualstudio.com/t/bug-in-raw-string-implementation-converning-eof-02/254730)
    // (I was running 16.11.3 when I encountered...)
    //for(auto i = 0 ; i < ntowrite; ++i)
    //  if(fbuf[i] == 0x1a) fbuf[i] = 0;
    tdb_gzip_buf->write(fbuf, ntowrite);
    addhexchars(hexstreamvalues, fbuf, ntowrite);
    writehexstreamvaluesstring(hexstreamvalues);
    hexstreamvalues.clear();
    nremaining -= ntowrite;
  }
  #if 1
  #else
  if (fwrite("\"", 1, 1, outfile) != 1){
      printf("error writing compressed data");
      exit(4);
  }
  if (fwrite(hexstreamvalues.str().c_str(), 1, hexstreamvalues.str().length(), outfile)
     != hexstreamvalues.str().length()) {
      printf("error writing compressed data");
      exit(4);
  }
  if (fwrite("\"", 1, 1, outfile) != 1){
      printf("error writing compressed data");
      exit(4);
  }
  #endif
#else
  // note: vs19 possibility "error C3512: the delimiting character sequence for
  // a raw string literal shall have no more than 16 characters"
  // const char* prefix = "R\"tiledb_gzip_mgc(";
  const char* prefix = "R\"gzip_mgc(";
  // vs19 complaining "Error	C3516	unexpected end-of-file found while
  // processing the raw string literal; delimiter sequence 'gzip_mgc)' was not
  // matched const char* postfix = ")tiledb_gzip_mgc\"";
  // const char* postfix = "tiledb_gzip_mgc)\"";
  // const char* postfix = ")gzip_mgc\"";
  // const char* postfix = ")gzip_mgc)\"\n // end of binary data (above)";
  const char* postfix = ")gzip_mgc\"\n // end of binary data (above)\n";
  if (fwrite(prefix, 1, strlen(prefix), outfile) != strlen(prefix)) {
    printf("error writing prefix\n");
    exit(1);
  }
  tdb_gzip_buf->write(&cntread, sizeof(cntread));
  if (fwrite(&cntread, 1, sizeof(cntread), outfile) != sizeof(cntread)) {
    printf("error writing original bytecnt\n");
    exit(2);
  }
  uint64_t compressed_size = zipped_buf->size();
  tdb_gzip_buf->write(&compressed_size, sizeof(compressed_size));
  if (fwrite(&compressed_size, 1, sizeof(compressed_size), outfile) !=
      sizeof(compressed_size)) {
    printf("error writing compressed bytecnt\n");
    exit(3);
  }

  printf("compressed bytes only size %llu\n", zipped_buf->size());
  // now output the compressed data
  auto nremaining = zipped_buf->size();
  while (nremaining) {
    auto ntowrite = nremaining > seg_sz ? seg_sz : nremaining;
    // TBD: error from ->read()?
    zipped_buf->read(fbuf, ntowrite);
    // temp code tryingt to elim pseudo EOF chars to see if this is cause of
    // VS19's complaint trying to include file containing them
    // (can also see https://developercommunity.visualstudio.com/t/bug-in-raw-string-implementation-converning-eof-02/254730)
    // (I was running 16.11.3 when I encountered...)
    for(auto i = 0 ; i < ntowrite; ++i)
      if(fbuf[i] == 0x1a) fbuf[i] = 0;
    tdb_gzip_buf->write(fbuf, ntowrite);
#if 01
    
#else
    //if (fwrite(&ntowrite, 1, ntowrite, outfile) != ntowrite) {
    if (fwrite(&fbuf[0], 1, ntowrite, outfile) != ntowrite) {
      printf("error writing compressed data");
      exit(4);
    }
#endif
    nremaining -= ntowrite;
  }
  if (fwrite(postfix, 1, strlen(postfix), outfile) != strlen(postfix)) {
    printf("error writing postfix\n");
    exit(5);
  }
#endif
  {
    // filter_stg will be static file scope in gzip_wrappers.cc
    auto filter_stg = make_shared<tiledb::sm::FilterStorage>(HERE());
    // auto zipped_buf = make_shared<tiledb::sm::FilterBuffer>(HERE(),
    // &*filter_stg);

    #if 0
    auto gzip_compress = [&filter_stg](shared_ptr<tiledb::sm::FilterBuffer>&
                                out_gzipped_buf,
                            const shared_ptr<tiledb::sm::Buffer>& inbuf) {
      tiledb::sm::ConstBuffer const_inbuf(&*inbuf);
      out_gzipped_buf =
          make_shared<tiledb::sm::FilterBuffer>(HERE(), &*filter_stg);
      // Ensure space in output buffer for worst case.
      if (!out_gzipped_buf->prepend_buffer(inbuf->size() + 2 * sizeof(uint64_t)).ok()) {
        printf("output buffer allocation error!\n");
        exit(-3);
      }
      tiledb::sm::Buffer* out_buffer_ptr = out_gzipped_buf->buffer_ptr(0);
      assert(out_buffer_ptr != nullptr);
      //out_buffer_ptr->reset_offset();
      out_buffer_ptr->reset_size();
      // skip space to write compressed/uncompressed sizes
      out_buffer_ptr->advance_size(2 * sizeof(uint64_t));
      out_buffer_ptr->advance_offset(2 * sizeof(uint64_t));
      if (!tiledb::sm::GZip::compress(9, &const_inbuf, out_buffer_ptr).ok()) {
        printf("Error compressing data!\n");
        exit(-4);
      }
      printf(
          "C2 offset %llu, size %llu\n",
          out_buffer_ptr->offset(),
          out_buffer_ptr->size());
      out_buffer_ptr->realloc(out_buffer_ptr->offset());
      printf(
          "D2 offset %llu, size %llu\n",
          out_buffer_ptr->offset(),
          out_buffer_ptr->size());
      // write sizes to beginning of buffer
      uint64_t uncompressed_size = inbuf->size();
      uint64_t compressed_size =
          out_buffer_ptr->offset() - 2 * sizeof(uint64_t);
      out_buffer_ptr->reset_offset();
      // size();
      out_buffer_ptr->write(&uncompressed_size, sizeof(uncompressed_size));
      out_buffer_ptr->write(&compressed_size, sizeof(compressed_size));
      printf(
       "G2 offset %llu, size %llu\n",
         out_buffer_ptr->offset(),
         out_buffer_ptr->size());

    };
    #endif
    #if 0
    auto gzip_compress2 = [](
                             shared_ptr<tiledb::sm::Buffer>&
                                 out_gzipped_buf,
                             const shared_ptr<tiledb::sm::Buffer>& inbuf) {
      uint64_t overhead_size = 2 * sizeof(uint64_t);
      tiledb::sm::ConstBuffer const_inbuf(&*inbuf);
      //out_gzipped_buf =
      //    make_shared<tiledb::sm::Buffer>(HERE());
      // Ensure space in output buffer for worst case.
      if (!out_gzipped_buf
               ->realloc(inbuf->size() + overhead_size)
               .ok()) {
        printf("output buffer allocation error!\n");
        exit(-3);
      }
      out_gzipped_buf->reset_offset();
      out_gzipped_buf->reset_size();
      // printf(
      //    "0 offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
      // tiledb::sm::Buffer* out_buffer_ptr = out_gzipped_buf->buffer_ptr(0);
      tiledb::sm::Buffer* out_buffer_ptr = out_gzipped_buf.get();
      assert(out_buffer_ptr != nullptr);
      // printf(
      //    "5 offset %llu, size %llu\n",
      //    out_buffer_ptr->offset(),
      //    out_buffer_ptr->size());
      // printf(
      //    "5b offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
      out_buffer_ptr->reset_offset();
      // printf("A offset %llu, size %llu\n", out_buffer_ptr->offset(),
      // out_buffer_ptr->size());
      // 
      // skip space to write compressed/uncompressed sizes
      // (must advance_size() first as offset cannot be advanced beyond current size())
      out_buffer_ptr->advance_size(overhead_size);
      out_buffer_ptr->advance_offset(overhead_size);
      printf("overhead_size %llu\n", overhead_size);
      // printf(
      //    "B offset %llu, size %llu\n",
      //    out_buffer_ptr->offset(),
      //    out_buffer_ptr->size());
      // printf(
      //    "Bb offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
      if (!tiledb::sm::GZip::compress(9, &const_inbuf, out_buffer_ptr).ok()) {
        // TODO: Handle possibility that 'error' is just 'not enuf buffer', i.e.
        // unable to compress into <= space of inbuf
        printf("Error compressing data!\n");
        exit(-4);
      }
      // printf(
      //    "C offset %llu, size %llu\n",
      //    out_buffer_ptr->offset(),
      //    out_buffer_ptr->size());
      // printf(
      //    "Cb offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
      // downsize to only what's required
      out_buffer_ptr->realloc(out_buffer_ptr->offset());
      // printf(
      //    "D offset %llu, size %llu\n",
      //    out_buffer_ptr->offset(),
      //    out_buffer_ptr->size());
      // printf(
      //    "Db offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
      uint64_t compressed_size =
          out_buffer_ptr->offset() - overhead_size;
      out_buffer_ptr->reset_offset();
      // printf(
      //    "E offset %llu, size %llu\n",
      //    out_buffer_ptr->offset(),
      //    out_buffer_ptr->size());
      // printf(
      //    "Eb offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
      uint64_t uncompressed_size = inbuf->size();
      // write sizes to beginning of buffer
      out_buffer_ptr->write(&uncompressed_size, sizeof(uncompressed_size));
      // printf(
      //    "F offset %llu, size %llu\n",
      //    out_buffer_ptr->offset(),
      //    out_buffer_ptr->size());
      // printf(
      //    "Fb offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
      out_buffer_ptr->write(&compressed_size, sizeof(compressed_size));
      //printf(
      //  "G offset %llu, size %llu\n",
      //    out_buffer_ptr->offset(),
      //    out_buffer_ptr->size());
      // printf(
      //    "Gb offset %llu, size %llu\n",
      //    out_gzipped_buf->offset(),
      //    out_gzipped_buf->size());
    };
    #endif

    if (1)
    {
      //__debugbreak();
      shared_ptr<tiledb::sm::Buffer> out_gzipped_buf =
          make_shared<tiledb::sm::Buffer>(HERE());
      //gzip_compress2(out_gzipped_buf, inbuf);
      //gzip_compress(out_gzipped_buf, inbuf);
      gzip_compress(out_gzipped_buf, inbuf->data(0), inbuf->size());
      if (out_gzipped_buf->size() != tdb_gzip_buf->size()) {
        printf(
            "Error, compressed data sizes mismatch! %llu, %llu\n",
            out_gzipped_buf->size(),
            tdb_gzip_buf->size());
        exit(-13);
      }
      if (memcmp(
              out_gzipped_buf->data(0),
              tdb_gzip_buf->data(0),
              out_gzipped_buf->size()) //tdb_gzip_buf->size())
        ) {
        printf("Error, compressed data mismatch!\n");
        exit(-11);
      }
    }
    if (0)
    {
      //shared_ptr<tiledb::sm::FilterBuffer> out_gzipped_buf;
      shared_ptr<tiledb::sm::Buffer> out_gzipped_buf;
      //gzip_compress(out_gzipped_buf, inbuf);
      gzip_compress(out_gzipped_buf, inbuf->data(0), inbuf->size());
      // if (out_gzipped_buf->buffer_ptr(0)->size() != tdb_gzip_buf->size()) {
      if (out_gzipped_buf->size() != tdb_gzip_buf->size()) {
        printf(
            "Error, compressed data sizes mismatch! %llu, %llu\n",
            //out_gzipped_buf->buffer_ptr(0)->size(),
            out_gzipped_buf->size(),
            tdb_gzip_buf->size());
        exit(-13);
      }
      if (memcmp(
              //out_gzipped_buf->buffer_ptr(0)->data(0),
              out_gzipped_buf->data(),
              tdb_gzip_buf->data(0),
              tdb_gzip_buf->size())) {
        printf("Error, compressed data mismatch!\n");
        exit(-11);
      }
    }
  }

  shared_ptr<tiledb::sm::ByteVecValue> expanded_buffer =
      make_shared<tiledb::sm::ByteVecValue>(HERE());
#if 0
  auto gzip_uncompress = [](shared_ptr<tiledb::sm::ByteVecValue>& outbuf,
                            const uint8_t* compbuf) -> int {
    //uint64_t expanded_size = *reinterpret_cast<uint64_t*>(outbuf->data());
    //uint64_t compressed_size =
    //    *reinterpret_cast<uint64_t*>(outbuf->data() + sizeof(uint64_t));
    uint64_t expanded_size = *reinterpret_cast<const uint64_t*>(compbuf);
    uint64_t compressed_size =
        *reinterpret_cast<const uint64_t*>(compbuf + sizeof(uint64_t));

    //uint8_t* comp_data = outbuf->data() + 2 * sizeof(uint64_t);
    const uint8_t* comp_data = compbuf + 2 * sizeof(uint64_t);
 
    outbuf = make_shared<tiledb::sm::ByteVecValue>(HERE(), expanded_size);

    tiledb::sm::PreallocatedBuffer pa_gunzip_outbuf(
        outbuf->data(),
        expanded_size);  // the expected uncompressed size

    tiledb::sm::ConstBuffer gzipped_input_buffer(comp_data, compressed_size);

    if (!tiledb::sm::GZip::decompress(&gzipped_input_buffer, &pa_gunzip_outbuf)
               .ok()) {
      printf("Error decompressing data!\n");
      //exit(-4);
      return -4;
    }
    return 0;
  };
  #endif

#if 1
  tdb_gzip_buf->set_offset(0);
  //gzip_uncompress(expanded_buffer, static_cast<uint8_t*>(tdb_gzip_buf->data()));
  gzip_decompress(expanded_buffer, static_cast<uint8_t*>(tdb_gzip_buf->data()));
  if (memcmp(expanded_buffer->data(), inbuf->data(), inbuf->size())) {
    printf("Error uncompress data != original data!\n");
    exit(9);
  }

#else
  auto uncompressed_size = inbuf->size();
  auto uncompressed_buf =
      make_shared<tiledb::sm::ByteVecValue>(HERE(), uncompressed_size);

  tiledb::sm::PreallocatedBuffer pa_gunzip_outbuf(
      uncompressed_buf->data(),
      uncompressed_size);  // the expected uncompressed size 

  zipped_buf->set_offset(0);
  auto comp_data = zipped_buf->buffer_ptr(0)->cur_data();
  tiledb::sm::ConstBuffer gzipped_input_buffer(comp_data, compressed_size);

  if (!tiledb::sm::GZip::decompress(&gzipped_input_buffer, &pa_gunzip_outbuf)
           .ok()) {
    printf("Error decompressing data!\n");
    exit(-4);
  }

  if (memcmp(pa_gunzip_outbuf.data(), inbuf->data(), inbuf->size())) {
    printf("Error uncompress data != original data!\n");
    exit(9);
  }

#endif

  return 0;
}
