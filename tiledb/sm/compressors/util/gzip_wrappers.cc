/**
 * @file gzip_wrappers.cc
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
 *
 * @section DESCRIPTION
 *
 * This file defines gzip_compress and gzip_uncompress, routines
 * that slightly simplify use of the gzip compression routines.
 */

#include "gzip_wrappers.h"

#include <inttypes.h>

void gzip_compress(
    shared_ptr<tiledb::sm::Buffer>& out_gzipped_buf,
    const void* in_bytes,
    uint64_t nbytes) {
  assert(out_gzipped_buf.get() != nullptr);
  /**
   * buffer format:
   *    uint64_t uncompressed_size
   *    uint64_t compressed_size
   *    uint8_t [compressed_size] - compressed data
   */
  uint64_t overhead_size = 2 * sizeof(uint64_t);
  tiledb::sm::ConstBuffer const_in_buf(in_bytes, nbytes);
  // Ensure space in output buffer for worst acceptable case.
  if (!out_gzipped_buf->realloc(nbytes + overhead_size).ok()) {
    throw std::logic_error("gzip output buffer allocation error");
  }
  out_gzipped_buf->reset_size();
  out_gzipped_buf->advance_size(overhead_size);
  out_gzipped_buf->advance_offset(overhead_size);
  tiledb::sm::GZip::compress(9, &const_in_buf, out_gzipped_buf.get());

  // TODO: Handle possibility that 'error' is just 'not enuf buffer', i.e.
  // unable to compress into <= space of in_buf (currently that is not
  // returned as a distinct error from Gzip::compress())

  // adjust to size actually used
  out_gzipped_buf->set_size(out_gzipped_buf->offset());
  uint64_t compressed_size = out_gzipped_buf->offset() - overhead_size;
  uint64_t uncompressed_size = nbytes;
  // return next 'write()' position to beginning of buffer
  out_gzipped_buf->reset_offset();
  // write sizes to beginning of buffer
  throw_if_not_ok(
      out_gzipped_buf->write(&uncompressed_size, sizeof(uncompressed_size)));
  throw_if_not_ok(
      out_gzipped_buf->write(&compressed_size, sizeof(compressed_size)));
}

void gzip_decompress(
    shared_ptr<tiledb::sm::ByteVecValue>& out_buf, const uint8_t* comp_buf) {
  /**
   * Expected compressed buffer format:
   *    uint64_t uncompressed_size
   *    uint64_t compressed_size
   *    uint8_t [compressed_size] - compressed data
   */
  uint64_t expanded_size = *reinterpret_cast<const uint64_t*>(comp_buf);
  uint64_t compressed_size =
      *reinterpret_cast<const uint64_t*>(comp_buf + sizeof(uint64_t));

  const uint8_t* comp_data = comp_buf + 2 * sizeof(uint64_t);

  out_buf->resize(expanded_size);

  tiledb::sm::PreallocatedBuffer pa_gunzip_out_buf(
      out_buf->data(),
      expanded_size);  // the expected uncompressed size

  tiledb::sm::ConstBuffer gzipped_input_buffer(comp_data, compressed_size);

  tiledb::sm::GZip::decompress(&gzipped_input_buffer, &pa_gunzip_out_buf);
}
