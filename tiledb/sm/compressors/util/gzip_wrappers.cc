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
 */

#include "gzip_wrappers.h"

Status gzip_compress(shared_ptr<tiledb::sm::Buffer>& out_gzipped_buf,
    const void* in_bytes, uint64_t nbytes) {
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
    return LOG_STATUS(
        Status_CompressionError(
        "gzip output buffer allocation error"));
  }
  out_gzipped_buf->reset_offset();
  out_gzipped_buf->reset_size();
  tiledb::sm::Buffer* out_buffer_ptr = out_gzipped_buf.get();
  assert(out_buffer_ptr != nullptr);
  out_buffer_ptr->reset_offset();
  out_buffer_ptr->advance_size(overhead_size);
  out_buffer_ptr->advance_offset(overhead_size);
  printf("overhead_size %llu\n", overhead_size);
  if (!tiledb::sm::GZip::compress(9, &const_in_buf, out_buffer_ptr).ok()) {
    // TODO: Handle possibility that 'error' is just 'not enuf buffer', i.e.
    // unable to compress into <= space of in_buf
    return LOG_STATUS(
        Status_CompressionError("gzip Error compressing data!"));
  }
  RETURN_NOT_OK(out_buffer_ptr->realloc(out_buffer_ptr->offset()));
  uint64_t compressed_size = out_buffer_ptr->offset() - overhead_size;
  out_buffer_ptr->reset_offset();
  uint64_t uncompressed_size = nbytes;
  // write sizes to beginning of buffer
  RETURN_NOT_OK(
      out_buffer_ptr->write(&uncompressed_size, sizeof(uncompressed_size)));
  RETURN_NOT_OK(
      out_buffer_ptr->write(&compressed_size, sizeof(compressed_size)));

  return Status::Ok();
}

Status gzip_decompress(shared_ptr<tiledb::sm::ByteVecValue>& out_buf,
                          const uint8_t* comp_buf) {
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

  RETURN_NOT_OK(
      tiledb::sm::GZip::decompress(&gzipped_input_buffer, &pa_gunzip_out_buf));

  return Status::Ok();
}
