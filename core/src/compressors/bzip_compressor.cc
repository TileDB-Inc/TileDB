#include <bzlib.h>
#include <math.h>
#include <cassert>
#include <limits>

#include "bzip_compressor.h"

namespace tiledb {

size_t BZip::compress_bound(size_t nbytes) {
  // From the BZip2 documentation:
  // To guarantee that the compressed data will fit in its buffer, allocate an
  // output buffer of size 1% larger than the uncompressed data, plus six
  // hundred extra bytes.
  float fbytes = ceil(nbytes * 1.01) + 600;
  assert(fbytes <= std::numeric_limits<size_t>::max());
  return static_cast<size_t>(fbytes);
}

Status BZip::compress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  *compressed_size = 0;
  assert(input_buffer_size <= std::numeric_limits<unsigned int>::max());
  assert(output_buffer_size <= std::numeric_limits<unsigned int>::max());
  unsigned int out_size = output_buffer_size;
  int rc = BZ2_bzBuffToBuffCompress(
      static_cast<char*>(output_buffer),
      &out_size,
      static_cast<char*>(input_buffer),
      input_buffer_size,
      0,   // block size 100k
      0,   // verbosity
      0);  // work factor
  if (rc != BZ_OK) {
    switch (rc) {
      case BZ_CONFIG_ERROR:
        return Status::CompressionError(
            "BZip compression error: library has been miscompiled");
      case BZ_PARAM_ERROR:
        return Status::CompressionError(
            "BZip compression error: 'output_buffer' or 'output_buffer_size' "
            "is NULL");
      case BZ_MEM_ERROR:
        return Status::CompressionError(
            "BZip compression error: insufficient memory");
      case BZ_OUTBUFF_FULL:
        return Status::CompressionError(
            "BZip compression error: compressed size exceeds limits for "
            "'output_buffer_size'");
      default:
        return Status::CompressionError(
            "BZip compression error: unknown error code");
    }
  }
  *compressed_size = out_size;
  return Status::Ok();
}

Status BZip::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
  *decompressed_size = 0;
  assert(input_buffer_size <= std::numeric_limits<unsigned int>::max());
  assert(output_buffer_size <= std::numeric_limits<unsigned int>::max());
  unsigned int out_size = output_buffer_size;
  int rc = BZ2_bzBuffToBuffDecompress(
      static_cast<char*>(output_buffer),
      &out_size,
      static_cast<char*>(input_buffer),
      input_buffer_size,
      0,   // small bzip data format stream
      0);  // verbositiy
  if (rc != BZ_OK) {
    switch (rc) {
      case BZ_CONFIG_ERROR:
        return Status::CompressionError(
            "BZip decompression error: library has been miscompiled");
      case BZ_PARAM_ERROR:
        return Status::CompressionError(
            "BZip decompression error: 'output_buffer' or 'output_buffer_size' "
            "is NULL");
      case BZ_MEM_ERROR:
        return Status::CompressionError(
            "BZip decompression error: insufficient memory");
      case BZ_DATA_ERROR:
      case BZ_DATA_ERROR_MAGIC:
      case BZ_UNEXPECTED_EOF:
        return Status::CompressionError(
            "BZip decompression error: compressed data is corrupted");
      default:
        return Status::CompressionError(
            "BZip decompression error: unknown error code ");
    }
  }
  *decompressed_size = out_size;
  return Status::Ok();
};

}  // namespace tiledb