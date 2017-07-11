#include <lz4.h>
#include <cassert>
#include <limits>

#include "lz4_compressor.h"

namespace tiledb {

// TOOD: LZ4_MAX_INPUT_SIZE...
size_t LZ4::compress_bound(size_t nbytes) {
  assert(nbytes <= std::numeric_limits<int>::max());
  return static_cast<size_t>(LZ4_compressBound(static_cast<int>(nbytes)));
}

Status LZ4::compress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  *compressed_size = 0;
  // TODO: these parameters are int and can overflow with size_t, need to check
  // input. need something better than an assertion here
  assert(input_buffer_size <= std::numeric_limits<int>::max());
  assert(output_buffer_size <= std::numeric_limits<int>::max());
  int ret = LZ4_compress(
      static_cast<char*>(input_buffer),
      static_cast<char*>(output_buffer),
      input_buffer_size);
  if (ret < 0) {
    return Status::CompressionError("LZ4 compression failed");
  }
  *compressed_size = static_cast<size_t>(ret);
  return Status::Ok();
}

Status LZ4::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
  *decompressed_size = 0;
  assert(input_buffer_size <= std::numeric_limits<int>::max());
  assert(output_buffer_size <= std::numeric_limits<int>::max());
  int ret = LZ4_decompress_safe(
      static_cast<char*>(input_buffer),
      static_cast<char*>(output_buffer),
      input_buffer_size,
      output_buffer_size);
  if (ret < 0) {
    return Status::CompressionError("LZ4 decompression failed");
  }
  *decompressed_size = static_cast<size_t>(ret);
  return Status::Ok();
};

}  // namespace tiledb