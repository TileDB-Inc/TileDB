#include <blosc.h>
#include <cassert>
#include <limits>

#include "blosc_compressor.h"

namespace tiledb {

size_t Blosc::compress_bound(size_t nbytes) {
  // TODO: this may overflow
  return nbytes + BLOSC_MAX_OVERHEAD;
}

Status Blosc::compress(
    const char* compressor,
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  *compressed_size = 0;
  assert(input_buffer_size <= std::numeric_limits<int>::max());
  assert(output_buffer_size <= std::numeric_limits<int>::max());
  blosc_init();
  if (blosc_set_compressor(compressor) < 0) {
    return Status::CompressionError(
        std::string(
            "Blosc compression error, failed to set Blosc compressor ") +
        compressor);
  }
  int rc = blosc_compress(
      5,  // level
      1,  // shuffle
      type_size,
      input_buffer_size,
      input_buffer,
      output_buffer,
      output_buffer_size);
  blosc_destroy();
  if (rc < 0) {
    return Status::CompressionError("Blosc compression error");
  }
  *compressed_size = rc;
  return Status::Ok();
}

Status Blosc::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
  *decompressed_size = 0;
  assert(input_buffer_size <= std::numeric_limits<int>::max());
  assert(output_buffer_size <= std::numeric_limits<int>::max());
  // TODO: this should be performed once
  blosc_init();
  int rc = blosc_decompress(
      static_cast<char*>(input_buffer), output_buffer, output_buffer_size);
  blosc_destroy();
  if (rc <= 0) {
    return Status::CompressionError("Blosc decompress error");
  }
  *decompressed_size = static_cast<int>(rc);
  return Status::Ok();
}

};  // namespace tiledb