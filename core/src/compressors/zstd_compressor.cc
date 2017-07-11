#include <zstd.h>

#include "logger.h"
#include "zstd_compressor.h"

namespace tiledb {

size_t ZStd::compress_bound(size_t nbytes) {
  return ZSTD_compressBound(nbytes);
}

Status ZStd::compress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  *compressed_size = 0;
  // TODO: level
  size_t zstd_code = ZSTD_compress(
      output_buffer, output_buffer_size, input_buffer, input_buffer_size, 1);
  if (ZSTD_isError(zstd_code)) {
    const char* msg = ZSTD_getErrorName(zstd_code);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd compression failed: ") + msg));
  }
  *compressed_size = zstd_code;
  return Status::Ok();
}

Status ZStd::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
  *decompressed_size = 0;
  size_t zstd_code = ZSTD_decompress(
      output_buffer, output_buffer_size, input_buffer, input_buffer_size);
  if (ZSTD_isError(zstd_code)) {
    const char* msg = ZSTD_getErrorName(zstd_code);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd decompression failed: ") + msg));
  }
  *decompressed_size = zstd_code;
  return Status::Ok();
};

}  // namespace tiledb
