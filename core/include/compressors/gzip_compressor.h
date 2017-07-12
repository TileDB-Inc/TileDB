#ifndef TILEDB_GZIP_H
#define TILEDB_GZIP_H

#include "base_compressor.h"
#include "status.h"

namespace tiledb {

class GZip : public BaseCompressor {
 public:
  static size_t compress_bound(size_t nbytes);

  /**
   * GZIPs the input buffer and stores the result in the output buffer,
   * returning the size of compressed data.
   *
   * @param in The input buffer.
   * @param in_size The size of the input buffer.
   * @param out The output buffer.
   * @param out_size The available size in the output buffer.
   * @return Status
   */
  static Status compress(
      size_t type_size,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* compressed_size);
  /**
   * Decompresses the GZIPed input buffer and stores the result in the output
   * buffer, of maximum size avail_out.
   *
   * @param in The input buffer.
   * @param in_size The size of the input buffer.
   * @param out The output buffer.
   * @param avail_out_size The available size in the output buffer.
   * @param out_size The size of the decompressed data.
   * @return Status
   */
  static Status decompress(
      size_t type_size,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* decompressed_size);
};

};  // namespace tiledb

#endif  // TILEDB_GZIP_H
