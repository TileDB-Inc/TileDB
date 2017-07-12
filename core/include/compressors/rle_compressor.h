#ifndef TILEDB_RLE_COMPRESSOR_H
#define TILEDB_RLE_COMPRESSOR_H

#include "base_compressor.h"
#include "status.h"

namespace tiledb {

class RLE : public BaseCompressor {
 public:
  /**
   * Returns the maximum size of the output of RLE compression.
   *
   * @param nbytes The input buffer size in bytes.
   * @param type_size The size of a single value type in the input buffer.
   * @return The maximum size of the output after RLE-compressing the input with
   *     size input_size.
   */
  static size_t compress_bound(size_t nbytes, size_t type_size);

  static Status compress(
      size_t type_size,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* compressed_size);

  static Status decompress(
      size_t type_size,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* decompressed_size);
};

}  // namespace tiledb
#endif  // TILEDB_RLE_COMPRESSOR_H
