#ifndef TILEDB_BZIP_COMPRESSOR_H
#define TILEDB_BZIP_COMPRESSOR_H

#include "base_compressor.h"
#include "status.h"

namespace tiledb {

class BZip : public BaseCompressor {
 public:
  static size_t compress_bound(size_t nbytes);

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

};      // namespace tiledb
#endif  // TILEDB_BZIP_COMPRESSOR_H
