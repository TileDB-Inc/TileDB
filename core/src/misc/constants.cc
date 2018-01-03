/**
 * @file   constants.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines the TileDB constants.
 */

#include <cstdint>
#include <limits>

#include "compressor.h"
#include "datatype.h"

namespace tiledb {

namespace constants {

/** The array metadata file name. */
const char* array_metadata_filename = "__array_metadata.tdb";

/** The fragment file name. */
const char* fragment_filename = "__fragment.tdb";

/** The fragment metadata file name. */
const char* fragment_metadata_filename = "__fragment_metadata.tdb";

/** The default tile capacity. */
const uint64_t capacity = 10000;

/** The size of a variable cell offset. */
const uint64_t cell_var_offset_size = sizeof(uint64_t);

/** The type of a variable cell offset. */
const Datatype cell_var_offset_type = Datatype::UINT64;

/** A special value indicating varibale size. */
const uint64_t var_size = std::numeric_limits<uint64_t>::max();

/** The default compressor for the offsets of variable-sized cells. */
Compressor cell_var_offsets_compression = Compressor::BLOSC_ZSTD;

/** The default compression level for the offsets of variable-sized cells. */
int cell_var_offsets_compression_level = -1;

/** Special name reserved for the coordinates attribute. */
const char* coords = "__coords";

/** The default compressor for the coordinates. */
Compressor coords_compression = Compressor::BLOSC_ZSTD;

/** The default compressor for real coordinates. */
Compressor real_coords_compression = Compressor::BLOSC_ZSTD;

/** The default compression level for the coordinates. */
int coords_compression_level = -1;

/** The array filelock name. */
const char* array_filelock_name = "__array_lock.tdb";

/** The special value for an empty int32. */
const int empty_int32 = std::numeric_limits<int32_t>::max();

/** The special value for an empty int64. */
const int64_t empty_int64 = std::numeric_limits<int64_t>::max();

/** The special value for an empty float32. */
const float empty_float32 = std::numeric_limits<float>::max();

/** The special value for an empty float64. */
const double empty_float64 = std::numeric_limits<double>::max();

/** The special value for an empty char. */
const char empty_char = std::numeric_limits<char>::max();

/** The special value for an empty int8. */
const int8_t empty_int8 = std::numeric_limits<int8_t>::max();

/** The special value for an empty uint8. */
const uint8_t empty_uint8 = std::numeric_limits<uint8_t>::max();

/** The special value for an empty int16. */
const int16_t empty_int16 = std::numeric_limits<int16_t>::max();

/** The special value for an empty uint16. */
const uint16_t empty_uint16 = std::numeric_limits<uint16_t>::max();

/** The special value for an empty uint32. */
const uint32_t empty_uint32 = std::numeric_limits<uint32_t>::max();

/** The special value for an empty uint64. */
const uint64_t empty_uint64 = std::numeric_limits<uint64_t>::max();

/** The file suffix used in TileDB. */
const char* file_suffix = ".tdb";

/** Default datatype for a generic tile. */
const Datatype generic_tile_datatype = Datatype::CHAR;

/** Default compressor for a generic tile. */
Compressor generic_tile_compressor = Compressor::BLOSC_ZSTD;

/** Default compression level for a generic tile. */
int generic_tile_compression_level = -1;

/** Default cell size for a generic tile. */
uint64_t generic_tile_cell_size = sizeof(char);

/** The group file name. */
const char* group_filename = "__tiledb_group.tdb";

/** The initial internal buffer size for the case of sparse arrays. */
const uint64_t internal_buffer_size = 10000000;

/** The buffer size for each attribute used in consolidation. */
const uint64_t consolidation_buffer_size = 10000000;

/** The maximum number of bytes written in a single I/O. */
const uint64_t max_write_bytes = std::numeric_limits<int>::max();

/** The maximum name length. */
const unsigned name_max_len = 256;

/** The size of the buffer that holds the sorted cells. */
const uint64_t sorted_buffer_size = 10000000;

/** The size of the buffer that holds the sorted variable cells. */
const uint64_t sorted_buffer_var_size = 10000000;

/** Special value indicating a variable number of elements. */
const unsigned int var_num = std::numeric_limits<unsigned int>::max();

/** String describing no compression. */
const char* no_compression_str = "NO_COMPRESSION";

/** The array metadata cache size. */
const uint64_t array_metadata_cache_size = 10000000;

/** The fragment metadata cache size. */
const uint64_t fragment_metadata_cache_size = 100000000;

/** The tile cache size. */
const uint64_t tile_cache_size = 100000000;

/** String describing GZIP. */
const char* gzip_str = "GZIP";

/** String describing ZSTD. */
const char* zstd_str = "ZSTD";

/** String describing LZ4. */
const char* lz4_str = "LZ4";

/** String describing BLOSC. */
const char* blosc_str = "BLOSC_LZ";

/** String describing BLOSC_LZ4. */
const char* blosc_lz4_str = "BLOSC_LZ4";

/** String describing BLOSC_LZ4HC. */
const char* blosc_lz4hc_str = "BLOSC_LZ4HC";

/** String describing BLOSC_SNAPPY. */
const char* blosc_snappy_str = "BLOSC_SNAPPY";

/** String describing BLOSC_ZLIB. */
const char* blosc_zlib_str = "BLOSC_ZLIB";

/** String describing BLOSC_ZSTD. */
const char* blosc_zstd_str = "BLOSC_ZSTD";

/** String describing RLE. */
const char* rle_str = "RLE";

/** String describing BZIP2. */
const char* bzip2_str = "BZIP2";

/** String describing DOUBLE_DELTA. */
const char* double_delta_str = "DOUBLE_DELTA";

/** The string representation for type int32. */
const char* int32_str = "INT32";

/** The string representation for type int64. */
const char* int64_str = "INT64";

/** The string representation for type float32. */
const char* float32_str = "FLOAT32";

/** The string representation for type float64. */
const char* float64_str = "FLOAT64";

/** The string representation for type char. */
const char* char_str = "CHAR";

/** The string representation for type int8. */
const char* int8_str = "INT8";

/** The string representation for type uint8. */
const char* uint8_str = "UINT8";

/** The string representation for type int16. */
const char* int16_str = "INT16";

/** The string representation for type uint16. */
const char* uint16_str = "UINT16";

/** The string representation for type uint32. */
const char* uint32_str = "UINT32";

/** The string representation for type uint64. */
const char* uint64_str = "UINT64";

/** The string representation for the dense array type. */
const char* dense_str = "dense";

/** The string representation for the sparse array type. */
const char* sparse_str = "sparse";

/** The string representation for the column-major layout. */
const char* col_major_str = "col-major";

/** The string representation for the row-major layout. */
const char* row_major_str = "row-major";

/** The string representation for the global order layout. */
const char* global_order_str = "global-order";

/** The string representation for the unordered layout. */
const char* unordered_str = "unordered";

/** The string representation of null. */
const char* null_str = "null";

/** The version in format { major, minor, revision }. */
const int version[3] = {1, 2, 0};

/** The size of a tile chunk. */
const uint64_t tile_chunk_size = (uint64_t)std::numeric_limits<int>::max();

/** The default attribute name prefix. */
const char* default_attr_name = "__attr";

/** The default dimension name prefix. */
const char* default_dim_name = "__dim";

/** The key attribute name. */
const char* key_attr_name = "__key";

/** The key type attribute name. */
const char* key_type_attr_name = "__key_type";

/** The key attribute compressor. */
Compressor key_attr_compressor = Compressor::BLOSC_ZSTD;

/** The key type attribute compressor. */
Compressor key_type_attr_compressor = Compressor::BLOSC_ZSTD;

/**
 * The name of the first key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
const char* key_dim_1 = "__key_dim_1";

/**
 * The name of the second key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
const char* key_dim_2 = "__key_dim_2";

/** Name of special empty file that indicates a key-value store. */
const char* kv_filename = "__kv.tdb";

/**
 * Default size to be allocated for an internal key-value buffer when reading
 * from a key-value store.
 */
uint64_t kv_buffer_size = 100000;

#ifdef HAVE_S3
/** A directory file suffix (in S3, directories are also files). */
const char* s3_dir_suffix = ".dir";

/** Maximum number of attempts to wait for an S3 response. */
const unsigned int s3_max_attempts = 1000;

/** An allocation tag used for logging. */
const char* s3_allocation_tag = "TileDB";

/** Use virtual addressing (false for minio, true for AWS S3). */
const bool s3_use_virtual_addressing = false;

/** Connect timeout in milliseconds. */
const long s3_connect_timeout_ms = 3000;

/** Request timeout in milliseconds. */
const long s3_request_timeout_ms = 3000;

/** S3 scheme (http for local minio, https for AWS S3). */
const char* s3_scheme = "http";

/** Size of file buffers used in the S3 multi-part uploads. */
const uint64_t s3_file_buffer_size = 5 * 1024 * 1024;

/** S3 region. */
const char* s3_region = "";

/** S3 endpoint override. */
const char* s3_endpoint_override = "localhost:9000";
#endif

}  // namespace constants

}  // namespace tiledb
