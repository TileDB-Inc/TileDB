/**
 * @file   constants.h
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
 * This file declares the TileDB constants.
 */

#ifndef TILEDB_CONSTANTS_H
#define TILEDB_CONSTANTS_H

#include <cinttypes>

namespace tiledb {

enum class Datatype : char;
enum class Compressor : char;

namespace constants {

/** The array filelock name. */
extern const char* array_filelock_name;

/** The array metadata file name. */
extern const char* array_metadata_filename;

/** The default tile capacity. */
extern const uint64_t capacity;

/** The size of a variable cell offset. */
extern const uint64_t cell_var_offset_size;

/** The type of a variable cell offset. */
extern const Datatype cell_var_offset_type;

/** A special value indicating varibale size. */
extern const uint64_t var_size;

/** The default compressor for the offsets of variable-sized cells. */
extern Compressor cell_var_offsets_compression;

/** The default compression level for the offsets of variable-sized cells. */
extern int cell_var_offsets_compression_level;

/** The default compressor for the coordinates. */
extern Compressor coords_compression;

/** The default compressor for real coordinates. */
extern Compressor real_coords_compression;

/** The default compression level for the coordinates. */
extern int coords_compression_level;

/** Special name reserved for the coordinates attribute. */
extern const char* coords;

/** The special value for an empty int32. */
extern const int empty_int32;

/** The special value for an empty int64. */
extern const int64_t empty_int64;

/** The special value for an empty float32. */
extern const float empty_float32;

/** The special value for an empty float64. */
extern const double empty_float64;

/** The special value for an empty char. */
extern const char empty_char;

/** The special value for an empty int8. */
extern const int8_t empty_int8;

/** The special value for an empty uint8. */
extern const uint8_t empty_uint8;

/** The special value for an empty int16. */
extern const int16_t empty_int16;

/** The special value for an empty uint16. */
extern const uint16_t empty_uint16;

/** The special value for an empty uint32. */
extern const uint32_t empty_uint32;

/** The special value for an empty uint64. */
extern const uint64_t empty_uint64;

/** The file suffix used in TileDB. */
extern const char* file_suffix;

/** The fragment file name. */
extern const char* fragment_filename;

/** The fragment metadata file name. */
extern const char* fragment_metadata_filename;

/** Default datatype for a generic tile. */
extern const Datatype generic_tile_datatype;

/** Default compressor for a generic tile. */
extern Compressor generic_tile_compressor;

/** Default compression level for a generic tile. */
extern int generic_tile_compression_level;

/** Default cell size for a generic tile. */
extern uint64_t generic_tile_cell_size;

/** The group file name. */
extern const char* group_filename;

/** The initial internal buffer size for the case of sparse arrays. */
extern const uint64_t internal_buffer_size;

/** The buffer size for each attribute used in consolidation. */
extern const uint64_t consolidation_buffer_size;

/** The maximum number of bytes written in a single I/O. */
extern const uint64_t max_write_bytes;

/** The maximum name length. */
extern const unsigned name_max_len;

/** The size of the buffer that holds the sorted cells. */
extern const uint64_t sorted_buffer_size;

/** The size of the buffer that holds the sorted variable cells. */
extern const uint64_t sorted_buffer_var_size;

/** Special value indicating a variable number of elements. */
extern const unsigned int var_num;

/** String describing no compression. */
extern const char* no_compression_str;

/** The array metadata cache size. */
extern const uint64_t array_metadata_cache_size;

/** The fragment metadata cache size. */
extern const uint64_t fragment_metadata_cache_size;

/** The tile cache size. */
extern const uint64_t tile_cache_size;

/** String describing GZIP. */
extern const char* gzip_str;

/** String describing ZSTD. */
extern const char* zstd_str;

/** String describing LZ4. */
extern const char* lz4_str;

/** String describing BLOSC. */
extern const char* blosc_str;

/** String describing BLOSC_LZ4. */
extern const char* blosc_lz4_str;

/** String describing BLOSC_LZ4HC. */
extern const char* blosc_lz4hc_str;

/** String describing BLOSC_SNAPPY. */
extern const char* blosc_snappy_str;

/** String describing BLOSC_ZLIB. */
extern const char* blosc_zlib_str;

/** String describing BLOSC_ZSTD. */
extern const char* blosc_zstd_str;

/** String describing RLE. */
extern const char* rle_str;

/** String describing BZIP2. */
extern const char* bzip2_str;

/** String describing DOUBLE_DELTA. */
extern const char* double_delta_str;

/** The string representation for type int32. */
extern const char* int32_str;

/** The string representation for type int64. */
extern const char* int64_str;

/** The string representation for type float32. */
extern const char* float32_str;

/** The string representation for type float64. */
extern const char* float64_str;

/** The string representation for type char. */
extern const char* char_str;

/** The string representation for type int8. */
extern const char* int8_str;

/** The string representation for type uint8. */
extern const char* uint8_str;

/** The string representation for type int16. */
extern const char* int16_str;

/** The string representation for type uint16. */
extern const char* uint16_str;

/** The string representation for type uint32. */
extern const char* uint32_str;

/** The string representation for type uint64. */
extern const char* uint64_str;

/** The string representation for the dense array type. */
extern const char* dense_str;

/** The string representation for the sparse array type. */
extern const char* sparse_str;

/** The string representation for the column-major layout. */
extern const char* col_major_str;

/** The string representation for the row-major layout. */
extern const char* row_major_str;

/** The string representation for the global order layout. */
extern const char* global_order_str;

/** The string representation for the unordered layout. */
extern const char* unordered_str;

/** The string representation of null. */
extern const char* null_str;

/** The version in format { major, minor, revision }. */
extern const int version[3];

/** The size of a tile chunk. */
extern const uint64_t tile_chunk_size;

/** The default attribute name prefix. */
extern const char* default_attr_name;

/** The default dimension name prefix. */
extern const char* default_dim_name;

/** The key attribute name. */
extern const char* key_attr_name;

/** The key type attribute name. */
extern const char* key_type_attr_name;

/** The key attribute compressor. */
extern Compressor key_attr_compressor;

/** The key type attribute compressor. */
extern Compressor key_type_attr_compressor;

/**
 * The name of the first key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
extern const char* key_dim_1;

/**
 * The name of the second key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
extern const char* key_dim_2;

/** Name of special empty file that indicates a key-value store. */
extern const char* kv_filename;

/**
 * Default size to be allocated for a key-value internal buffer when reading
 * from a key-value store.
 */
extern uint64_t kv_buffer_size;

#ifdef HAVE_S3
/** A directory file suffix (in S3, directories are also files). */
extern const char* s3_dir_suffix;

/** Maximum number of attempts to wait for an S3 response. */
extern const unsigned int s3_max_attempts;

/** An allocation tag used for logging. */
extern const char* s3_allocation_tag;

/** Use virtual addressing (false for minio, true for AWS S3). */
extern const bool s3_use_virtual_addressing;

/** Connect timeout in milliseconds. */
extern const long s3_connect_timeout_ms;

/** Request timeout in milliseconds. */
extern const long s3_request_timeout_ms;

/** S3 scheme (http for local minio, https for AWS S3). */
extern const char* s3_scheme;

/** Size of file buffers used in the S3 multi-part uploads. */
extern const uint64_t s3_file_buffer_size;

/** S3 region. */
extern const char* s3_region;

/** S3 endpoint override. */
extern const char* s3_endpoint_override;
#endif

}  // namespace constants

}  // namespace tiledb

#endif  // TILEDB_CONSTANTS_H
