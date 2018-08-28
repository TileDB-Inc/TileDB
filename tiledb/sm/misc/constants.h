/**
 * @file   constants.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include <string>

namespace tiledb {
namespace sm {

enum class Datatype : char;
enum class Compressor : char;

namespace constants {

/**
 * If `true`, this will check for coordinate duplicates upon sparse
 * writes.
 */
extern const bool check_coord_dups;

/** If `true`, this will deduplicate coordinates upon sparse writes. */
extern const bool dedup_coords;

/** The object filelock name. */
extern const std::string filelock_name;

/** The array schema file name. */
extern const std::string array_schema_filename;

/** The key-value schema file name. */
extern const std::string kv_schema_filename;

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
extern const std::string coords;

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

/** The special value for an empty ASCII. */
extern const uint8_t empty_ascii;

/** The special value for an empty UTF8. */
extern const uint8_t empty_utf8;

/** The special value for an empty UTF16. */
extern const uint16_t empty_utf16;

/** The special value for an empty UTF32. */
extern const uint32_t empty_utf32;

/** The special value for an empty UCS2. */
extern const uint16_t empty_ucs2;

/** The special value for an empty UCS4. */
extern const uint32_t empty_ucs4;

/** The special value for an empty ANY. */
extern const uint8_t empty_any;

/** The file suffix used in TileDB. */
extern const std::string file_suffix;

/** The fragment metadata file name. */
extern const std::string fragment_metadata_filename;

/** Default datatype for a generic tile. */
extern const Datatype generic_tile_datatype;

/** Default compressor for a generic tile. */
extern Compressor generic_tile_compressor;

/** Default compression level for a generic tile. */
extern int generic_tile_compression_level;

/** Default cell size for a generic tile. */
extern uint64_t generic_tile_cell_size;

/** The group file name. */
extern const std::string group_filename;

/** The buffer size for each attribute used in consolidation. */
extern const uint64_t consolidation_buffer_size;

/** The maximum number of bytes written in a single I/O. */
extern const uint64_t max_write_bytes;

/** The default number of allocated VFS threads. */
extern const uint64_t vfs_num_threads;

/** The default minimum number of bytes in a parallel VFS operation. */
extern const uint64_t vfs_min_parallel_size;

/** The default maximum number of parallel file:/// operations. */
extern const uint64_t vfs_file_max_parallel_ops;

/** The maximum name length. */
extern const unsigned uri_max_len;

/** The maximum file path length (depending on platform). */
extern const unsigned path_max_len;

/** Special value indicating a variable number of elements. */
extern const unsigned int var_num;

/** String describing no compression. */
extern const std::string no_compression_str;

/** The array schema cache size. */
extern const uint64_t array_schema_cache_size;

/** The fragment metadata cache size. */
extern const uint64_t fragment_metadata_cache_size;

/** Whether or not the signal handlers are installed. */
extern const bool enable_signal_handlers;

/** The number of threads allocated per StorageManager for async queries. */
extern const uint64_t num_async_threads;

/** The number of threads allocated per StorageManager for read operations. */
extern const uint64_t num_reader_threads;

/** The number of threads allocated per StorageManager for write operations. */
extern const uint64_t num_writer_threads;

/** The number of threads allocated for TBB. */
extern const int num_tbb_threads;

/** The tile cache size. */
extern const uint64_t tile_cache_size;

/** Empty String reference **/
extern const std::string empty_str;

/** TILEDB_READ Query String **/
extern const std::string query_type_read_str;

/** TILEDB_WRITE Query String **/
extern const std::string query_type_write_str;

/** TILEDB_FAILED Query String **/
extern const std::string query_status_failed_str;

/** TILEDB_COMPLETE Query String **/
extern const std::string query_status_completed_str;

/** TILEDB_INPROGRESS Query String **/
extern const std::string query_status_inprogress_str;

/** TILEDB_INCOMPLETE Query String **/
extern const std::string query_status_incomplete_str;

/** TILEDB_UNINITIALIZED Query String **/
extern const std::string query_status_uninitialized_str;

/** TILEDB_COMPRESSION Filter type string */
extern const std::string filter_type_compression_str;

/** String describing GZIP. */
extern const std::string gzip_str;

/** String describing ZSTD. */
extern const std::string zstd_str;

/** String describing LZ4. */
extern const std::string lz4_str;

/** String describing BLOSC. */
extern const std::string blosc_lz_str;

/** String describing BLOSC_LZ4. */
extern const std::string blosc_lz4_str;

/** String describing BLOSC_LZ4HC. */
extern const std::string blosc_lz4hc_str;

/** String describing BLOSC_SNAPPY. */
extern const std::string blosc_snappy_str;

/** String describing BLOSC_ZLIB. */
extern const std::string blosc_zlib_str;

/** String describing BLOSC_ZSTD. */
extern const std::string blosc_zstd_str;

/** String describing RLE. */
extern const std::string rle_str;

/** String describing BZIP2. */
extern const std::string bzip2_str;

/** String describing DOUBLE_DELTA. */
extern const std::string double_delta_str;

/** The string representation for type int32. */
extern const std::string int32_str;

/** The string representation for type int64. */
extern const std::string int64_str;

/** The string representation for type float32. */
extern const std::string float32_str;

/** The string representation for type float64. */
extern const std::string float64_str;

/** The string representation for type char. */
extern const std::string char_str;

/** The string representation for type int8. */
extern const std::string int8_str;

/** The string representation for type uint8. */
extern const std::string uint8_str;

/** The string representation for type int16. */
extern const std::string int16_str;

/** The string representation for type uint16. */
extern const std::string uint16_str;

/** The string representation for type uint32. */
extern const std::string uint32_str;

/** The string representation for type uint64. */
extern const std::string uint64_str;

/** The string representation for type STRING_ASCII. */
extern const std::string string_ascii_str;

/** The string representation for type STRING_UTF8. */
extern const std::string string_utf8_str;

/** The string representation for type STRING_UTF16. */
extern const std::string string_utf16_str;

/** The string representation for type STRING_UTF32. */
extern const std::string string_utf32_str;

/** The string representation for type STRING_UCS2. */
extern const std::string string_ucs2_str;

/** The string representation for type STRING_UCS4. */
extern const std::string string_ucs4_str;

/** The string representation for type ANY. */
extern const std::string any_str;

/** The string representation for the dense array type. */
extern const std::string dense_str;

/** The string representation for the sparse array type. */
extern const std::string sparse_str;

/** The string representation for the column-major layout. */
extern const std::string col_major_str;

/** The string representation for the row-major layout. */
extern const std::string row_major_str;

/** The string representation for the global order layout. */
extern const std::string global_order_str;

/** The string representation for the unordered layout. */
extern const std::string unordered_str;

/** The string representation of null. */
extern const std::string null_str;

/** The version in format { major, minor, revision }. */
extern const int version[3];

/** The maximum size of a tile chunk (unit of compression) in bytes. */
extern const uint64_t max_tile_chunk_size;

/** The default attribute name prefix. */
extern const std::string default_attr_name;

/** The default dimension name prefix. */
extern const std::string default_dim_name;

/** The key attribute name. */
extern const std::string key_attr_name;

/** The key attribute type. */
extern Datatype key_attr_type;

/** The key type attribute name. */
extern const std::string key_type_attr_name;

/** The key type attribute type. */
extern Datatype key_type_attr_type;

/** The key attribute compressor. */
extern Compressor key_attr_compressor;

/** The key type attribute compressor. */
extern Compressor key_type_attr_compressor;

/**
 * The name of the first key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
extern const std::string key_dim_1;

/**
 * The name of the second key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
extern const std::string key_dim_2;

/** Maximum number of items to be buffered before a flush. */
extern uint64_t kv_max_items;

/** Maximum number of attempts to wait for an S3 response. */
extern const unsigned int s3_max_attempts;

/** Milliseconds of wait time between S3 attempts. */
extern const unsigned int s3_attempt_sleep_ms;

/** An allocation tag used for logging. */
extern const std::string s3_allocation_tag;

/** Use virtual addressing (false for minio, true for AWS S3). */
extern const bool s3_use_virtual_addressing;

/** Connect timeout in milliseconds. */
extern const long s3_connect_timeout_ms;

/** Connect max tries. */
extern const long s3_connect_max_tries;

/** Connect scale factor for exponential backoff. */
extern const long s3_connect_scale_factor;

/** Request timeout in milliseconds. */
extern const long s3_request_timeout_ms;

/** S3 scheme (http for local minio, https for AWS S3). */
extern const std::string s3_scheme;

/** The default maximum number of parallel S3 operations. */
extern const uint64_t s3_max_parallel_ops;

/** Size of parts used in the S3 multi-part uploads. */
extern const uint64_t s3_multipart_part_size;

/** S3 region. */
extern const std::string s3_region;

/** S3 endpoint override. */
extern const std::string s3_endpoint_override;

/** S3 proxy scheme. */
extern const std::string s3_proxy_scheme;

/** S3 proxy host. */
extern const std::string s3_proxy_host;

/** S3 proxy port. */
extern const unsigned s3_proxy_port;

/** S3 proxy username. */
extern const std::string s3_proxy_username;

/** S3 proxy password. */
extern const std::string s3_proxy_password;

/** HDFS default kerb ticket cache path. */
extern const std::string hdfs_kerb_ticket_cache_path;

/** HDFS default name node uri. */
extern const std::string hdfs_name_node_uri;

/** HDFS default username. */
extern const std::string hdfs_username;

/** Prefix indicating a special name reserved by TileDB. */
extern const std::string special_name_prefix;

/** Number of milliseconds between watchdog thread wakeups. */
extern const unsigned watchdog_thread_sleep_ms;

/** Returns the empty fill value based on the input datatype. */
const void* fill_value(Datatype type);

}  // namespace constants

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONSTANTS_H
