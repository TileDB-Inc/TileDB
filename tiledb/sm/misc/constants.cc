/**
 * @file   constants.cc
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
 * This file defines the TileDB constants.
 */

#include <cstdint>
#include <limits>
#include <thread>

#ifdef HAVE_TBB
#include <tbb/task_scheduler_init.h>
#endif

#include "tiledb/sm/c_api/tiledb_version.h"

// Include files for platform path max definition.
#ifdef _WIN32
#include "tiledb/sm/misc/win_constants.h"
#elif __APPLE__
#include <sys/syslimits.h>
#else
#include <limits.h>
#endif

#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"

namespace tiledb {
namespace sm {

namespace constants {

/**
 * If `true`, this will check for coordinate duplicates upon sparse
 * writes.
 */
const bool check_coord_dups = true;

/**
 * If `true`, this will check for out-of-bound coordinates upon sparse
 * writes.
 */
const bool check_coord_oob = true;

/** If `true`, this will deduplicate coordinates upon sparse writes. */
const bool dedup_coords = false;

/** The array schema file name. */
const std::string array_schema_filename = "__array_schema.tdb";

/** The key-value schema file name. */
const std::string kv_schema_filename = "__kv_schema.tdb";

/** The fragment metadata file name. */
const std::string fragment_metadata_filename = "__fragment_metadata.tdb";

/** The default tile capacity. */
const uint64_t capacity = 10000;

/** The size of a variable cell offset. */
const uint64_t cell_var_offset_size = sizeof(uint64_t);

/** The type of a variable cell offset. */
const Datatype cell_var_offset_type = Datatype::UINT64;

/** A special value indicating varibale size. */
const uint64_t var_size = std::numeric_limits<uint64_t>::max();

/** The default compressor for the offsets of variable-sized cells. */
Compressor cell_var_offsets_compression = Compressor::ZSTD;

/** The default compression level for the offsets of variable-sized cells. */
int cell_var_offsets_compression_level = -1;

/** Special name reserved for the coordinates attribute. */
const std::string coords = "__coords";

/** The default compressor for the coordinates. */
Compressor coords_compression = Compressor::ZSTD;

/** The default compressor for real coordinates. */
Compressor real_coords_compression = Compressor::ZSTD;

/** The default compression level for the coordinates. */
int coords_compression_level = -1;

/** The filelock name. */
const std::string filelock_name = "__lock.tdb";

/** The special value for an empty int32. */
const int empty_int32 = std::numeric_limits<int32_t>::min();

/** The special value for an empty int64. */
const int64_t empty_int64 = std::numeric_limits<int64_t>::min();

/** The special value for an empty float32. */
const float empty_float32 = std::numeric_limits<float>::quiet_NaN();

/** The special value for an empty float64. */
const double empty_float64 = std::numeric_limits<double>::quiet_NaN();

/** The special value for an empty char. */
const char empty_char = std::numeric_limits<char>::min();

/** The special value for an empty int8. */
const int8_t empty_int8 = std::numeric_limits<int8_t>::min();

/** The special value for an empty uint8. */
const uint8_t empty_uint8 = std::numeric_limits<uint8_t>::max();

/** The special value for an empty int16. */
const int16_t empty_int16 = std::numeric_limits<int16_t>::min();

/** The special value for an empty uint16. */
const uint16_t empty_uint16 = std::numeric_limits<uint16_t>::max();

/** The special value for an empty uint32. */
const uint32_t empty_uint32 = std::numeric_limits<uint32_t>::max();

/** The special value for an empty uint64. */
const uint64_t empty_uint64 = std::numeric_limits<uint64_t>::max();

/** The special value for an empty ASCII. */
const uint8_t empty_ascii = 0;

/** The special value for an empty UTF8. */
const uint8_t empty_utf8 = 0;

/** The special value for an empty UTF16. */
const uint16_t empty_utf16 = 0;

/** The special value for an empty UTF32. */
const uint32_t empty_utf32 = 0;

/** The special value for an empty UCS2. */
const uint16_t empty_ucs2 = 0;

/** The special value for an empty UCS4. */
const uint32_t empty_ucs4 = 0;

/** The special value for an empty ANY. */
const uint8_t empty_any = 0;

/** The file suffix used in TileDB. */
const std::string file_suffix = ".tdb";

/** Default datatype for a generic tile. */
const Datatype generic_tile_datatype = Datatype::CHAR;

/** Default compressor for a generic tile. */
Compressor generic_tile_compressor = Compressor::GZIP;

/** Default compression level for a generic tile. */
int generic_tile_compression_level = 1;

/** Default cell size for a generic tile. */
uint64_t generic_tile_cell_size = sizeof(char);

/** The group file name. */
const std::string group_filename = "__tiledb_group.tdb";

/**
 * The factor by which the size of the dense fragment resulting
 * from consolidating a set of fragments (containing at least one
 * dense fragment) can be amplified. This is important when
 * the union of the non-empty domains of the fragments to be
 * consolidated have a lot of empty cells, which the consolidated
 * fragment will have to fill with the special fill value
 * (since the resulting fragment is dense).
 */
const float consolidation_amplification = 1.0f;

/** The buffer size for each attribute used in consolidation. */
const uint64_t consolidation_buffer_size = 50000000;

/** Number of steps in the consolidation algorithm. */
const uint32_t consolidation_steps = std::numeric_limits<unsigned>::max();

/** Minimum number of fragments to consolidate per step. */
const uint32_t consolidation_step_min_frags = UINT32_MAX;

/** Maximum number of fragments to consolidate per step. */
const uint32_t consolidation_step_max_frags = UINT32_MAX;

/**
 * Size ratio of two fragments to be considered for consolidation in a step.
 * This should be a value in [0.0, 1.0].
 * 0.0 means always consolidate and 1.0 consolidate only if sizes are equal.
 */
const float consolidation_step_size_ratio = 0.0f;

/** The maximum number of bytes written in a single I/O. */
const uint64_t max_write_bytes = std::numeric_limits<int>::max();

/** The default number of allocated VFS threads. */
const uint64_t vfs_num_threads = std::thread::hardware_concurrency();

/** The default minimum number of bytes in a parallel VFS operation. */
const uint64_t vfs_min_parallel_size = 10 * 1024 * 1024;

/** The default maximum number of bytes in a batched VFS read operation. */
const uint64_t vfs_max_batch_read_size = 100 * 1024 * 1024;

/** The default maximum amplification factor for batched VFS read operations. */
const float vfs_max_batch_read_amplification = 1.0;

/** The default maximum number of parallel file:/// operations. */
const uint64_t vfs_file_max_parallel_ops = vfs_num_threads;

/** The maximum name length. */
const uint32_t uri_max_len = 256;

/** The maximum file path length (depending on platform). */
#ifndef _WIN32
const uint32_t path_max_len = PATH_MAX;
#endif

/** Special value indicating a variable number of elements. */
const uint32_t var_num = std::numeric_limits<unsigned int>::max();

/** String describing no compression. */
const std::string no_compression_str = "NO_COMPRESSION";

/** The array schema cache size. */
const uint64_t array_schema_cache_size = 10000000;

/** The fragment metadata cache size. */
const uint64_t fragment_metadata_cache_size = 10000000;

/** Whether or not the signal handlers are installed. */
const bool enable_signal_handlers = true;

/** The number of threads allocated per StorageManager for async queries. */
const uint64_t num_async_threads = 1;

/** The number of threads allocated per StorageManager for the Reader pool. */
const uint64_t num_reader_threads = 1;

/** The number of threads allocated per StorageManager for the Writer pool. */
const uint64_t num_writer_threads = 1;

/** The number of threads allocated for TBB. */
#ifdef HAVE_TBB
const int num_tbb_threads = tbb::task_scheduler_init::automatic;
#else
const int num_tbb_threads = -1;
#endif

/** The tile cache size. */
const uint64_t tile_cache_size = 10000000;

/** Empty String **/
const std::string empty_str = "";

/** TILEDB_READ Query String **/
const std::string query_type_read_str = "READ";

/** TILEDB_WRITE Query String **/
const std::string query_type_write_str = "WRITE";
/** TILEDB_FAILED Query String **/
const std::string query_status_failed_str = "FAILED";

/** TILEDB_COMPLETED Query String **/
const std::string query_status_completed_str = "COMPLETED";

/** TILEDB_INPROGRESS Query String **/
const std::string query_status_inprogress_str = "INPROGRESS";

/** TILEDB_INCOMPLETE Query String **/
const std::string query_status_incomplete_str = "INCOMPLETE";

/** TILEDB_UNINITIALIZED Query String **/
const std::string query_status_uninitialized_str = "UNINITIALIZED";

/** TILEDB_COMPRESSION Filter type string */
const std::string filter_type_compression_str = "COMPRESSION";

/** String describing no encryption. */
const std::string no_encryption_str = "NO_ENCRYPTION";

/** String describing AES_256_GCM. */
const std::string aes_256_gcm_str = "AES_256_GCM";

/** String describing GZIP. */
const std::string gzip_str = "GZIP";

/** String describing ZSTD. */
const std::string zstd_str = "ZSTD";

/** String describing LZ4. */
const std::string lz4_str = "LZ4";

/** String describing RLE. */
const std::string rle_str = "RLE";

/** String describing BZIP2. */
const std::string bzip2_str = "BZIP2";

/** String describing DOUBLE_DELTA. */
const std::string double_delta_str = "DOUBLE_DELTA";

/** The string representation for type int32. */
const std::string int32_str = "INT32";

/** The string representation for type int64. */
const std::string int64_str = "INT64";

/** The string representation for type float32. */
const std::string float32_str = "FLOAT32";

/** The string representation for type float64. */
const std::string float64_str = "FLOAT64";

/** The string representation for type char. */
const std::string char_str = "CHAR";

/** The string representation for type int8. */
const std::string int8_str = "INT8";

/** The string representation for type uint8. */
const std::string uint8_str = "UINT8";

/** The string representation for type int16. */
const std::string int16_str = "INT16";

/** The string representation for type uint16. */
const std::string uint16_str = "UINT16";

/** The string representation for type uint32. */
const std::string uint32_str = "UINT32";

/** The string representation for type uint64. */
const std::string uint64_str = "UINT64";

/** The string representation for type STRING_ASCII. */
const std::string string_ascii_str = "STRING_ASCII";

/** The string representation for type STRING_UTF8. */
const std::string string_utf8_str = "STRING_UTF8";

/** The string representation for type STRING_UTF16. */
const std::string string_utf16_str = "STRING_UTF16";

/** The string representation for type STRING_UTF32. */
const std::string string_utf32_str = "STRING_UTF32";

/** The string representation for type STRING_UCS2. */
const std::string string_ucs2_str = "STRING_UCS2";

/** The string representation for type STRING_UCS4. */
const std::string string_ucs4_str = "STRING_UCS4";

/** The string representation for type ANY. */
const std::string any_str = "ANY";

/** The string representation for the dense array type. */
const std::string dense_str = "dense";

/** The string representation for the sparse array type. */
const std::string sparse_str = "sparse";

/** The string representation for the column-major layout. */
const std::string col_major_str = "col-major";

/** The string representation for the row-major layout. */
const std::string row_major_str = "row-major";

/** The string representation for the global order layout. */
const std::string global_order_str = "global-order";

/** The string representation for the unordered layout. */
const std::string unordered_str = "unordered";

/** The string representation of null. */
const std::string null_str = "null";

/** The version in format { major, minor, revision }. */
const int32_t library_version[3] = {
    TILEDB_VERSION_MAJOR, TILEDB_VERSION_MINOR, TILEDB_VERSION_PATCH};

/** The TileDB serialization format version number. */
const uint32_t format_version = 2;

/** The maximum size of a tile chunk (unit of compression) in bytes. */
const uint64_t max_tile_chunk_size = 64 * 1024;

/** The default attribute name prefix. */
const std::string default_attr_name = "__attr";

/** The default dimension name prefix. */
const std::string default_dim_name = "__dim";

/** The key attribute name. */
const std::string key_attr_name = "__key";

/** The key attribute type. */
Datatype key_attr_type = Datatype::ANY;

/** The key attribute compressor. */
Compressor key_attr_compressor = Compressor::ZSTD;

/**
 * The name of the first key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
const std::string key_dim_1 = "__key_dim_1";

/**
 * The name of the second key dimension (recall that a key in a
 * key-value store is hashed into a 16-byte MD5 digest, which
 * is represented as a 2-dimensional uint64_t value.
 */
const std::string key_dim_2 = "__key_dim_2";

/** Maximum number of items to be buffered before a flush. */
uint64_t kv_max_items = 1000;

/** Maximum number of attempts to wait for an S3 response. */
const unsigned int s3_max_attempts = 1000;

/** Milliseconds of wait time between S3 attempts. */
const unsigned int s3_attempt_sleep_ms = 100;

/** An allocation tag used for logging. */
const std::string s3_allocation_tag = "TileDB";

/** Use virtual addressing (false for minio, true for AWS S3). */
const bool s3_use_virtual_addressing = true;

/** Connect timeout in milliseconds. */
const long s3_connect_timeout_ms = 3000;

/** Connect max tries. */
const long s3_connect_max_tries = 5;

/** Connect scale factor for exponential backoff. */
const long s3_connect_scale_factor = 25;

/** Request timeout in milliseconds. */
const long s3_request_timeout_ms = 3000;

/** S3 scheme (http for local minio, https for AWS S3). */
const std::string s3_scheme = "https";

/** S3 max parallel operations. */
const uint64_t s3_max_parallel_ops = vfs_num_threads;

/** Size of parts used in the S3 multi-part uploads. */
const uint64_t s3_multipart_part_size = 5 * 1024 * 1024;

/** S3 region. */
const std::string s3_region = "us-east-1";

/** S3 aws access key id. */
const std::string aws_access_key_id = "";

/** S3 aws secret access key. */
const std::string aws_secret_access_key = "";

/** S3 endpoint override. */
const std::string s3_endpoint_override = "";

/** S3 proxy scheme. */
const std::string s3_proxy_scheme = "https";

/** S3 proxy host. */
const std::string s3_proxy_host = "";

/** S3 proxy port. */
const unsigned s3_proxy_port = 0;

/** S3 proxy username. */
const std::string s3_proxy_username = "";

/** S3 proxy password. */
const std::string s3_proxy_password = "";

/** HDFS default kerb ticket cache path. */
const std::string hdfs_kerb_ticket_cache_path = "";

/** HDFS default name node uri. */
const std::string hdfs_name_node_uri = "";

/** HDFS default username. */
const std::string hdfs_username = "";

/** Prefix indicating a special name reserved by TileDB. */
const std::string special_name_prefix = "__";

/** Number of milliseconds between watchdog thread wakeups. */
const unsigned watchdog_thread_sleep_ms = 1000;

const void* fill_value(Datatype type) {
  switch (type) {
    case Datatype::INT8:
      return &constants::empty_int8;
    case Datatype::UINT8:
      return &constants::empty_uint8;
    case Datatype::INT16:
      return &constants::empty_int16;
    case Datatype::UINT16:
      return &constants::empty_uint16;
    case Datatype::INT32:
      return &constants::empty_int32;
    case Datatype::UINT32:
      return &constants::empty_uint32;
    case Datatype::INT64:
      return &constants::empty_int64;
    case Datatype::UINT64:
      return &constants::empty_uint64;
    case Datatype::FLOAT32:
      return &constants::empty_float32;
    case Datatype::FLOAT64:
      return &constants::empty_float64;
    case Datatype::CHAR:
      return &constants::empty_char;
    case Datatype::ANY:
      return &constants::empty_any;
    case Datatype::STRING_ASCII:
      return &constants::empty_ascii;
    case Datatype::STRING_UTF8:
      return &constants::empty_utf8;
    case Datatype::STRING_UTF16:
      return &constants::empty_utf16;
    case Datatype::STRING_UTF32:
      return &constants::empty_utf32;
    case Datatype::STRING_UCS2:
      return &constants::empty_ucs2;
    case Datatype::STRING_UCS4:
      return &constants::empty_ucs4;
  }

  return nullptr;
}

}  // namespace constants

}  // namespace sm
}  // namespace tiledb
