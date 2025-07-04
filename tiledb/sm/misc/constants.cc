/**
 * @file   constants.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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

#include "tiledb/common/common.h"
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
#include "tiledb/sm/enums/serialization_type.h"

namespace tiledb::sm::constants {

/**
 * Reduction factor (must be in [0.0, 1.0]) for the multi_range subarray
 * split by the partitioner. If the number is equal to 0.3, then this
 * means that the number of ranges will be reduced by 30%.
 */
const double multi_range_reduction_in_split = 0.3;

/** Amplification factor for the result size estimation. */
const double est_result_size_amplification = 1.0;

/** Default fanout for RTrees. */
const unsigned rtree_fanout = 10;

/** The array schema file name. */
const std::string array_schema_filename = "__array_schema.tdb";

/** The array schema directory name. */
const std::string array_schema_dir_name = "__schema";

/** The array metadata directory name. */
const std::string array_metadata_dir_name = "__meta";

/** The array fragment metadata directory name. */
const std::string array_fragment_meta_dir_name = "__fragment_meta";

/** The array fragments directory name. */
const std::string array_fragments_dir_name = "__fragments";

/** The array commit directory name. */
const std::string array_commits_dir_name = "__commits";

/** The fragment metadata file name. */
const std::string fragment_metadata_filename = "__fragment_metadata.tdb";

/** The array dimension labels directory name. */
const std::string array_dimension_labels_dir_name = "__labels";

/** The array enumerations directory name. */
const std::string array_enumerations_dir_name = "__enumerations";

/** The array directory names. */
const std::vector<std::string> array_dir_names = {
    array_schema_dir_name,
    array_metadata_dir_name,
    array_fragment_meta_dir_name,
    array_fragments_dir_name,
    array_commits_dir_name,
    array_dimension_labels_dir_name,
    array_enumerations_dir_name};

/** The default tile capacity. */
const uint64_t capacity = 10000;

/** The size of a variable cell offset. */
const uint64_t cell_var_offset_size = sizeof(uint64_t);

/** The type of a variable cell offset. */
const Datatype cell_var_offset_type = Datatype::UINT64;

/** The size of a validity cell. */
const uint64_t cell_validity_size = sizeof(uint8_t);

/** The type of a validity cell. */
const Datatype cell_validity_type = Datatype::UINT8;

/** A special value indicating variable size. */
const uint64_t var_size = std::numeric_limits<uint64_t>::max();

/** The default compressor for the offsets of variable-sized cells. */
Compressor cell_var_offsets_compression = Compressor::ZSTD;

/** The default compression level for the offsets of variable-sized cells. */
int cell_var_offsets_compression_level = -1;

/** The default compressor for the validity value cells. */
Compressor cell_validity_compression = Compressor::RLE;

/** The default compression level for the validity value cells. */
int cell_validity_compression_level = -1;

/** Special name reserved for the coordinates attribute. */
const std::string coords = "__coords";

/** Special name reserved for the timestamp attribute. */
const std::string timestamps = "__timestamps";

/** Special name reserved for the delete timestamp attribute. */
const std::string delete_timestamps = "__delete_timestamps";

/** Special name reserved for the delete condition index attribute. */
const std::string delete_condition_index = "__delete_condition_index";

/** Special name reserved for count of rows. */
const std::string count_of_rows = "__count_of_rows";

/** The size of a timestamp cell. */
const uint64_t timestamp_size = sizeof(uint64_t);

/** The type of a timestamp cell. */
extern const Datatype timestamp_type = Datatype::UINT64;

/** The type of a delete condition index cell. */
extern const Datatype delete_condition_index_type = Datatype::UINT64;

/** The default compressor for the coordinates. */
Compressor coords_compression = Compressor::ZSTD;

/** The default compressor for real coordinates. */
Compressor real_coords_compression = Compressor::ZSTD;

/** The default compression level for the coordinates. */
int coords_compression_level = -1;

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

/** The special value for an empty blob. */
constexpr std::byte empty_blob{0};

/** The special value for an empty geom_wkb. */
constexpr std::byte empty_geom_wkb{0};

/** The special value for an empty geom_wkt. */
constexpr std::byte empty_geom_wkt{0};

/** The special value for an empty bool. */
const uint8_t empty_bool = 0;

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

/** The return value for missing entries in an Enumeration */
const uint64_t enumeration_missing_value = std::numeric_limits<uint64_t>::max();

/** The file suffix used in TileDB. */
const std::string file_suffix = ".tdb";

/** Suffix for the special vacuum files used in TileDB. */
const std::string vacuum_file_suffix = ".vac";

/** Suffix for the special ok files used in TileDB. */
const std::string ok_file_suffix = ".ok";

/** Suffix for the special write files used in TileDB. */
const std::string write_file_suffix = ".wrt";

/** Suffix for the special delete files used in TileDB. */
const std::string delete_file_suffix = ".del";

/** Suffix for the special update files used in TileDB. */
const std::string update_file_suffix = ".upd";

/** Suffix for the special metadata files used in TileDB. */
const std::string meta_file_suffix = ".meta";

/** Suffix for the special commits files used in TileDB. */
const std::string con_commits_file_suffix = ".con";

/** Suffix for the special ignore files used in TileDB. */
const std::string ignore_file_suffix = ".ign";

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

/** The group details directory name. */
const std::string group_detail_dir_name = "__group";

/** The group metadata directory name. */
const std::string group_metadata_dir_name = "__meta";

/** The group directory names. */
const std::vector<std::string> group_dir_names = {
    group_detail_dir_name, group_metadata_dir_name};

/** The maximum number of bytes written in a single I/O. */
const uint64_t max_write_bytes = std::numeric_limits<int>::max();

/** The maximum file path length (depending on platform). */
#ifndef _WIN32
const uint32_t path_max_len = PATH_MAX;
#endif

/** Special value indicating a variable number of elements. */
const uint32_t var_num = cell_val_num_var;

/** String describing no compression. */
const std::string no_compression_str = "NO_COMPRESSION";

/** Empty String **/
const std::string empty_str = "";

/** TILEDB_READ Query String **/
const std::string query_type_read_str = "READ";

/** TILEDB_WRITE Query String **/
const std::string query_type_write_str = "WRITE";

/** TILEDB_DELETE Query String **/
const std::string query_type_delete_str = "DELETE";

/** TILEDB_UPDATE Query String **/
const std::string query_type_update_str = "UPDATE";

/** TILEDB_MODIFY_EXCLUSIVE Query String **/
const std::string query_type_modify_exclusive_str = "MODIFY_EXCLUSIVE";

/** TILEDB_FAILED Query String **/
const std::string query_status_failed_str = "FAILED";

/** TILEDB_COMPLETED Query String **/
const std::string query_status_completed_str = "COMPLETED";

/** TILEDB_INPROGRESS Query String **/
const std::string query_status_inprogress_str = "INPROGRESS";

/** TILEDB_INCOMPLETE Query String **/
const std::string query_status_incomplete_str = "INCOMPLETE";

/** TILEDB_INITIALIZED Query String **/
const std::string query_status_initialized_str = "INITIALIZED";

/** TILEDB_UNINITIALIZED Query String **/
const std::string query_status_uninitialized_str = "UNINITIALIZED";

/** TILEDB_ALWAYS_TRUE Query Condition Op String **/
const std::string query_condition_op_always_true_str = "ALWAYS_TRUE";

/** TILEDB_ALWAYS_FALSE Query Condition Op String **/
const std::string query_condition_op_always_false_str = "ALWAYS_FALSE";

/** TILEDB_LT Query Condition Op String **/
const std::string query_condition_op_lt_str = "LT";

/** TILEDB_LE Query Condition Op String **/
const std::string query_condition_op_le_str = "LE";

/** TILEDB_GT Query Condition Op String **/
const std::string query_condition_op_gt_str = "GT";

/** TILEDB_GE Query Condition Op String **/
const std::string query_condition_op_ge_str = "GE";

/** TILEDB_EQ Query Condition Op String **/
const std::string query_condition_op_eq_str = "EQ";

/** TILEDB_NE Query Condition Op String **/
const std::string query_condition_op_ne_str = "NE";

/** TILEDB_IN Query Condition Op String **/
const std::string query_condition_op_in_str = "IN";

/** TILEDB_NIN Query Condition Op String **/
const std::string query_condition_op_not_in_str = "NOT_IN";

/** TILEDB_AND Query Condition Combination Op String **/
const std::string query_condition_combination_op_and_str = "AND";

/** TILEDB_OR Query Condition Combination Op String **/
const std::string query_condition_combination_op_or_str = "OR";

/** TILEDB_NOT Query Condition Combination Op String **/
const std::string query_condition_combination_op_not_str = "NOT";

/** TILEDB_COMPRESSION Filter type string */
const std::string filter_type_compression_str = "COMPRESSION";

/** String describing no encryption. */
const std::string no_encryption_str = "NO_ENCRYPTION";

/** String describing AES_256_GCM. */
const std::string aes_256_gcm_str = "AES_256_GCM";

/** TILEDB_JSON **/
const std::string serialization_type_json_str = "JSON";

/** TILEDB_CAPNP **/
const std::string serialization_type_capnp_str = "CAPNP";

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

/** String describing DELTA. */
const std::string delta_str = "DELTA";

/** String describing FILTER_NONE. */
const std::string filter_none_str = "NONE";

/** String describing FILTER_BIT_WIDTH_REDUCTION. */
const std::string filter_bit_width_reduction_str = "BIT_WIDTH_REDUCTION";

/** String describing FILTER_BITSHUFFLE. */
const std::string filter_bitshuffle_str = "BITSHUFFLE";

/** String describing FILTER_BYTESHUFFLE. */
const std::string filter_byteshuffle_str = "BYTESHUFFLE";

/** String describing FILTER_POSITIVE_DELTA. */
const std::string filter_positive_delta_str = "POSITIVE_DELTA";

/** String describing FILTER_CHECKSUM_MD5. */
const std::string filter_checksum_md5_str = "CHECKSUM_MD5";

/** String describing FILTER_CHECKSUM_SHA256. */
const std::string filter_checksum_sha256_str = "CHECKSUM_SHA256";

/** String describing FILTER_DICTIONARY. */
const std::string filter_dictionary_str = "DICTIONARY_ENCODING";

/** String describing FILTER_SCALE_FLOAT. */
const std::string filter_scale_float_str = "SCALE_FLOAT";

/** String describing FILTER_XOR. */
const std::string filter_xor_str = "XOR";

/** String describing FILTER_WEBP. */
const std::string filter_webp_str = "WEBP";

/** The string representation for FilterOption type compression_level. */
const std::string filter_option_compression_level_str = "COMPRESSION_LEVEL";

/** The string representation for FilterOption type bit_width_max_window. */
const std::string filter_option_bit_width_max_window_str =
    "BIT_WIDTH_MAX_WINDOW";

/** The string representation for FilterOption type positives_delta_max_window.
 */
const std::string filter_option_positive_delta_max_window_str =
    "POSITIVE_DELTA_MAX_WINDOW";

/** The string representation for FilterOption type scale_float_bytewidth. */
const std::string filter_option_scale_float_bytewidth = "SCALE_FLOAT_BYTEWIDTH";

/** The string representation for FilterOption type scale_float_factor. */
const std::string filter_option_scale_float_factor = "SCALE_FLOAT_FACTOR";

/** The string representation for FilterOption type scale_float_offset. */
const std::string filter_option_scale_float_offset = "SCALE_FLOAT_OFFSET";

/** The string representation for FilterOption type webp_quality. */
const std::string filter_option_webp_quality = "WEBP_QUALITY";

/** The string representation for FilterOption type webp_input_format. */
const std::string filter_option_webp_input_format = "WEBP_INPUT_FORMAT";

/** The string representation for FilterOption type webp_lossless. */
const std::string filter_option_webp_lossless = "WEBP_LOSSLESS";

/**
 * The string representation for FilterOption type
 * compression_reinterpret_datatype.
 */
const std::string filter_option_compression_reinterpret_datatype =
    "COMPRESSION_REINTERPRET_DATATYPE";

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

/** The string representation for type blob. */
const std::string blob_str = "BLOB";

/** The string representation for type geom_wkb. */
const std::string geom_wkb_str = "GEOM_WKB";

/** The string representation for type geom_wkt. */
const std::string geom_wkt_str = "GEOM_WKT";

/** The string representation for type bool. */
const std::string bool_str = "BOOL";

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

/** The string representation for type DATETIME_YEAR. */
const std::string datetime_year_str = "DATETIME_YEAR";

/** The string representation for type DATETIME_MONTH. */
const std::string datetime_month_str = "DATETIME_MONTH";

/** The string representation for type DATETIME_WEEK. */
const std::string datetime_week_str = "DATETIME_WEEK";

/** The string representation for type DATETIME_DAY. */
const std::string datetime_day_str = "DATETIME_DAY";

/** The string representation for type DATETIME_HR. */
const std::string datetime_hr_str = "DATETIME_HR";

/** The string representation for type DATETIME_MIN. */
const std::string datetime_min_str = "DATETIME_MIN";

/** The string representation for type DATETIME_SEC. */
const std::string datetime_sec_str = "DATETIME_SEC";

/** The string representation for type DATETIME_MS. */
const std::string datetime_ms_str = "DATETIME_MS";

/** The string representation for type DATETIME_US. */
const std::string datetime_us_str = "DATETIME_US";

/** The string representation for type DATETIME_NS. */
const std::string datetime_ns_str = "DATETIME_NS";

/** The string representation for type DATETIME_PS. */
const std::string datetime_ps_str = "DATETIME_PS";

/** The string representation for type DATETIME_FS. */
const std::string datetime_fs_str = "DATETIME_FS";

/** The string representation for type DATETIME_AS. */
const std::string datetime_as_str = "DATETIME_AS";

/** The string representation for type TIME_HR. */
const std::string time_hr_str = "TIME_HR";

/** The string representation for type TIME_MIN. */
const std::string time_min_str = "TIME_MIN";

/** The string representation for type TIME_SEC. */
const std::string time_sec_str = "TIME_SEC";

/** The string representation for type TIME_MS. */
const std::string time_ms_str = "TIME_MS";

/** The string representation for type TIME_US. */
const std::string time_us_str = "TIME_US";

/** The string representation for type TIME_NS. */
const std::string time_ns_str = "TIME_NS";

/** The string representation for type TIME_PS. */
const std::string time_ps_str = "TIME_PS";

/** The string representation for type TIME_FS. */
const std::string time_fs_str = "TIME_FS";

/** The string representation for type TIME_AS. */
const std::string time_as_str = "TIME_AS";

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

/** The string representation for the Hilbert layout. */
const std::string hilbert_str = "hilbert";

/** The string representation of null. */
const std::string null_str = "null";

/** The string representation of unordered data. */
const std::string data_unordered_str = "unordered";

/** The string representation of increasing order data. */
const std::string data_increasing_str = "increasing";

/** The string representation of decreasing order data. */
const std::string data_decreasing_str = "decreasing";

/** The string representation for object type invalid. */
const std::string object_type_invalid_str = "INVALID";

/** The string representation for object type group. */
const std::string object_type_group_str = "GROUP";

/** The string representation for object type array. */
const std::string object_type_array_str = "ARRAY";

/** The string representation for filesystem type hdfs. */
const std::string filesystem_type_hdfs_str = "HDFS";

/** The string representation for filesystem type s3. */
const std::string filesystem_type_s3_str = "S3";

/** The string representation for filesystem type Azure. */
const std::string filesystem_type_azure_str = "AZURE";

/** The string representation for filesystem type GCS. */
const std::string filesystem_type_gcs_str = "GCS";

/** The string representation for in-memory filesystem */
const std::string filesystem_type_mem_str = "MEM";

/** The string representation for WalkOrder preorder. */
const std::string walkorder_preorder_str = "PREORDER";

/** The string representation for WalkOrder postorder. */
const std::string walkorder_postorder_str = "POSTORDER";

/** The string representation for VFSMode read. */
const std::string vfsmode_read_str = "VFS_READ";

/** The string representation for VFSMode write. */
const std::string vfsmode_write_str = "VFS_WRITE";

/** The string representation for VFSMode append. */
const std::string vfsmode_append_str = "VFS_APPEND";

/** The version in format { major, minor, revision }. */
const int32_t library_version[3] = {
    TILEDB_VERSION_MAJOR, TILEDB_VERSION_MINOR, TILEDB_VERSION_PATCH};

/** The TileDB serialization base format version number. */
const format_version_t format_version = 22;

/** The lowest version supported for back compat writes. */
const format_version_t back_compat_writes_min_format_version = 7;

/** The lowest version supported for consolidation with timestamps. */
const format_version_t consolidation_with_timestamps_min_version = 15;

/** The lowest version supported for deletes. */
const format_version_t deletes_min_version = 16;

/** The lowest version supported for updates. */
const format_version_t updates_min_version = 16;

/** The lowest version supported for tile min/max/sum/null count data. */
const format_version_t tile_metadata_min_version = 11;

/** The lowest version supported format version for enumerations. */
const format_version_t enumerations_min_format_version = 20;

/** The current enumerations version. */
const format_version_t enumerations_version = 0;

/** The lowest version supported format version for CurrentDomain API. */
const format_version_t current_domain_min_format_version = 22;

/** The current CurrentDomain API version. */
const format_version_t current_domain_version = 0;

/** The NDRectangle current domain */
const std::string current_domain_ndrectangle_str = "NDRECTANGLE";

/** The maximum size of a tile chunk (unit of compression) in bytes. */
const uint64_t max_tile_chunk_size = 64 * 1024;

/**
 * Maximum splitting depth on a string dimension. For a maximum depth
 * of D, the maximum number of splits is 2^D.
 */
const uint64_t max_string_dim_split_depth = 7;

/** Maximum number of attempts to wait for an S3 response. */
const unsigned int s3_max_attempts = 100;

/** Milliseconds of wait time between S3 attempts. */
const unsigned int s3_attempt_sleep_ms = 100;

/** Maximum number of attempts to wait for a GCS response. */
const unsigned int gcs_max_attempts = 100;

/** Milliseconds of wait time between GCS attempts. */
const unsigned int gcs_attempt_sleep_ms = 1000;

/** An allocation tag used for logging. */
const std::string s3_allocation_tag = "TileDB";

/** The config key prefix for S3 custom headers. */
const std::string s3_header_prefix = "vfs.s3.custom_headers.";

/** Prefix indicating a special name reserved by TileDB. */
const std::string special_name_prefix = "__";

/** Number of milliseconds between watchdog thread wakeups. */
const unsigned watchdog_thread_sleep_ms = 1000;

/** Maximum number of concurrent attribute reads. */
const unsigned concurrent_attr_reads = 2;

/** The redirection header key in REST response. */
extern const std::string redirection_header_key = "location";

/** The config key prefix for REST custom headers. */
const std::string rest_header_prefix = "rest.custom_headers.";

/** The current RestProfile API version. */
const format_version_t rest_profile_version = 1;

/**
 * Foldername that the REST profiles are stored
 * in when using the home directory.
 */
const std::string rest_profile_foldername = ".tiledb";

/** The filename of the REST profiles file. */
const std::string rest_profile_filename = "profiles.json";

/** String describing MIME_AUTODETECT. */
const std::string mime_autodetect_str = "AUTODETECT";

/** String describing MIME_TIFF. */
const std::string mime_tiff_str = "image/tiff";

/** String describing MIME_PDF. */
const std::string mime_pdf_str = "application/pdf";

/** The default tile extent used in filestore arrays. */
const uint64_t filestore_default_tile_extent = 1024;

/** Name of the single dimension used in filestore arrays. */
const std::string filestore_dimension_name = "position";

/** Name of the single attribute used in filestore arrays. */
const std::string filestore_attribute_name = "contents";

/** Name of the metadata key used in filestore arrays for current size. */
const std::string filestore_metadata_size_key = "file_size";

/** Name of the metadata key used in filestore arrays for mime type. */
const std::string filestore_metadata_mime_type_key = "mime_type";

/** Name of the metadata key used in filestore arrays for mime encoding. */
const std::string filestore_metadata_mime_encoding_key = "mime_encoding";

/** Name of the metadata key used in filestore arrays for original filename. */
const std::string filestore_metadata_original_filename_key =
    "original_file_name";

/** Name of the metadata key used in filestore arrays for filename extension. */
const std::string filestore_metadata_file_extension_key = "file_extension";

/** The minimum size of an s3 multipart upload intermediate part. */
const uint64_t s3_min_multipart_part_size = 5242880;

/**
 * The dir name under fragment uri used by s3 to store remote
 * global order writes intermediate chunks
 */
const std::string s3_multipart_buffering_dirname =
    "__global_order_write_chunks";

const void* fill_value(Datatype type) {
  switch (type) {
    case Datatype::BLOB:
      return &constants::empty_blob;
    case Datatype::GEOM_WKB:
      return &constants::empty_geom_wkb;
    case Datatype::GEOM_WKT:
      return &constants::empty_geom_wkt;
    case Datatype::BOOL:
      return &constants::empty_bool;
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
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return &constants::empty_int64;
  }

  return nullptr;
}

const std::string config_delimiter = ",";

}  // namespace tiledb::sm::constants
