/**
 * @file   constants.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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

#include <array>
#include <cinttypes>
#include <string>

namespace tiledb {
namespace sm {

enum class Datatype : uint8_t;
enum class Compressor : uint8_t;
enum class SerializationType : uint8_t;

namespace constants {

/**
 * Reduction factor (must be in [0.0, 1.0]) for the multi_range subarray
 * split by the partitioner. If the number is equal to 0.3, then this
 * means that the number of ranges will be reduced by 30%.
 */
extern const double multi_range_reduction_in_split;

/** Amplification factor for the result size estimation. */
extern const double est_result_size_amplification;

/** Default fanout for RTrees. */
extern const unsigned rtree_fanout;

/** The array schema file name. */
extern const std::string array_schema_filename;

/** The array schema directory name. */
extern const std::string array_schema_dir_name;

/** The array metadata directory name. */
extern const std::string array_metadata_dir_name;

/** The array fragment metadata directory name. */
extern const std::string array_fragment_meta_dir_name;

/** The array fragments directory name. */
extern const std::string array_fragments_dir_name;

/** The array commit directory name. */
extern const std::string array_commits_dir_name;

/** The array dimension labels directory name. */
extern const std::string array_dimension_labels_dir_name;

/** The default tile capacity. */
extern const uint64_t capacity;

/** The size of a variable offset cell. */
extern const uint64_t cell_var_offset_size;

/** The type of a variable offset cell. */
extern const Datatype cell_var_offset_type;

/** The size of a validity cell. */
extern const uint64_t cell_validity_size;

/** The type of a validity cell. */
extern const Datatype cell_validity_type;

/** A special value indicating varibale size. */
extern const uint64_t var_size;

/** The default compressor for the offsets of variable-sized cells. */
extern Compressor cell_var_offsets_compression;

/** The default compression level for the offsets of variable-sized cells. */
extern int cell_var_offsets_compression_level;

/** The default compressor for the validity value cells. */
extern Compressor cell_validity_compression;

/** The default compression level for the validity value cells. */
extern int cell_validity_compression_level;

/** The default compressor for the coordinates. */
extern Compressor coords_compression;

/** The default compressor for real coordinates. */
extern Compressor real_coords_compression;

/** The default compression level for the coordinates. */
extern int coords_compression_level;

/** Special name reserved for the coordinates attribute. */
extern const std::string coords;

/** Special name reserved for the timestamp attribute. */
extern const std::string timestamps;

/** Special name reserved for the delete timestamp attribute. */
extern const std::string delete_timestamps;

/** Special name reserved for the delete condition index attribute. */
extern const std::string delete_condition_index;

/** The size of a timestamp cell. */
extern const uint64_t timestamp_size;

/** The type of a timestamp cell. */
extern const Datatype timestamp_type;

/** The type of a delete condition index cell. */
extern const Datatype delete_condition_index_type;

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

/** The special value for an empty bool. */
extern const uint8_t empty_bool;

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

/** Suffix for the special vacuum files used in TileDB. */
extern const std::string vacuum_file_suffix;

/** Suffix for the special ok files used in TileDB. */
extern const std::string ok_file_suffix;

/** Suffix for the special write files used in TileDB. */
extern const std::string write_file_suffix;

/** Suffix for the special delete files used in TileDB. */
extern const std::string delete_file_suffix;

/** Suffix for the special update files used in TileDB. */
extern const std::string update_file_suffix;

/** Suffix for the special metadata files used in TileDB. */
extern const std::string meta_file_suffix;

/** Suffix for the special consolidated commits files used in TileDB. */
extern const std::string con_commits_file_suffix;

/** Suffix for the special ignore files used in TileDB. */
extern const std::string ignore_file_suffix;

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

/** The group details directory name. */
extern const std::string group_detail_dir_name;

/** The group metadata directory name. */
extern const std::string group_metadata_dir_name;

/** The maximum number of bytes written in a single I/O. */
extern const uint64_t max_write_bytes;

/** The maximum file path length (depending on platform). */
extern const uint32_t path_max_len;

/** Special value indicating a variable number of elements. */
extern const uint32_t var_num;

/** String describing no compression. */
extern const std::string no_compression_str;

/** Empty String reference **/
extern const std::string empty_str;

/** TILEDB_READ Query String **/
extern const std::string query_type_read_str;

/** TILEDB_WRITE Query String **/
extern const std::string query_type_write_str;

/** TILEDB_DELETE Query String **/
extern const std::string query_type_delete_str;

/** TILEDB_UPDATE Query String **/
extern const std::string query_type_update_str;

/** TILEDB_MODIFY_EXCLUSIVE Query String **/
extern const std::string query_type_modify_exclusive_str;

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

/** TILEDB_LT Query Condition Op String **/
extern const std::string query_condition_op_lt_str;

/** TILEDB_LE Query Condition Op String **/
extern const std::string query_condition_op_le_str;

/** TILEDB_GT Query Condition Op String **/
extern const std::string query_condition_op_gt_str;

/** TILEDB_GE Query Condition Op String **/
extern const std::string query_condition_op_ge_str;

/** TILEDB_EQ Query Condition Op String **/
extern const std::string query_condition_op_eq_str;

/** TILEDB_NE Query Condition Op String **/
extern const std::string query_condition_op_ne_str;

/** TILEDB_AND Query Condition Combination Op String **/
extern const std::string query_condition_combination_op_and_str;

/** TILEDB_OR Query Condition Combination Op String **/
extern const std::string query_condition_combination_op_or_str;

/** TILEDB_NOT Query Condition Combination Op String **/
extern const std::string query_condition_combination_op_not_str;

/** TILEDB_COMPRESSION Filter type string */
extern const std::string filter_type_compression_str;

/** String describing no encryption. */
extern const std::string no_encryption_str;

/** String describing AES_256_GCM. */
extern const std::string aes_256_gcm_str;

/** TILEDB_JSON **/
extern const std::string serialization_type_json_str;

/** TILEDB_CAPNP **/
extern const std::string serialization_type_capnp_str;

/** String describing GZIP. */
extern const std::string gzip_str;

/** String describing ZSTD. */
extern const std::string zstd_str;

/** String describing LZ4. */
extern const std::string lz4_str;

/** String describing RLE. */
extern const std::string rle_str;

/** String describing BZIP2. */
extern const std::string bzip2_str;

/** String describing DOUBLE_DELTA. */
extern const std::string double_delta_str;

/** String describing FILTER_NONE. */
extern const std::string filter_none_str;

/** String describing FILTER_BIT_WIDTH_REDUCTION. */
extern const std::string filter_bit_width_reduction_str;

/** String describing FILTER_BITSHUFFLE. */
extern const std::string filter_bitshuffle_str;

/** String describing FILTER_BYTESHUFFLE. */
extern const std::string filter_byteshuffle_str;

/** String describing FILTER_POSITIVE_DELTA. */
extern const std::string filter_positive_delta_str;

/** String describing FILTER_CHECKSUM_MD5. */
extern const std::string filter_checksum_md5_str;

/** String describing FILTER_CHECKSUM_SHA256. */
extern const std::string filter_checksum_sha256_str;

/** String describing FILTER_DICTIONARY. */
extern const std::string filter_dictionary_str;

/** String describing FILTER_SCALE_FLOAT. */
extern const std::string filter_scale_float_str;

/** String describing FILTER_XOR. */
extern const std::string filter_xor_str;

/** The string representation for FilterOption type compression_level. */
extern const std::string filter_option_compression_level_str;

/** The string representation for FilterOption type bit_width_max_window. */
extern const std::string filter_option_bit_width_max_window_str;

/** The string representation for FilterOption type positive_delta_max_window.
 */
extern const std::string filter_option_positive_delta_max_window_str;

/** The string representation for FilterOption type scale_float_bytewidth. */
extern const std::string filter_option_scale_float_bytewidth;

/** The string representation for FilterOption type scale_float_factor. */
extern const std::string filter_option_scale_float_factor;

/** The string representation for FilterOption type scale_float_offset. */
extern const std::string filter_option_scale_float_offset;

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

/** The string representation for type blob. */
extern const std::string blob_str;

/** The string representation for type bool. */
extern const std::string bool_str;

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

/** The string representation for type DATETIME_YEAR. */
extern const std::string datetime_year_str;

/** The string representation for type DATETIME_MONTH. */
extern const std::string datetime_month_str;

/** The string representation for type DATETIME_WEEK. */
extern const std::string datetime_week_str;

/** The string representation for type DATETIME_DAY. */
extern const std::string datetime_day_str;

/** The string representation for type DATETIME_HR. */
extern const std::string datetime_hr_str;

/** The string representation for type DATETIME_MIN. */
extern const std::string datetime_min_str;

/** The string representation for type DATETIME_SEC. */
extern const std::string datetime_sec_str;

/** The string representation for type DATETIME_MS. */
extern const std::string datetime_ms_str;

/** The string representation for type DATETIME_US. */
extern const std::string datetime_us_str;

/** The string representation for type DATETIME_NS. */
extern const std::string datetime_ns_str;

/** The string representation for type DATETIME_PS. */
extern const std::string datetime_ps_str;

/** The string representation for type DATETIME_FS. */
extern const std::string datetime_fs_str;

/** The string representation for type DATETIME_AS. */
extern const std::string datetime_as_str;

/** The string representation for type TIME_HR. */
extern const std::string time_hr_str;

/** The string representation for type TIME_MIN. */
extern const std::string time_min_str;

/** The string representation for type TIME_SEC. */
extern const std::string time_sec_str;

/** The string representation for type TIME_MS. */
extern const std::string time_ms_str;

/** The string representation for type TIME_US. */
extern const std::string time_us_str;

/** The string representation for type TIME_NS. */
extern const std::string time_ns_str;

/** The string representation for type TIME_PS. */
extern const std::string time_ps_str;

/** The string representation for type TIME_FS. */
extern const std::string time_fs_str;

/** The string representation for type TIME_AS. */
extern const std::string time_as_str;

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

/** The string representation for the Hilbert layout. */
extern const std::string hilbert_str;

/** The string representation of null. */
extern const std::string null_str;

/** The string representation of unordered label. */
extern const std::string label_unordered_str;

/** The string representation of increasing order label. */
extern const std::string label_increasing_str;

/** The string representation of decreasing order label. */
extern const std::string label_decreasing_str;

/** The string representation for object type invalid. */
extern const std::string object_type_invalid_str;

/** The string representation for object type group. */
extern const std::string object_type_group_str;

/** The string representation for object type array. */
extern const std::string object_type_array_str;

/** The string representation for filesystem type hdfs. */
extern const std::string filesystem_type_hdfs_str;

/** The string representation for filesystem type s3. */
extern const std::string filesystem_type_s3_str;

/** The string representation for filesystem type azure. */
extern const std::string filesystem_type_azure_str;

/** The string representation for filesystem type gcs. */
extern const std::string filesystem_type_gcs_str;

/** The string representation for in-memory filesystem. */
extern const std::string filesystem_type_mem_str;

/** The string representation for WalkOrder preorder. */
extern const std::string walkorder_preorder_str;

/** The string representation for WalkOrder postorder. */
extern const std::string walkorder_postorder_str;

/** The string representation for VFSMode read. */
extern const std::string vfsmode_read_str;

/** The string representation for VFSMode write. */
extern const std::string vfsmode_write_str;

/** The string representation for VFSMode append. */
extern const std::string vfsmode_append_str;

/** The TileDB library version in format { major, minor, revision }. */
extern const int32_t library_version[3];

/** The TileDB serialization base format version number. */
extern const uint32_t base_format_version;

/** The TileDB serialization format version number. */
extern const uint32_t format_version;

/** The lowest version supported for back compat writes. */
extern const uint32_t back_compat_writes_min_format_version;

/** The lowest version supported for consolidation with timestamps. */
extern const uint32_t consolidation_with_timestamps_min_version;

/** The lowest version supported for deletes. */
extern const uint32_t deletes_min_version;

/** The lowest version supported for updates. */
extern const uint32_t updates_min_version;

/** The maximum size of a tile chunk (unit of compression) in bytes. */
extern const uint64_t max_tile_chunk_size;

/**
 * Maximum splitting depth on a string dimension. For a maximum depth
 * of D, the maximum number of splits is 2^D.
 */
extern const uint64_t max_string_dim_split_depth;

/** Maximum number of attempts to wait for an S3 response. */
extern const unsigned int s3_max_attempts;

/** Milliseconds of wait time between S3 attempts. */
extern const unsigned int s3_attempt_sleep_ms;

/** Maximum number of attempts to wait for an Azure response. */
extern const unsigned int azure_max_attempts;

/**
 * Milliseconds of wait time between Azure attempts. Currently,
 * the Azure SDK only supports retries with a second-granularity.
 * This number will be floored to the nearest second.
 */
extern const unsigned int azure_attempt_sleep_ms;

/** Maximum number of attempts to wait for an GCS response. */
extern const unsigned int gcs_max_attempts;

/** Milliseconds of wait time between GCS attempts. */
extern const unsigned int gcs_attempt_sleep_ms;

/** An allocation tag used for logging. */
extern const std::string s3_allocation_tag;

/** Prefix indicating a special name reserved by TileDB. */
extern const std::string special_name_prefix;

/** Number of milliseconds between watchdog thread wakeups. */
extern const unsigned watchdog_thread_sleep_ms;

/** Maximum number of concurrent attribute reads. */
extern const unsigned concurrent_attr_reads;

/** Returns the empty fill value based on the input datatype. */
const void* fill_value(Datatype type);

/** The redirection header key in REST response. */
extern const std::string redirection_header_key;

#ifdef __linux__
/** List of possible certificates files for libcurl */
extern const std::array<std::string, 6> cert_files_linux;
#endif

/** Delimiter for lists passed as config parameter */
extern const std::string config_delimiter;

/** String describing MIME_AUTODETECT. */
extern const std::string mime_autodetect_str;

/** String describing MIME_TIFF. */
extern const std::string mime_tiff_str;

/** String describing MIME_PDF. */
extern const std::string mime_pdf_str;

/** The default tile extent used in filestore arrays. */
extern const uint64_t filestore_default_tile_extent;

/** Name of the single dimension used in filestore arrays. */
extern const std::string filestore_dimension_name;

/** Name of the single attribute used in filestore arrays. */
extern const std::string filestore_attribute_name;

/** Name of the metadata key used in filestore arrays for current size. */
extern const std::string filestore_metadata_size_key;

/** Name of the metadata key used in filestore arrays for mime type. */
extern const std::string filestore_metadata_mime_type_key;

/** Name of the metadata key used in filestore arrays for mime encoding. */
extern const std::string filestore_metadata_mime_encoding_key;

/** Name of the metadata key used in filestore arrays for original filename. */
extern const std::string filestore_metadata_original_filename_key;

/** Name of the metadata key used in filestore arrays for filename extension. */
extern const std::string filestore_metadata_file_extension_key;

}  // namespace constants

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONSTANTS_H
