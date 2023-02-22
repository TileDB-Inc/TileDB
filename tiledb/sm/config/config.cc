/**
 * @file   config.cc
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
 * This file implements class Config.
 */

#include "config.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/parse_argument.h"

#include <fstream>
#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace {
bool ignore_default_via_env(const std::string& param) {
  if (param == "vfs.s3.region") {
    // We should not use the default value for `vfs.s3.region` if the user
    // has set either AWS_REGION or AWS_DEFAULT_REGION in their environment.
    // We defer to the SDK to interpret these values.

    if ((std::getenv("AWS_REGION") != nullptr) ||
        (std::getenv("AWS_DEFAULT_REGION") != nullptr)) {
      return true;
    }
  }
  return false;
}
}  // namespace

namespace tiledb::sm {

/** Return a Config error class Status with a given message **/
inline Status Status_ConfigError(const std::string& msg) {
  return {"[TileDB::Config] Error", msg};
}

void throw_config_exception(const std::string& msg) {
  throw StatusException(Status_ConfigError(msg));
}

/* ****************************** */
/*        CONFIG DEFAULTS         */
/* ****************************** */

const std::string Config::CONFIG_ENVIRONMENT_VARIABLE_PREFIX = "TILEDB_";
#ifdef TILEDB_VERBOSE
const std::string Config::CONFIG_LOGGING_LEVEL = "1";
#else
const std::string Config::CONFIG_LOGGING_LEVEL = "0";
#endif
const std::string Config::CONFIG_LOGGING_DEFAULT_FORMAT = "DEFAULT";
const std::string Config::REST_SERVER_DEFAULT_ADDRESS =
    "https://api.tiledb.com";
const std::string Config::REST_SERIALIZATION_DEFAULT_FORMAT = "CAPNP";
const std::string Config::REST_SERVER_DEFAULT_HTTP_COMPRESSOR = "any";
const std::string Config::REST_RETRY_HTTP_CODES = "503";
const std::string Config::REST_RETRY_COUNT = "25";
const std::string Config::REST_RETRY_INITIAL_DELAY_MS = "500";
const std::string Config::REST_RETRY_DELAY_FACTOR = "1.25";
const std::string Config::REST_CURL_BUFFER_SIZE = "524288";
const std::string Config::REST_CURL_VERBOSE = "false";
const std::string Config::REST_LOAD_METADATA_ON_ARRAY_OPEN = "true";
const std::string Config::REST_LOAD_NON_EMPTY_DOMAIN_ON_ARRAY_OPEN = "true";
const std::string Config::REST_USE_REFACTORED_ARRAY_OPEN = "false";
const std::string Config::REST_USE_REFACTORED_QUERY_SUBMIT = "false";
const std::string Config::SM_ALLOW_SEPARATE_ATTRIBUTE_WRITES = "false";
const std::string Config::SM_ALLOW_UPDATES_EXPERIMENTAL = "false";
const std::string Config::SM_ENCRYPTION_KEY = "";
const std::string Config::SM_ENCRYPTION_TYPE = "NO_ENCRYPTION";
const std::string Config::SM_DEDUP_COORDS = "false";
const std::string Config::SM_CHECK_COORD_DUPS = "true";
const std::string Config::SM_CHECK_COORD_OOB = "true";
const std::string Config::SM_READ_RANGE_OOB = "warn";
const std::string Config::SM_CHECK_GLOBAL_ORDER = "true";
const std::string Config::SM_SKIP_EST_SIZE_PARTITIONING = "false";
const std::string Config::SM_SKIP_UNARY_PARTITIONING_BUDGET_CHECK = "false";
const std::string Config::SM_MEMORY_BUDGET = "5368709120";       // 5GB
const std::string Config::SM_MEMORY_BUDGET_VAR = "10737418240";  // 10GB
const std::string Config::SM_QUERY_DENSE_QC_COORDS_MODE = "false";
const std::string Config::SM_QUERY_DENSE_READER = "refactored";
const std::string Config::SM_QUERY_SPARSE_GLOBAL_ORDER_READER = "refactored";
const std::string Config::SM_QUERY_SPARSE_UNORDERED_WITH_DUPS_READER =
    "refactored";
const std::string Config::SM_MEM_MALLOC_TRIM = "true";
const std::string Config::SM_UPPER_MEMORY_LIMIT = "1073741824";  // 1GB
const std::string Config::SM_MEM_TOTAL_BUDGET = "10737418240";   // 10GB
const std::string Config::SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_COORDS = "0.5";
const std::string Config::SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_TILE_RANGES = "0.1";
const std::string Config::SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_ARRAY_DATA = "0.1";
const std::string Config::SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_COORDS =
    "0.5";
const std::string Config::SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_TILE_RANGES =
    "0.1";
const std::string Config::SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_ARRAY_DATA =
    "0.1";
const std::string Config::SM_ENABLE_SIGNAL_HANDLERS = "true";
const std::string Config::SM_COMPUTE_CONCURRENCY_LEVEL =
    utils::parse::to_str(std::thread::hardware_concurrency());
const std::string Config::SM_IO_CONCURRENCY_LEVEL =
    utils::parse::to_str(std::thread::hardware_concurrency());
const std::string Config::SM_SKIP_CHECKSUM_VALIDATION = "false";
const std::string Config::SM_CONSOLIDATION_AMPLIFICATION = "1.0";
const std::string Config::SM_CONSOLIDATION_BUFFER_SIZE = "50000000";
const std::string Config::SM_CONSOLIDATION_MAX_FRAGMENT_SIZE =
    std::to_string(UINT64_MAX);
const std::string Config::SM_CONSOLIDATION_PURGE_DELETED_CELLS = "false";
const std::string Config::SM_CONSOLIDATION_STEPS = "4294967295";
const std::string Config::SM_CONSOLIDATION_STEP_MIN_FRAGS = "4294967295";
const std::string Config::SM_CONSOLIDATION_STEP_MAX_FRAGS = "4294967295";
const std::string Config::SM_CONSOLIDATION_STEP_SIZE_RATIO = "0.0";
const std::string Config::SM_CONSOLIDATION_MODE = "fragments";
const std::string Config::SM_CONSOLIDATION_TIMESTAMP_START = "0";
const std::string Config::SM_CONSOLIDATION_TIMESTAMP_END =
    std::to_string(UINT64_MAX);
const std::string Config::SM_VACUUM_MODE = "fragments";
const std::string Config::SM_VACUUM_TIMESTAMP_START = "0";
const std::string Config::SM_VACUUM_TIMESTAMP_END = std::to_string(UINT64_MAX);
const std::string Config::SM_OFFSETS_BITSIZE = "64";
const std::string Config::SM_OFFSETS_EXTRA_ELEMENT = "false";
const std::string Config::SM_OFFSETS_FORMAT_MODE = "bytes";
const std::string Config::SM_MAX_TILE_OVERLAP_SIZE = "314572800";  // 300MiB
const std::string Config::SM_GROUP_TIMESTAMP_START = "0";
const std::string Config::SM_GROUP_TIMESTAMP_END = std::to_string(UINT64_MAX);
const std::string Config::SM_FRAGMENT_INFO_PRELOAD_MBRS = "false";
const std::string Config::SM_PARTIAL_TILE_OFFSETS_LOADING = "false";
const std::string Config::VFS_MIN_PARALLEL_SIZE = "10485760";
const std::string Config::VFS_MAX_BATCH_SIZE = "104857600";
const std::string Config::VFS_MIN_BATCH_GAP = "512000";
const std::string Config::VFS_MIN_BATCH_SIZE = "20971520";
const std::string Config::VFS_FILE_POSIX_FILE_PERMISSIONS = "644";
const std::string Config::VFS_FILE_POSIX_DIRECTORY_PERMISSIONS = "755";
const std::string Config::VFS_FILE_MAX_PARALLEL_OPS = "1";
const std::string Config::VFS_READ_AHEAD_SIZE = "102400";          // 100KiB
const std::string Config::VFS_READ_AHEAD_CACHE_SIZE = "10485760";  // 10MiB;
const std::string Config::VFS_AZURE_STORAGE_ACCOUNT_NAME = "";
const std::string Config::VFS_AZURE_STORAGE_ACCOUNT_KEY = "";
const std::string Config::VFS_AZURE_STORAGE_SAS_TOKEN = "";
const std::string Config::VFS_AZURE_BLOB_ENDPOINT = "";
const std::string Config::VFS_AZURE_USE_HTTPS = "true";
const std::string Config::VFS_AZURE_MAX_PARALLEL_OPS =
    Config::SM_IO_CONCURRENCY_LEVEL;
const std::string Config::VFS_AZURE_BLOCK_LIST_BLOCK_SIZE = "5242880";
const std::string Config::VFS_AZURE_USE_BLOCK_LIST_UPLOAD = "true";
const std::string Config::VFS_GCS_PROJECT_ID = "";
const std::string Config::VFS_GCS_MAX_PARALLEL_OPS =
    Config::SM_IO_CONCURRENCY_LEVEL;
const std::string Config::VFS_GCS_MULTI_PART_SIZE = "5242880";
const std::string Config::VFS_GCS_USE_MULTI_PART_UPLOAD = "true";
const std::string Config::VFS_GCS_REQUEST_TIMEOUT_MS = "3000";
const std::string Config::VFS_S3_REGION = "us-east-1";
const std::string Config::VFS_S3_AWS_ACCESS_KEY_ID = "";
const std::string Config::VFS_S3_AWS_SECRET_ACCESS_KEY = "";
const std::string Config::VFS_S3_AWS_SESSION_TOKEN = "";
const std::string Config::VFS_S3_AWS_ROLE_ARN = "";
const std::string Config::VFS_S3_AWS_EXTERNAL_ID = "";
const std::string Config::VFS_S3_AWS_LOAD_FREQUENCY = "";
const std::string Config::VFS_S3_AWS_SESSION_NAME = "";
const std::string Config::VFS_S3_SCHEME = "https";
const std::string Config::VFS_S3_ENDPOINT_OVERRIDE = "";
const std::string Config::VFS_S3_USE_VIRTUAL_ADDRESSING = "true";
const std::string Config::VFS_S3_SKIP_INIT = "false";
const std::string Config::VFS_S3_USE_MULTIPART_UPLOAD = "true";
const std::string Config::VFS_S3_MAX_PARALLEL_OPS =
    Config::SM_IO_CONCURRENCY_LEVEL;
const std::string Config::VFS_S3_MULTIPART_PART_SIZE = "5242880";
const std::string Config::VFS_S3_CA_FILE = "";
const std::string Config::VFS_S3_CA_PATH = "";
const std::string Config::VFS_S3_CONNECT_TIMEOUT_MS = "10800";
const std::string Config::VFS_S3_CONNECT_MAX_TRIES = "5";
const std::string Config::VFS_S3_CONNECT_SCALE_FACTOR = "25";
const std::string Config::VFS_S3_SSE = "";
const std::string Config::VFS_S3_SSE_KMS_KEY_ID = "";
const std::string Config::VFS_S3_REQUEST_TIMEOUT_MS = "3000";
const std::string Config::VFS_S3_REQUESTER_PAYS = "false";
const std::string Config::VFS_S3_PROXY_SCHEME = "http";
const std::string Config::VFS_S3_PROXY_HOST = "";
const std::string Config::VFS_S3_PROXY_PORT = "0";
const std::string Config::VFS_S3_PROXY_USERNAME = "";
const std::string Config::VFS_S3_PROXY_PASSWORD = "";
const std::string Config::VFS_S3_LOGGING_LEVEL = "Off";
const std::string Config::VFS_S3_VERIFY_SSL = "true";
const std::string Config::VFS_S3_NO_SIGN_REQUEST = "false";
const std::string Config::VFS_S3_BUCKET_CANNED_ACL = "NOT_SET";
const std::string Config::VFS_S3_OBJECT_CANNED_ACL = "NOT_SET";
const std::string Config::VFS_HDFS_KERB_TICKET_CACHE_PATH = "";
const std::string Config::VFS_HDFS_NAME_NODE_URI = "";
const std::string Config::VFS_HDFS_USERNAME = "";
const std::string Config::FILESTORE_BUFFER_SIZE = "104857600";

const std::map<std::string, std::string> default_config_values = {
    std::make_pair("rest.server_address", Config::REST_SERVER_DEFAULT_ADDRESS),
    std::make_pair(
        "rest.server_serialization_format",
        Config::REST_SERIALIZATION_DEFAULT_FORMAT),
    std::make_pair(
        "rest.http_compressor", Config::REST_SERVER_DEFAULT_HTTP_COMPRESSOR),
    std::make_pair("rest.retry_http_codes", Config::REST_RETRY_HTTP_CODES),
    std::make_pair("rest.retry_count", Config::REST_RETRY_COUNT),
    std::make_pair(
        "rest.retry_initial_delay_ms", Config::REST_RETRY_INITIAL_DELAY_MS),
    std::make_pair("rest.retry_delay_factor", Config::REST_RETRY_DELAY_FACTOR),
    std::make_pair("rest.curl.buffer_size", Config::REST_CURL_BUFFER_SIZE),
    std::make_pair("rest.curl.verbose", Config::REST_CURL_VERBOSE),
    std::make_pair(
        "rest.load_metadata_on_array_open",
        Config::REST_LOAD_METADATA_ON_ARRAY_OPEN),
    std::make_pair(
        "rest.load_non_empty_domain_on_array_open",
        Config::REST_LOAD_NON_EMPTY_DOMAIN_ON_ARRAY_OPEN),
    std::make_pair(
        "rest.use_refactored_array_open",
        Config::REST_USE_REFACTORED_ARRAY_OPEN),
    std::make_pair(
        "rest.use_refactored_array_open_and_query_submit",
        Config::REST_USE_REFACTORED_QUERY_SUBMIT),
    std::make_pair(
        "config.env_var_prefix", Config::CONFIG_ENVIRONMENT_VARIABLE_PREFIX),
    std::make_pair("config.logging_level", Config::CONFIG_LOGGING_LEVEL),
    std::make_pair(
        "config.logging_format", Config::CONFIG_LOGGING_DEFAULT_FORMAT),
    std::make_pair(
        "sm.allow_separate_attribute_writes",
        Config::SM_ALLOW_SEPARATE_ATTRIBUTE_WRITES),
    std::make_pair(
        "sm.allow_updates_experimental", Config::SM_ALLOW_UPDATES_EXPERIMENTAL),
    std::make_pair("sm.encryption_key", Config::SM_ENCRYPTION_KEY),
    std::make_pair("sm.encryption_type", Config::SM_ENCRYPTION_TYPE),
    std::make_pair("sm.dedup_coords", Config::SM_DEDUP_COORDS),
    std::make_pair("sm.check_coord_dups", Config::SM_CHECK_COORD_DUPS),
    std::make_pair("sm.check_coord_oob", Config::SM_CHECK_COORD_OOB),
    std::make_pair("sm.read_range_oob", Config::SM_READ_RANGE_OOB),
    std::make_pair("sm.check_global_order", Config::SM_CHECK_GLOBAL_ORDER),
    std::make_pair(
        "sm.skip_est_size_partitioning", Config::SM_SKIP_EST_SIZE_PARTITIONING),
    std::make_pair(
        "sm.skip_unary_partitioning_budget_check",
        Config::SM_SKIP_UNARY_PARTITIONING_BUDGET_CHECK),
    std::make_pair("sm.memory_budget", Config::SM_MEMORY_BUDGET),
    std::make_pair("sm.memory_budget_var", Config::SM_MEMORY_BUDGET_VAR),
    std::make_pair(
        "sm.query.dense.qc_coords_mode", Config::SM_QUERY_DENSE_QC_COORDS_MODE),
    std::make_pair("sm.query.dense.reader", Config::SM_QUERY_DENSE_READER),
    std::make_pair(
        "sm.query.sparse_global_order.reader",
        Config::SM_QUERY_SPARSE_GLOBAL_ORDER_READER),
    std::make_pair(
        "sm.query.sparse_unordered_with_dups.reader",
        Config::SM_QUERY_SPARSE_UNORDERED_WITH_DUPS_READER),
    std::make_pair("sm.mem.malloc_trim", Config::SM_MEM_MALLOC_TRIM),
    std::make_pair(
        "sm.mem.tile_upper_memory_limit", Config::SM_UPPER_MEMORY_LIMIT),
    std::make_pair("sm.mem.total_budget", Config::SM_MEM_TOTAL_BUDGET),
    std::make_pair(
        "sm.mem.reader.sparse_global_order.ratio_coords",
        Config::SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_COORDS),
    std::make_pair(
        "sm.mem.reader.sparse_global_order.ratio_tile_ranges",
        Config::SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_TILE_RANGES),
    std::make_pair(
        "sm.mem.reader.sparse_global_order.ratio_array_data",
        Config::SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_ARRAY_DATA),
    std::make_pair(
        "sm.mem.reader.sparse_unordered_with_dups.ratio_coords",
        Config::SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_COORDS),
    std::make_pair(
        "sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges",
        Config::SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_TILE_RANGES),
    std::make_pair(
        "sm.mem.reader.sparse_unordered_with_dups.ratio_array_data",
        Config::SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_ARRAY_DATA),
    std::make_pair(
        "sm.enable_signal_handlers", Config::SM_ENABLE_SIGNAL_HANDLERS),
    std::make_pair(
        "sm.compute_concurrency_level", Config::SM_COMPUTE_CONCURRENCY_LEVEL),
    std::make_pair("sm.io_concurrency_level", Config::SM_IO_CONCURRENCY_LEVEL),
    std::make_pair(
        "sm.skip_checksum_validation", Config::SM_SKIP_CHECKSUM_VALIDATION),
    std::make_pair(
        "sm.consolidation.amplification",
        Config::SM_CONSOLIDATION_AMPLIFICATION),
    std::make_pair(
        "sm.consolidation.buffer_size", Config::SM_CONSOLIDATION_BUFFER_SIZE),
    std::make_pair(
        "sm.consolidation.max_fragment_size",
        Config::SM_CONSOLIDATION_MAX_FRAGMENT_SIZE),
    std::make_pair(
        "sm.consolidation.purge_deleted_cells",
        Config::SM_CONSOLIDATION_PURGE_DELETED_CELLS),
    std::make_pair(
        "sm.consolidation.step_min_frags",
        Config::SM_CONSOLIDATION_STEP_MIN_FRAGS),
    std::make_pair(
        "sm.consolidation.step_max_frags",
        Config::SM_CONSOLIDATION_STEP_MAX_FRAGS),
    std::make_pair(
        "sm.consolidation.step_size_ratio",
        Config::SM_CONSOLIDATION_STEP_SIZE_RATIO),
    std::make_pair("sm.consolidation.steps", Config::SM_CONSOLIDATION_STEPS),
    std::make_pair("sm.consolidation.mode", Config::SM_CONSOLIDATION_MODE),
    std::make_pair(
        "sm.consolidation.timestamp_start",
        Config::SM_CONSOLIDATION_TIMESTAMP_START),
    std::make_pair(
        "sm.consolidation.timestamp_end",
        Config::SM_CONSOLIDATION_TIMESTAMP_END),
    std::make_pair("sm.vacuum.mode", Config::SM_VACUUM_MODE),
    std::make_pair("sm.var_offsets.bitsize", Config::SM_OFFSETS_BITSIZE),
    std::make_pair(
        "sm.var_offsets.extra_element", Config::SM_OFFSETS_EXTRA_ELEMENT),
    std::make_pair("sm.var_offsets.mode", Config::SM_OFFSETS_FORMAT_MODE),
    std::make_pair(
        "sm.max_tile_overlap_size", Config::SM_MAX_TILE_OVERLAP_SIZE),
    std::make_pair(
        "sm.group.timestamp_start", Config::SM_GROUP_TIMESTAMP_START),
    std::make_pair("sm.group.timestamp_end", Config::SM_GROUP_TIMESTAMP_END),
    std::make_pair(
        "sm.fragment_info.preload_mbrs", Config::SM_FRAGMENT_INFO_PRELOAD_MBRS),
    std::make_pair(
        "sm.partial_tile_offsets_loading",
        Config::SM_PARTIAL_TILE_OFFSETS_LOADING),
    std::make_pair("vfs.min_parallel_size", Config::VFS_MIN_PARALLEL_SIZE),
    std::make_pair("vfs.max_batch_size", Config::VFS_MAX_BATCH_SIZE),
    std::make_pair("vfs.min_batch_gap", Config::VFS_MIN_BATCH_GAP),
    std::make_pair("vfs.min_batch_size", Config::VFS_MIN_BATCH_SIZE),
    std::make_pair("vfs.read_ahead_size", Config::VFS_READ_AHEAD_SIZE),
    std::make_pair(
        "vfs.read_ahead_cache_size", Config::VFS_READ_AHEAD_CACHE_SIZE),
    std::make_pair(
        "vfs.file.posix_file_permissions",
        Config::VFS_FILE_POSIX_FILE_PERMISSIONS),
    std::make_pair(
        "vfs.file.posix_directory_permissions",
        Config::VFS_FILE_POSIX_DIRECTORY_PERMISSIONS),
    std::make_pair(
        "vfs.file.max_parallel_ops", Config::VFS_FILE_MAX_PARALLEL_OPS),
    std::make_pair(
        "vfs.azure.storage_account_name",
        Config::VFS_AZURE_STORAGE_ACCOUNT_NAME),
    std::make_pair(
        "vfs.azure.storage_account_key", Config::VFS_AZURE_STORAGE_ACCOUNT_KEY),
    std::make_pair("vfs.azure.blob_endpoint", Config::VFS_AZURE_BLOB_ENDPOINT),
    std::make_pair(
        "vfs.azure.max_parallel_ops", Config::VFS_AZURE_MAX_PARALLEL_OPS),
    std::make_pair(
        "vfs.azure.block_list_block_size",
        Config::VFS_AZURE_BLOCK_LIST_BLOCK_SIZE),
    std::make_pair(
        "vfs.azure.use_block_list_upload",
        Config::VFS_AZURE_USE_BLOCK_LIST_UPLOAD),
    std::make_pair("vfs.gcs.project_id", Config::VFS_GCS_PROJECT_ID),
    std::make_pair(
        "vfs.gcs.max_parallel_ops", Config::VFS_GCS_MAX_PARALLEL_OPS),
    std::make_pair("vfs.gcs.multi_part_size", Config::VFS_GCS_MULTI_PART_SIZE),
    std::make_pair(
        "vfs.gcs.use_multi_part_upload", Config::VFS_GCS_USE_MULTI_PART_UPLOAD),
    std::make_pair(
        "vfs.gcs.request_timeout_ms", Config::VFS_GCS_REQUEST_TIMEOUT_MS),
    std::make_pair("vfs.s3.region", Config::VFS_S3_REGION),
    std::make_pair(
        "vfs.s3.aws_access_key_id", Config::VFS_S3_AWS_ACCESS_KEY_ID),
    std::make_pair(
        "vfs.s3.aws_secret_access_key", Config::VFS_S3_AWS_SECRET_ACCESS_KEY),
    std::make_pair(
        "vfs.s3.aws_session_token", Config::VFS_S3_AWS_SESSION_TOKEN),
    std::make_pair("vfs.s3.aws_role_arn", Config::VFS_S3_AWS_ROLE_ARN),
    std::make_pair("vfs.s3.aws_external_id", Config::VFS_S3_AWS_EXTERNAL_ID),
    std::make_pair(
        "vfs.s3.aws_load_frequency", Config::VFS_S3_AWS_LOAD_FREQUENCY),
    std::make_pair("vfs.s3.aws_session_name", Config::VFS_S3_AWS_SESSION_NAME),
    std::make_pair("vfs.s3.scheme", Config::VFS_S3_SCHEME),
    std::make_pair(
        "vfs.s3.endpoint_override", Config::VFS_S3_ENDPOINT_OVERRIDE),
    std::make_pair(
        "vfs.s3.use_virtual_addressing", Config::VFS_S3_USE_VIRTUAL_ADDRESSING),
    std::make_pair("vfs.s3.skip_init", Config::VFS_S3_SKIP_INIT),
    std::make_pair(
        "vfs.s3.use_multipart_upload", Config::VFS_S3_USE_MULTIPART_UPLOAD),
    std::make_pair("vfs.s3.max_parallel_ops", Config::VFS_S3_MAX_PARALLEL_OPS),
    std::make_pair(
        "vfs.s3.multipart_part_size", Config::VFS_S3_MULTIPART_PART_SIZE),
    std::make_pair("vfs.s3.ca_file", Config::VFS_S3_CA_FILE),
    std::make_pair("vfs.s3.ca_path", Config::VFS_S3_CA_PATH),
    std::make_pair(
        "vfs.s3.connect_timeout_ms", Config::VFS_S3_CONNECT_TIMEOUT_MS),
    std::make_pair(
        "vfs.s3.connect_max_tries", Config::VFS_S3_CONNECT_MAX_TRIES),
    std::make_pair(
        "vfs.s3.connect_scale_factor", Config::VFS_S3_CONNECT_SCALE_FACTOR),
    std::make_pair("vfs.s3.sse", Config::VFS_S3_SSE),
    std::make_pair("vfs.s3.sse_kms_key_id", Config::VFS_S3_SSE_KMS_KEY_ID),
    std::make_pair(
        "vfs.s3.request_timeout_ms", Config::VFS_S3_REQUEST_TIMEOUT_MS),
    std::make_pair("vfs.s3.requester_pays", Config::VFS_S3_REQUESTER_PAYS),
    std::make_pair("vfs.s3.proxy_scheme", Config::VFS_S3_PROXY_SCHEME),
    std::make_pair("vfs.s3.proxy_host", Config::VFS_S3_PROXY_HOST),
    std::make_pair("vfs.s3.proxy_port", Config::VFS_S3_PROXY_PORT),
    std::make_pair("vfs.s3.proxy_username", Config::VFS_S3_PROXY_USERNAME),
    std::make_pair("vfs.s3.proxy_password", Config::VFS_S3_PROXY_PASSWORD),
    std::make_pair("vfs.s3.logging_level", Config::VFS_S3_LOGGING_LEVEL),
    std::make_pair("vfs.s3.verify_ssl", Config::VFS_S3_VERIFY_SSL),
    std::make_pair("vfs.s3.no_sign_request", Config::VFS_S3_NO_SIGN_REQUEST),
    std::make_pair(
        "vfs.s3.bucket_canned_acl", Config::VFS_S3_BUCKET_CANNED_ACL),
    std::make_pair(
        "vfs.s3.object_canned_acl", Config::VFS_S3_OBJECT_CANNED_ACL),
    std::make_pair("vfs.hdfs.name_node_uri", Config::VFS_HDFS_NAME_NODE_URI),
    std::make_pair("vfs.hdfs.username", Config::VFS_HDFS_USERNAME),
    std::make_pair(
        "vfs.hdfs.kerb_ticket_cache_path",
        Config::VFS_HDFS_KERB_TICKET_CACHE_PATH),
    std::make_pair("filestore.buffer_size", Config::FILESTORE_BUFFER_SIZE),
};

/* ****************************** */
/*        PRIVATE CONSTANTS       */
/* ****************************** */

const char Config::COMMENT_START = '#';

const std::set<std::string> Config::unserialized_params_ = {
    "vfs.azure.storage_account_name",
    "vfs.azure.storage_account_key",
    "vfs.s3.proxy_username",
    "vfs.s3.proxy_password",
    "vfs.s3.aws_access_key_id",
    "vfs.s3.aws_secret_access_key",
    "vfs.s3.aws_session_token",
    "vfs.s3.aws_role_arn",
    "vfs.s3.aws_external_id",
    "vfs.s3.aws_load_frequency",
    "vfs.s3.aws_session_name",
    "rest.username",
    "rest.password",
    "rest.token",
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Config::Config() {
  // Set config values
  param_values_ = default_config_values;
}

Config::~Config() = default;

/* ****************************** */
/*                API             */
/* ****************************** */

Status Config::load_from_file(const std::string& filename) {
  // Do nothing if filename is empty
  if (filename.empty())
    return LOG_STATUS(
        Status_ConfigError("Cannot load from file; Invalid filename"));

  std::ifstream ifs(filename);
  if (!ifs.is_open()) {
    std::stringstream msg;
    msg << "Failed to open config file '" << filename << "'";
    return LOG_STATUS(Status_ConfigError(msg.str()));
  }

  size_t linenum = 0;
  std::string param, value, extra;
  for (std::string line; std::getline(ifs, line);) {
    std::stringstream line_ss(line);

    // Parse parameter
    line_ss >> param;
    if (param.empty() || param[0] == COMMENT_START) {
      linenum += 1;
      continue;
    }

    // Parse value
    line_ss >> value;
    if (value.empty()) {
      std::stringstream msg;
      msg << "Failed to parse config file '" << filename << "'; ";
      msg << "Missing parameter value (line: " << linenum << ")";
      return LOG_STATUS(Status_ConfigError(msg.str()));
    }

    // Parse extra
    line_ss >> extra;
    if (!extra.empty() && extra[0] != COMMENT_START) {
      std::stringstream msg;
      msg << "Failed to parse config file '" << filename << "'; ";
      msg << "Invalid line format (line: " << linenum << ")";
      return LOG_STATUS(Status_ConfigError(msg.str()));
    }

    // Set param-value pair
    param_values_[param] = value;

    linenum += 1;
  }
  return Status::Ok();
}

Status Config::save_to_file(const std::string& filename) {
  // Do nothing if filename is empty
  if (filename.empty())
    return LOG_STATUS(
        Status_ConfigError("Cannot save to file; Invalid filename"));

  std::ofstream ofs(filename);
  if (!ofs.is_open()) {
    std::stringstream msg;
    msg << "Failed to open config file '" << filename << "' for writing";
    return LOG_STATUS(Status_ConfigError(msg.str()));
  }
  for (auto& pv : param_values_) {
    if (unserialized_params_.count(pv.first) != 0)
      continue;
    if (!pv.second.empty())
      ofs << pv.first << " " << pv.second << "\n";
  }
  return Status::Ok();
}

Status Config::set(const std::string& param, const std::string& value) {
  RETURN_NOT_OK(sanity_check(param, value));
  param_values_[param] = value;
  set_params_.insert(param);

  return Status::Ok();
}

std::string Config::get(const std::string& param, bool* found) const {
  const char* val = get_from_config_or_env(param, found);
  return *found ? val : "";
}

Status Config::get(const std::string& param, const char** value) const {
  bool found;
  const char* val = get_from_config_or_env(param, &found);
  *value = found ? val : nullptr;

  return Status::Ok();
}

template <class T>
Status Config::get(const std::string& param, T* value, bool* found) const {
  // Check if parameter exists
  const char* val = get_from_config_or_env(param, found);
  if (!*found)
    return Status::Ok();

  // Parameter found, retrieve value
  auto status = utils::parse::convert(val, value);
  if (!status.ok()) {
    return Status_ConfigError(
        std::string("Failed to parse config value '") + std::string(val) +
        std::string("' for key '") + param + "' due to: " + status.to_string());
  }
  return Status::Ok();
}

/*
 * Template definition not in header; explicitly instantiated below. It's here
 * to deal with legacy difficulties with header dependencies.
 */
template <class T>
Status Config::get_vector(
    const std::string& param, std::vector<T>* value, bool* found) const {
  // Check if parameter exists
  const char* val = get_from_config_or_env(param, found);
  if (!*found)
    return Status::Ok();

  // Parameter found, retrieve value
  return utils::parse::convert<T>(val, value);
}

const std::map<std::string, std::string>& Config::param_values() const {
  return param_values_;
}

const std::set<std::string>& Config::set_params() const {
  return set_params_;
}

Status Config::unset(const std::string& param) {
  // Set back to default
  auto it = default_config_values.find(param);
  if (it != default_config_values.end()) {
    param_values_[param] = it->second;
  } else {
    param_values_.erase(param);
  }

  return Status::Ok();
}

void Config::inherit(const Config& config) {
  bool found;
  auto set_params = config.set_params();
  for (const auto& p : set_params) {
    auto v = config.get(p, &found);
    assert(found);
    throw_if_not_ok(set(p, v));
  }
}

bool Config::operator==(const Config& rhs) const {
  if (param_values_.size() != rhs.param_values_.size())
    return false;
  for (const auto& pv : param_values_) {
    const std::string& key = pv.first;
    if (rhs.param_values_.count(key) == 0)
      return false;
    const std::string& value = pv.second;
    if (rhs.param_values_.at(key) != value)
      return false;
  }
  return true;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Config::sanity_check(
    const std::string& param, const std::string& value) const {
  bool v = false;
  uint64_t vuint64 = 0;
  uint32_t v32 = 0;
  float vf = 0.0f;
  int64_t vint64 = 0;
  int chkno = -1;

  if (param == "rest.server_serialization_format") {
    SerializationType serialization_type;
    RETURN_NOT_OK(serialization_type_enum(value, &serialization_type));
  } else if (param == "config.logging_level") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "config.logging_format") {
    if (value != "DEFAULT" && value != "JSON")
      return LOG_STATUS(
          Status_ConfigError("Invalid logging format parameter value"));
  } else if (param == "sm.allow_separate_attribute_writes") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.allow_updates_experimental") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.dedup_coords") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.check_coord_dups") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.check_coord_oob") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.check_global_order") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.memory_budget") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.memory_budget_var") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.enable_signal_handlers") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.compute_concurrency_level") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.io_concurrency_level") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.consolidation.amplification") {
    RETURN_NOT_OK(utils::parse::convert(value, &vf));
  } else if (param == "sm.consolidation.buffer_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.consolidation.max_fragment_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.consolidation.purge_deleted_cells") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.consolidation.steps") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "sm.consolidation.step_min_frags") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "sm.consolidation.step_max_frags") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "sm.consolidation.step_size_ratio") {
    RETURN_NOT_OK(utils::parse::convert(value, &vf));
  } else if (param == "sm.var_offsets.bitsize") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "sm.var_offsets.extra_element") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.var_offsets.mode") {
    if (value != "bytes" && value != "elements")
      return LOG_STATUS(
          Status_ConfigError("Invalid offsets format parameter value"));
  } else if (param == "sm.fragment_info.preload_mbrs") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "vfs.min_parallel_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.max_batch_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.min_batch_gap") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.min_batch_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.read_ahead_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.read_ahead_cache_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.file.posix_file_permissions") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "vfs.file.posix_directory_permissions") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "vfs.file.max_parallel_ops") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.s3.scheme") {
    if (value != "http" && value != "https")
      return LOG_STATUS(
          Status_ConfigError("Invalid S3 scheme parameter value"));
  } else if (param == "vfs.s3.use_virtual_addressing") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "vfs.s3.skit_init") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "vfs.s3.use_multipart_upload") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "vfs.s3.max_parallel_ops") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.s3.multipart_part_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "vfs.s3.connect_timeout_ms") {
    RETURN_NOT_OK(utils::parse::convert(value, &vint64));
  } else if (param == "vfs.s3.connect_max_tries") {
    RETURN_NOT_OK(utils::parse::convert(value, &vint64));
  } else if (param == "vfs.s3.connect_scale_factor") {
    RETURN_NOT_OK(utils::parse::convert(value, &vint64));
  } else if (param == "vfs.s3.request_timeout_ms") {
    RETURN_NOT_OK(utils::parse::convert(value, &vint64));
  } else if (param == "vfs.s3.requester_pays") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "vfs.s3.proxy_port") {
    RETURN_NOT_OK(utils::parse::convert(value, &vint64));
  } else if (param == "vfs.s3.verify_ssl") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "vfs.s3.no_sign_request") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (
      (chkno = 1, param == "vfs.s3.bucket_canned_acl") ||
      (chkno = 2, param == "vfs.s3.object_canned_acl")) {
    // first items valid for both ObjectCannedACL and BucketCannedACL
    if (!((value == "NOT_SET") || (value == "private_") ||
          (value == "public_read") || (value == "public_read_write") ||
          (value == "authenticated_read") ||
          // items only valid for ObjectCannedACL
          (chkno == 2 &&
           ((value == "aws_exec_read") || (value == "bucket_owner_read") ||
            (value == "bucket_owner_full_control"))))) {
      std::stringstream msg;
      msg << "value " << param << " invalid canned acl for " << param;
      return Status_Error(msg.str());
    }
  }

  return Status::Ok();
}

std::string Config::convert_to_env_param(const std::string& param) const {
  std::stringstream ss;
  for (auto& c : param) {
    // We convert "." in parameter name to "_" for convention
    if (std::strncmp(&c, ".", 1) == 0) {
      ss << "_";
    } else {
      ss << static_cast<char>(::toupper(c));
    }
  }

  return ss.str();
}

const char* Config::get_from_env(const std::string& param, bool* found) const {
  std::string env_param = convert_to_env_param(param);

  // Get env variable prefix
  bool found_prefix;
  std::string env_prefix =
      get_from_config("config.env_var_prefix", &found_prefix);

  if (found_prefix)
    env_param = env_prefix + env_param;

  char* value = std::getenv(env_param.c_str());

  // getenv returns nullptr if variable is not found
  *found = value != nullptr;

  return value;
}

const char* Config::get_from_config(
    const std::string& param, bool* found) const {
  // First check config
  auto it = param_values_.find(param);
  *found = it != param_values_.end();
  return *found ? it->second.c_str() : "";
}

const char* Config::get_from_config_or_env(
    const std::string& param, bool* found) const {
  // First let's see if the User has set the parameter
  // If it is not a user set parameter it might be a default value if found in
  // the config
  bool user_set_parameter = set_params_.find(param) != set_params_.end();

  // First check config
  bool found_config;
  const char* value_config = get_from_config(param, &found_config);
  // If its a user set parameter from the config return it
  if (found_config && user_set_parameter) {
    *found = found_config;
    return value_config;
  }

  // Check if default for this parameter should be ignored based on
  // environment variables
  if (ignore_default_via_env(param)) {
    *found = true;
    return "";
  }

  // Check env if not found in config or if it was found in the config but is
  // a default value
  const char* value_env = get_from_env(param, found);
  if (*found)
    return value_env;

  // At this point the value was not found to be user set in the config or an
  // environmental variable so return any default value from the config or
  // indicate it was not found
  *found = found_config;
  return *found ? value_config : "";
}

template <class T, bool must_find_>
optional<T> Config::get_internal(const std::string& key) const {
  auto value = get_internal_string<must_find_>(key);
  if (value.has_value()) {
    T converted_value;
    auto status = utils::parse::convert(value.value(), &converted_value);
    if (status.ok()) {
      return {converted_value};
    }
    throw_config_exception(
        "Failed to parse config value '" + std::string(value.value()) +
        "' for key '" + key + "'. Reason: " + status.to_string());
  }

  return {nullopt};
}

template <bool must_find_>
optional<std::string> Config::get_internal_string(
    const std::string& key) const {
  bool found;
  const char* value = get_from_config_or_env(key, &found);
  if (found) {
    return {value};
  }

  if constexpr (must_find_) {
    throw_config_exception("Failed to get config value for key: " + key);
  }
  return {nullopt};
}

/*
 * Explicit instantiations
 */
template Status Config::get<bool>(
    const std::string& param, bool* value, bool* found) const;
template Status Config::get<int>(
    const std::string& param, int* value, bool* found) const;
template Status Config::get<uint32_t>(
    const std::string& param, uint32_t* value, bool* found) const;
template Status Config::get<int64_t>(
    const std::string& param, int64_t* value, bool* found) const;
template Status Config::get<uint64_t>(
    const std::string& param, uint64_t* value, bool* found) const;
template Status Config::get<float>(
    const std::string& param, float* value, bool* found) const;
template Status Config::get<double>(
    const std::string& param, double* value, bool* found) const;
template Status Config::get_vector<uint32_t>(
    const std::string& param, std::vector<uint32_t>* value, bool* found) const;

template optional<bool> Config::get<bool>(const std::string&) const;
template optional<int> Config::get<int>(const std::string&) const;
template optional<uint32_t> Config::get<uint32_t>(const std::string&) const;
template optional<int64_t> Config::get<int64_t>(const std::string&) const;
template optional<uint64_t> Config::get<uint64_t>(const std::string&) const;
template optional<float> Config::get<float>(const std::string&) const;
template optional<double> Config::get<double>(const std::string&) const;

template bool Config::get<bool>(
    const std::string&, const Config::MustFindMarker&) const;
template int Config::get<int>(
    const std::string&, const Config::MustFindMarker&) const;
template uint32_t Config::get<uint32_t>(
    const std::string&, const Config::MustFindMarker&) const;
template int64_t Config::get<int64_t>(
    const std::string&, const Config::MustFindMarker&) const;
template uint64_t Config::get<uint64_t>(
    const std::string&, const Config::MustFindMarker&) const;
template float Config::get<float>(
    const std::string&, const Config::MustFindMarker&) const;
template double Config::get<double>(
    const std::string&, const Config::MustFindMarker&) const;

template optional<bool> Config::get_internal<bool, false>(
    const std::string& key) const;
template optional<bool> Config::get_internal<bool, true>(
    const std::string& key) const;
template optional<int> Config::get_internal<int, false>(
    const std::string& key) const;
template optional<int> Config::get_internal<int, true>(
    const std::string& key) const;
template optional<uint32_t> Config::get_internal<uint32_t, false>(
    const std::string& key) const;
template optional<uint32_t> Config::get_internal<uint32_t, true>(
    const std::string& key) const;
template optional<int64_t> Config::get_internal<int64_t, false>(
    const std::string& key) const;
template optional<int64_t> Config::get_internal<int64_t, true>(
    const std::string& key) const;
template optional<uint64_t> Config::get_internal<uint64_t, false>(
    const std::string& key) const;
template optional<uint64_t> Config::get_internal<uint64_t, true>(
    const std::string& key) const;
template optional<float> Config::get_internal<float, false>(
    const std::string& key) const;
template optional<float> Config::get_internal<float, true>(
    const std::string& key) const;
template optional<double> Config::get_internal<double, false>(
    const std::string& key) const;
template optional<double> Config::get_internal<double, true>(
    const std::string& key) const;

template optional<std::string> Config::get_internal_string<true>(
    const std::string& key) const;
template optional<std::string> Config::get_internal_string<false>(
    const std::string& key) const;

}  // namespace tiledb::sm
