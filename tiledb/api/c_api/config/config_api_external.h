/**
 * @file tiledb/api/c_api/config/config_api_external.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares the configuration section of the C API for TileDB. It
 * include both configurations and their iterators.
 */

#ifndef TILEDB_CAPI_CONFIG_EXTERNAL_H
#define TILEDB_CAPI_CONFIG_EXTERNAL_H

#include "../api_external_common.h"
#include "../error/error_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB configuration
 */
typedef struct tiledb_config_handle_t tiledb_config_t;

/**
 * C API carrier for an iterator over a TileDB configuration
 */
typedef struct tiledb_config_iter_handle_t tiledb_config_iter_t;

/**
 * Creates a TileDB config.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_t* config;
 * tiledb_error_t* error = NULL;
 * tiledb_config_alloc(&config, &error);
 * @endcode
 *
 * @param config The config to be created.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_alloc(
    tiledb_config_t** config, tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB config.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_t* config;
 * tiledb_error_t* error = NULL;
 * tiledb_config_alloc(&config, &error);
 * tiledb_config_free(&config);
 * @endcode
 *
 * @param config The config to be freed.
 */
TILEDB_EXPORT void tiledb_config_free(tiledb_config_t** config) TILEDB_NOEXCEPT;

/**
 * Sets a config parameter.
 *
 * **Parameters**
 *
 * - `sm.allow_separate_attribute_writes` <br>
 *    **Experimental** <br>
 *    Allow separate attribute write queries.<br>
 *    **Default**: false
 * - `sm.allow_updates_experimental` <br>
 *    **Experimental** <br>
 *    Allow update queries. Experimental for testing purposes, do not use.<br>
 *    **Default**: false
 * - `sm.dedup_coords` <br>
 *    If `true`, cells with duplicate coordinates will be removed during sparse
 *    fragment writes. Note that ties during deduplication are broken
 *    arbitrarily. <br>
 *    **Default**: false
 * - `sm.check_coord_dups` <br>
 *    This is applicable only if `sm.dedup_coords` is `false`.
 *    If `true`, an error will be thrown if there are cells with duplicate
 *    coordinates during sparse fragmnet writes. If `false` and there are
 *    duplicates, the duplicates will be written without errors. <br>
 *    **Default**: true
 * - `sm.check_coord_oob` <br>
 *    If `true`, an error will be thrown if there are cells with coordinates
 *    lying outside the domain during sparse fragment writes.  <br>
 *    **Default**: true
 * - `sm.read_range_oob` <br>
 *    If `error`, this will check ranges for read with out-of-bounds on the
 *    dimension domain's. If `warn`, the ranges will be capped at the
 *    dimension's domain and a warning logged. <br>
 *    **Default**: warn
 * - `sm.check_global_order` <br>
 *    Checks if the coordinates obey the global array order. Applicable only
 *    to sparse writes in global order.
 *    **Default**: true
 * - `sm.enable_signal_handlers` <br>
 *    Determines whether or not TileDB will install signal handlers. <br>
 *    **Default**: true
 * - `sm.compute_concurrency_level` <br>
 *    Upper-bound on number of threads to allocate for compute-bound tasks. <br>
 *    **Default*: # cores
 * - `sm.io_concurrency_level` <br>
 *    Upper-bound on number of threads to allocate for IO-bound tasks. <br>
 *    **Default*: # cores
 * - `sm.vacuum.mode` <br>
 *    The vacuuming mode, one of
 *    `commits` (remove only consolidated commit files),
 *    `fragments` (remove only consolidated fragments),
 *    `fragment_meta` (remove only consolidated fragment metadata),
 *    `array_meta` (remove only consolidated array metadata files), or
 *    `group_meta` (remove only consolidate group metadata only).
 *    <br>
 *    **Default**: fragments
 * - `sm.consolidation.mode` <br>
 *    The consolidation mode, one of
 *    `commits` (consolidate all commit files),
 *    `fragments` (consolidate all fragments),
 *    `fragment_meta` (consolidate only fragment metadata footers to a single
 *    file), `array_meta` (consolidate array metadata only), or `group_meta`
 *    (consolidate group metadata only). <br>
 *    **Default**: "fragments"
 * - `sm.consolidation.amplification` <br>
 *    The factor by which the size of the dense fragment resulting
 *    from consolidating a set of fragments (containing at least one
 *    dense fragment) can be amplified. This is important when
 *    the union of the non-empty domains of the fragments to be
 *    consolidated have a lot of empty cells, which the consolidated
 *    fragment will have to fill with the special fill value
 *    (since the resulting fragment is dense). <br>
 *    **Default**: 1.0
 * - `sm.consolidation.buffer_size` <br>
 *    The size (in bytes) of the attribute buffers used during
 *    consolidation. <br>
 *    **Default**: 50,000,000
 * - `sm.consolidation.max_fragment_size` <br>
 *    **Experimental** <br>
 *    The size (in bytes) of the maximum on-disk fragment size that will be
 *    created by consolidation. When it is reached, consolidation will continue
 *    the operation in a new fragment. The result will be a multiple fragments,
 *    but with seperate MBRs. <br>
 * - `sm.consolidation.steps` <br>
 *    The number of consolidation steps to be performed when executing
 *    the consolidation algorithm.<br>
 *    **Default**: 1
 * - `sm.consolidation.purge_deleted_cells` <br>
 *    **Experimental** <br>
 *    Purge deleted cells from the consolidated fragment or not.<br>
 *    **Default**: false
 * - `sm.consolidation.step_min_frags` <br>
 *    The minimum number of fragments to consolidate in a single step.<br>
 *    **Default**: UINT32_MAX
 * - `sm.consolidation.step_max_frags` <br>
 *    The maximum number of fragments to consolidate in a single step.<br>
 *    **Default**: UINT32_MAX
 * - `sm.consolidation.step_size_ratio` <br>
 *    The size ratio that two ("adjacent") fragments must satisfy to be
 *    considered for consolidation in a single step.<br>
 *    **Default**: 0.0
 * - `sm.consolidation.timestamp_start` <br>
 *    **Experimental** <br>
 *    When set, an array will be consolidated between this value and
 *    `sm.consolidation.timestamp_end` (inclusive). <br>
 *    Only for `fragments` and `array_meta` consolidation mode. <br>
 *    **Default**: 0
 * - `sm.consolidation.timestamp_end` <br>
 *    **Experimental** <br>
 *    When set, an array will be consolidated between
 *    `sm.consolidation.timestamp_start` and this value (inclusive). <br>
 *    Only for `fragments` and `array_meta` consolidation mode. <br>
 *    **Default**: UINT64_MAX
 * - `sm.memory_budget` <br>
 *    The memory budget for tiles of fixed-sized attributes (or offsets for
 *    var-sized attributes) to be fetched during reads.<br>
 *    **Default**: 5GB
 * - `sm.memory_budget_var` <br>
 *    The memory budget for tiles of var-sized attributes
 *    to be fetched during reads.<br>
 *    **Default**: 10GB
 * - `sm.var_offsets.bitsize` <br>
 *    The size of offsets in bits to be used for offset buffers of var-sized
 *    attributes<br>
 *    **Default**: 64
 * - `sm.var_offsets.extra_element` <br>
 *    Add an extra element to the end of the offsets buffer of var-sized
 *    attributes which will point to the end of the values buffer.<br>
 *    **Default**: false
 * - `sm.var_offsets.mode` <br>
 *    The offsets format (`bytes` or `elements`) to be used for
 *    var-sized attributes.<br>
 *    **Default**: bytes
 * - `sm.query.dense.reader` <br>
 *    Which reader to use for dense queries. "refactored" or "legacy".<br>
 *    **Default**: refactored
 * - `sm.query.sparse_global_order.reader` <br>
 *    Which reader to use for sparse global order queries. "refactored"
 *    or "legacy".<br>
 *    **Default**: legacy
 * - `sm.query.sparse_unordered_with_dups.reader` <br>
 *    Which reader to use for sparse unordered with dups queries.
 *    "refactored" or "legacy".<br>
 *    **Default**: refactored
 * - `sm.mem.malloc_trim` <br>
 *    Should malloc_trim be called on context and query destruction? This might
 *    reduce residual memory usage. <br>
 *    **Default**: true
 * - `sm.mem.tile_upper_memory_limit` <br>
 *    **Experimental** <br>
 *    This is the upper memory limit that is used when loading tiles. For now it
 *    is only used in the dense reader but will be eventually used by all
 *    readers. The readers using this value will use it as a way to limit the
 *    amount of tile data that is brought into memory at once so that we don't
 *    incur performance penalties during memory movement operations. It is a
 *    soft limit that we might go over if a single tile doesn't fit into memory,
 *    we will allow to load that tile if it still fits within
 *    `sm.mem.total_budget`. <br>
 *    **Default**: 1GB
 * - `sm.mem.total_budget` <br>
 *    Memory budget for readers and writers. <br>
 *    **Default**: 10GB
 * - `sm.mem.reader.sparse_global_order.ratio_coords` <br>
 *    Ratio of the budget allocated for coordinates in the sparse global
 *    order reader. <br>
 *    **Default**: 0.5
 * - `sm.mem.reader.sparse_global_order.ratio_tile_ranges` <br>
 *    Ratio of the budget allocated for tile ranges in the sparse global
 *    order reader. <br>
 *    **Default**: 0.1
 * - `sm.mem.reader.sparse_global_order.ratio_array_data` <br>
 *    Ratio of the budget allocated for array data in the sparse global
 *    order reader. <br>
 *    **Default**: 0.1
 * - `sm.mem.reader.sparse_unordered_with_dups.ratio_coords` <br>
 *    Ratio of the budget allocated for coordinates in the sparse unordered
 *    with duplicates reader. <br>
 *    **Default**: 0.5
 * - `sm.mem.reader.sparse_unordered_with_dups.ratio_tile_ranges` <br>
 *    Ratio of the budget allocated for tile ranges in the sparse unordered
 *    with duplicates reader. <br>
 *    **Default**: 0.1
 * - `sm.mem.reader.sparse_unordered_with_dups.ratio_array_data` <br>
 *    Ratio of the budget allocated for array data in the sparse unordered
 *    with duplicates reader. <br>
 *    **Default**: 0.1
 *    The maximum byte size to read-ahead from the backend. <br>
 *    **Default**: 102400
 * - `sm.group.timestamp_start` <br>
 *    The start timestamp used for opening the group. <br>
 *    **Default**: 0
 * - `sm.group.timestamp_end` <br>
 *    The end timestamp used for opening the group. <br>
 *    Also used for the write timestamp if set. <br>
 *    **Default**: UINT64_MAX
 * -  `sm.partial_tile_offsets_loading`
 *    **Experimental** <br>
 *    If `true` tile offsets can be partially loaded and unloaded by the
 *    readers. <br>
 *    **Default**: false
 * - `sm.fragment_info.preload_mbrs` <br>
 *    If `true` MBRs will be loaded at the same time as the rest of fragment
 *    info, otherwise they will be loaded lazily when some info related to MBRs
 *    is requested by the user. <br>
 *    **Default**: false
 * - `sm.partial_tile_offset_loading`
 *    **Experimental** <br>
 *    If `true` tile offsets can be partially loaded and unloaded by the
 *    readers. <br>
 *    **Default**: false
 * - `vfs.read_ahead_cache_size` <br>
 *    The the total maximum size of the read-ahead cache, which is an LRU. <br>
 *    **Default**: 10485760
 * - `vfs.min_parallel_size` <br>
 *    The minimum number of bytes in a parallel VFS operation
 *    (except parallel S3 writes, which are controlled by
 *    `vfs.s3.multipart_part_size`). <br>
 *    **Default**: 10MB
 * - `vfs.max_batch_size` <br>
 *    The maximum number of bytes in a VFS read operation<br>
 *    **Default**: UINT64_MAX
 * - `vfs.min_batch_size` <br>
 *    The minimum number of bytes in a VFS read operation<br>
 *    **Default**: 20MB
 * - `vfs.min_batch_gap` <br>
 *    The minimum number of bytes between two VFS read batches.<br>
 *    **Default**: 500KB
 * - `vfs.file.posix_file_permissions` <br>
 *    Permissions to use for posix file system with file creation.<br>
 *    **Default**: 644
 * - `vfs.file.posix_directory_permissions` <br>
 *    Permissions to use for posix file system with directory creation.<br>
 *    **Default**: 755
 * - `vfs.file.max_parallel_ops` <br>
 *    The maximum number of parallel operations on objects with `file:///`
 *    URIs. <br>
 *    **Default**: `1`
 * - `vfs.azure.storage_account_name` <br>
 *    Set the Azure Storage Account name. <br>
 *    **Default**: ""
 * - `vfs.azure.storage_account_key` <br>
 *    Set the Azure Storage Account key. <br>
 *    **Default**: ""
 * - `vfs.azure.blob_endpoint` <br>
 *    Overrides the default Azure Storage Blob endpoint. <br>
 *    **Default**: ""
 * - `vfs.azure.block_list_block_size` <br>
 *    The block size (in bytes) used in Azure blob block list writes.
 *    Any `uint64_t` value is acceptable. Note: `vfs.azure.block_list_block_size
 *    vfs.azure.max_parallel_ops` bytes will be buffered before issuing block
 *    uploads in parallel. <br>
 *    **Default**: "5242880"
 * - `vfs.azure.max_parallel_ops` <br>
 *    The maximum number of Azure backend parallel operations. <br>
 *    **Default**: `sm.io_concurrency_level`
 * - `vfs.azure.use_block_list_upload` <br>
 *    Determines if the Azure backend can use chunked block uploads. <br>
 *    **Default**: "true"
 * - `vfs.gcs.project_id` <br>
 *    Set the GCS project id. <br>
 *    **Default**: ""
 * - `vfs.gcs.multi_part_size` <br>
 *    The part size (in bytes) used in GCS multi part writes.
 *    Any `uint64_t` value is acceptable. Note:
 *    `vfs.gcs.multi_part_size * vfs.gcs.max_parallel_ops` bytes will
 *    be buffered before issuing part uploads in parallel. <br>
 *    **Default**: "5242880"
 * - `vfs.gcs.max_parallel_ops` <br>
 *    The maximum number of GCS backend parallel operations. <br>
 *    **Default**: `sm.io_concurrency_level`
 * - `vfs.gcs.use_multi_part_upload` <br>
 *    Determines if the GCS backend can use chunked part uploads. <br>
 *    **Default**: "true"
 * - `vfs.gcs.request_timeout_ms` <br>
 *    The maximum amount of time to retry network requests to GCS. <br>
 *    **Default**: "3000"
 * - `vfs.s3.region` <br>
 *    The S3 region, if S3 is enabled. <br>
 *    **Default**: us-east-1
 * - `vfs.s3.aws_access_key_id` <br>
 *    Set the AWS_ACCESS_KEY_ID <br>
 *    **Default**: ""
 * - `vfs.s3.aws_secret_access_key` <br>
 *    Set the AWS_SECRET_ACCESS_KEY <br>
 *    **Default**: ""
 * - `vfs.s3.aws_session_token` <br>
 *    Set the AWS_SESSION_TOKEN <br>
 *    **Default**: ""
 * - `vfs.s3.aws_role_arn` <br>
 *    Determines the role that we want to assume.
 *    Set the AWS_ROLE_ARN <br>
 *    **Default**: ""
 * - `vfs.s3.aws_external_id` <br>
 *    Third party access ID to your resources when assuming a role.
 *    Set the AWS_EXTERNAL_ID <br>
 *    **Default**: ""
 * - `vfs.s3.aws_load_frequency` <br>
 *    Session time limit when assuming a role.
 *    Set the AWS_LOAD_FREQUENCY <br>
 *    **Default**: ""
 * - `vfs.s3.aws_session_name` <br>
 *    (Optional) session name when assuming a role.
 *    Can be used for tracing and bookkeeping.
 *    Set the AWS_SESSION_NAME <br>
 *    **Default**: ""
 * - `vfs.s3.scheme` <br>
 *    The S3 scheme (`http` or `https`), if S3 is enabled. <br>
 *    **Default**: https
 * - `vfs.s3.endpoint_override` <br>
 *    The S3 endpoint, if S3 is enabled. <br>
 *    **Default**: ""
 * - `vfs.s3.use_virtual_addressing` <br>
 *    The S3 use of virtual addressing (`true` or `false`), if S3 is
 *    enabled. <br>
 *    **Default**: true
 * - `vfs.s3.skip_init` <br>
 *    Skip Aws::InitAPI for the S3 layer (`true` or `false`) <br>
 *    **Default**: false
 * - `vfs.s3.use_multipart_upload` <br>
 *    The S3 use of multi-part upload requests (`true` or `false`), if S3 is
 *    enabled. <br>
 *    **Default**: true
 * - `vfs.s3.max_parallel_ops` <br>
 *    The maximum number of S3 backend parallel operations. <br>
 *    **Default**: `sm.io_concurrency_level`
 * - `vfs.s3.multipart_part_size` <br>
 *    The part size (in bytes) used in S3 multipart writes.
 *    Any `uint64_t` value is acceptable. Note: `vfs.s3.multipart_part_size *
 *    vfs.s3.max_parallel_ops` bytes will be buffered before issuing multipart
 *    uploads in parallel. <br>
 *    **Default**: 5MB
 * - `vfs.s3.ca_file` <br>
 *    Path to SSL/TLS certificate file to be used by cURL for for S3 HTTPS
 *    encryption. Follows cURL conventions:
 *    https://curl.haxx.se/docs/manpage.html
 *    **Default**: ""
 * - `vfs.s3.ca_path` <br>
 *    Path to SSL/TLS certificate directory to be used by cURL for S3 HTTPS
 *    encryption. Follows cURL conventions:
 *    https://curl.haxx.se/docs/manpage.html
 *    **Default**: ""
 * - `vfs.s3.connect_timeout_ms` <br>
 *    The connection timeout in ms. Any `long` value is acceptable. <br>
 *    **Default**: 3000
 * - `vfs.s3.connect_max_tries` <br>
 *    The maximum tries for a connection. Any `long` value is acceptable. <br>
 *    **Default**: 5
 * - `vfs.s3.connect_scale_factor` <br>
 *    The scale factor for exponential backoff when connecting to S3.
 *    Any `long` value is acceptable. <br>
 *    **Default**: 25
 * - `vfs.s3.logging_level` <br>
 *    The AWS SDK logging level. This is a process-global setting. The
 *    configuration of the most recently constructed context will set
 *    process state. Log files are written to the process working directory.
 *    **Default**: ""
 * - `vfs.s3.request_timeout_ms` <br>
 *    The request timeout in ms. Any `long` value is acceptable. <br>
 *    **Default**: 3000
 * - `vfs.s3.requester_pays` <br>
 *    The requester pays for the S3 access charges. <br>
 *    **Default**: false
 * - `vfs.s3.proxy_host` <br>
 *    The S3 proxy host. <br>
 *    **Default**: ""
 * - `vfs.s3.proxy_port` <br>
 *    The S3 proxy port. <br>
 *    **Default**: 0
 * - `vfs.s3.proxy_scheme` <br>
 *    The S3 proxy scheme. <br>
 *    **Default**: "http"
 * - `vfs.s3.proxy_username` <br>
 *    The S3 proxy username. Note: this parameter is not serialized by
 *    `tiledb_config_save_to_file`. <br>
 *    **Default**: ""
 * - `vfs.s3.proxy_password` <br>
 *    The S3 proxy password. Note: this parameter is not serialized by
 *    `tiledb_config_save_to_file`. <br>
 *    **Default**: ""
 * - `vfs.s3.verify_ssl` <br>
 *    Enable HTTPS certificate verification. <br>
 *    **Default**: true""
 * - `vfs.s3.no_sign_request` <br>
 *    Make unauthenticated requests to s3. <br>
 *    **Default**: false
 * - `vfs.s3.sse` <br>
 *    The server-side encryption algorithm to use. Supported non-empty
 *    values are "aes256" and "kms" (AWS key management service). <br>
 *    **Default**: ""
 * - `vfs.s3.bucket_canned_acl` <br>
 *    Names of values found in Aws::S3::Model::BucketCannedACL enumeration.
 *    "NOT_SET"
 *    "private_"
 *    "public_read"
 *    "public_read_write"
 *    "authenticated_read"
 *    **Default**: "NOT_SET"
 * - `vfs.s3.object_canned_acl` <br>
 *    Names of values found in Aws::S3::Model::ObjectCannedACL enumeration.
 *    (The first 5 are the same as for "vfs.s3.bucket_canned_acl".)
 *    "NOT_SET"
 *    "private_"
 *    "public_read"
 *    "public_read_write"
 *    "authenticated_read"
 *    (The following three items are found only in
 *     Aws::S3::Model::ObjectCannedACL.) "aws_exec_read" "owner_read"
 *    "bucket_owner_full_control"
 *    **Default**: "NOT_SET"
 * - `vfs.hdfs.name_node_uri"` <br>
 *    Name node for HDFS. <br>
 *    **Default**: ""
 * - `vfs.hdfs.username` <br>
 *    HDFS username. <br>
 *    **Default**: ""
 * - `vfs.hdfs.kerb_ticket_cache_path` <br>
 *    HDFS kerb ticket cache path. <br>
 *    **Default**: ""
 * - `config.env_var_prefix` <br>
 *    Prefix of environmental variables for reading configuration
 *    parameters. <br>
 *    **Default**: "TILEDB_"
 * - `config.logging_level` <br>
 *    The logging level configured, possible values: "0": fatal, "1": error,
 *    "2": warn, "3": info "4": debug, "5": trace <br>
 *    **Default**: "1" if --enable-verbose bootstrap flag is provided,
 *    "0" otherwise <br>
 * - `config.logging_format` <br>
 *    The logging format configured (DEFAULT or JSON)
 *    **Default**: "DEFAULT"
 * - `rest.server_address` <br>
 *    URL for REST server to use for remote arrays. <br>
 *    **Default**: "https://api.tiledb.com"
 * - `rest.server_serialization_format` <br>
 *    Serialization format to use for remote array requests (CAPNP or
 *    JSON). <br>
 *    **Default**: "CAPNP"
 * - `rest.username` <br>
 *    Username for login to REST server. <br>
 *    **Default**: ""
 * - `rest.password` <br>
 *    Password for login to REST server. <br>
 *    **Default**: ""
 * - `rest.token` <br>
 *    Authentication token for REST server (used instead of
 *    username/password). <br>
 *    **Default**: ""
 * - `rest.ignore_ssl_validation` <br>
 *    Have curl ignore ssl peer and host validation for REST server. <br>
 *    **Default**: false
 * - `rest.creation_access_credentials_name` <br>
 *    The name of the registered access key to use for creation of the REST
 *    server. <br>
 *    **Default**: no default set
 * - `rest.retry_http_codes` <br>
 *    CSV list of http status codes to automatically retry a REST request for
 *    <br>
 *    **Default**: "503"
 * - `rest.retry_count` <br>
 *    Number of times to retry failed REST requests <br>
 *    **Default**: 3
 * - `rest.retry_initial_delay_ms` <br>
 *    Initial delay in milliseconds to wait until retrying a REST request <br>
 *    **Default**: 500
 * - `rest.retry_delay_factor` <br>
 *    The delay factor to exponentially wait until further retries of a failed
 *    REST request <br>
 *    **Default**: 1.25
 * - `rest.curl.verbose` <br>
 *    Set curl to run in verbose mode for REST requests <br>
 *    curl will print to stdout with this option
 *    **Default**: false
 * - `rest.load_metadata_on_array_open` <br>
 *    If true, array metadata will be loaded and sent to server together with
 *    the open array <br>
 *    **Default**: true
 * - `rest.load_non_empty_domain_on_array_open` <br>
 *    If true, array non empty domain will be loaded and sent to server together
 *    with the open array <br>
 *    **Default**: true
 * - `rest.use_refactored_array_open` <br>
 *    If true, the new, experimental REST routes and APIs for opening an array
 *    will be used <br>
 *    **Default**: false
 * - `rest.use_refactored_array_open_and_query_submit` <br>
 *    If true, the new, experimental REST routes and APIs for opening an array
 *    and submitting a query will be used <br>
 *    **Default**: false
 * - `rest.curl.buffer_size` <br>
 *    Set curl buffer size for REST requests <br>
 *    **Default**: 524288 (512KB)
 * - `filestore.buffer_size` <br>
 *    Specifies the size in bytes of the internal buffers used in the filestore
 *    API. The size should be bigger than the minimum tile size filestore
 *    currently supports, that is currently 1024bytes. <br>
 *    **Default**: 100MB
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_error_t* error = NULL;
 * tiledb_config_set(config, "sm.memory_budget", "1000000", &error);
 * @endcode
 *
 * @param config The config object.
 * @param param The parameter to be set.
 * @param value The value of the parameter to be set.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_set(
    tiledb_config_t* config,
    const char* param,
    const char* value,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Gets a config parameter.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* value;
 * tiledb_error_t* error = NULL;
 * tiledb_config_get(config, "sm.memory_budget", &value, &error);
 * @endcode
 *
 * @param config The config object.
 * @param param The parameter to be set.
 * @param value A pointer to the value of the parameter to be retrieved
 *    (`NULL` if it does not exist).
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_get(
    tiledb_config_t* config,
    const char* param,
    const char** value,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Unsets a config parameter. This will set the config parameter to its
 * default value.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_error_t* error = NULL;
 * tiledb_config_unset(config, "sm.memory_budget", &error);
 * @endcode
 *
 * @param config The config object.
 * @param param The parameter to be unset.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_unset(
    tiledb_config_t* config,
    const char* param,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Loads config parameters from a (local) text file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_error_t* error = NULL;
 * tiledb_config_load_from_file(config, "tiledb.conf", &error);
 * @endcode
 *
 * @param config The config object.
 * @param filename The name of the file.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_load_from_file(
    tiledb_config_t* config,
    const char* filename,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Saves the config parameters to a (local) text file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_error_t* error = NULL;
 * tiledb_config_save_to_file(config, "tiledb.conf", &error);
 * @endcode
 *
 * @param config The config object.
 * @param filename The name of the file.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_save_to_file(
    tiledb_config_t* config,
    const char* filename,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Compares 2 configurations for equality
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_error_t error;
 * uint8_t equal;
 * tiledb_config_compare(lhs, rhs, &equal);
 * @endcode
 *
 * @param lhs The left-hand side config object.
 * @param rhs The right-hand side config object.
 * @param equal Integer of equality comparison
 *      1 = true, 0 = false
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_compare(
    tiledb_config_t* lhs, tiledb_config_t* rhs, uint8_t* equal) TILEDB_NOEXCEPT;

/**
 * Creates an iterator on a config object.
 *
 * **Examples:**
 *
 * The following creates a config iterator without a prefix. This
 * will iterate over all config param/values.
 *
 * @code{.c}
 * tiledb_error_t* error = NULL;
 * tiledb_config_iter_t* config_iter;
 * tiledb_config_iter_alloc(config, NULL, &config_iter, &error);
 * @endcode
 *
 * The following creates a config iterator with a prefix. This
 * will iterate over all S3 config param/values, stripping out
 * `vfs.s3.`. For instance, instead of retrieving `vfs.s3.region`
 * as a parameter via `tiledb_config_iter_here`, it will retrieve
 * `region`.
 *
 * @code{.c}
 * tiledb_error_t* error = NULL;
 * tiledb_config_iter_t* config_iter;
 * tiledb_config_iter_alloc(config, "vfs.s3.", &config_iter, &error);
 * @endcode
 *
 * @param config A config object the iterator will operate on.
 * @param prefix If not `NULL`, only the config parameters starting
 *     with `prefix*` will be iterated on. Moreover, the prefix will
 *     be stripped from the parameters. Otherwise, all parameters will
 *     be iterated on and their full name will be retrieved.
 * @param config_iter The config iterator to be created.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_iter_alloc(
    tiledb_config_t* config,
    const char* prefix,
    tiledb_config_iter_t** config_iter,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Resets the iterator.
 *
 * **Examples:**
 *
 * Without a prefix:
 *
 * @code{.c}
 * tiledb_config_iter_reset(config, config_iter, NULL, &error);
 * @endcode
 *
 * With a prefix:
 *
 * @code{.c}
 * tiledb_config_iter_reset(config, config_iter, "vfs.s3.", &error);
 * @endcode
 *
 * @param config A config object the iterator will operate on.
 * @param config_iter The config iterator to be reset.
 * @param prefix If not `NULL`, only the config parameters starting
 *     with `prefix*` will be iterated on. Moreover, the prefix will
 *     be stripped from the parameters. Otherwise, all parameters will
 *     be iterated on and their full name will be retrieved.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_iter_reset(
    tiledb_config_t* config,
    tiledb_config_iter_t* config_iter,
    const char* prefix,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Frees a config iterator.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_iter_free(&config_iter);
 * @endcode
 *
 * @param config_iter The config iterator to be freed.
 */
TILEDB_EXPORT void tiledb_config_iter_free(tiledb_config_iter_t** config_iter)
    TILEDB_NOEXCEPT;

/**
 * Retrieves the config param and value currently pointed by the iterator.
 *
 * **Example:**
 *
 * @code{.c}
 * const char *param, *value;
 * tiledb_config_iter_here(config_iter, &param, &value, &error);
 * @endcode
 *
 * @param config_iter The config iterator.
 * @param param The config parameter to be retrieved (`NULL` if the iterator
 *     is at the end).
 * @param value The config value to be retrieved (`NULL` if the iterator
 *     is at the end).
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_iter_here(
    tiledb_config_iter_t* config_iter,
    const char** param,
    const char** value,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Moves the iterator to the next param.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_iter_next(config_iter, &error);
 * @endcode
 *
 * @param config_iter The config iterator.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_iter_next(
    tiledb_config_iter_t* config_iter, tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Checks if the iterator is done.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t done;
 * tiledb_config_iter_done(config_iter, &done, &error);
 * @endcode
 *
 * @param config_iter The config iterator.
 * @param done Sets this to `1` if the iterator is done, `0` otherwise.
 * @param error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_config_iter_done(
    tiledb_config_iter_t* config_iter,
    int32_t* done,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_CONFIG_EXTERNAL_H
