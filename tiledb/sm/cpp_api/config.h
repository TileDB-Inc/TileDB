/**
 * @file   config.h
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
 * This file declares the C++ API for the TileDB Config object.
 */

#ifndef TILEDB_CPP_API_CONFIG_H
#define TILEDB_CPP_API_CONFIG_H

#include "tiledb.h"
#include "utils.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace tiledb {

class Config;  // Forward decl for impl classes

namespace impl {

class ConfigIter {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const std::pair<std::string, std::string>;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type*;
  using reference = value_type&;
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Iterate over a config for params matching a given prefix. **/
  ConfigIter(const Config& config, std::string prefix, bool done = false)
      : prefix_(std::move(prefix))
      , done_(done) {
    init(config);
  }

  ConfigIter(const ConfigIter&) = default;
  ConfigIter(ConfigIter&&) = default;
  ConfigIter& operator=(const ConfigIter&) = default;
  ConfigIter& operator=(ConfigIter&&) = default;

  bool operator==(const ConfigIter& o) const {
    return done_ == o.done_;
  }

  bool operator!=(const ConfigIter& o) const {
    return done_ != o.done_;
  }

  const std::pair<std::string, std::string>& operator*() const {
    return here_;
  }

  const std::pair<std::string, std::string>* operator->() const {
    return &here_;
  }

  ConfigIter& operator++();

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /** Wrapper function for freeing a config iter C object. */
  static void free(tiledb_config_iter_t* config_iter) {
    tiledb_config_iter_free(&config_iter);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Prefix of parameters to match. **/
  std::string prefix_;

  /** Pointer to iter object. **/
  std::shared_ptr<tiledb_config_iter_t> iter_;

  /** Current object. **/
  std::pair<std::string, std::string> here_;

  /** If iter is done. **/
  bool done_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Init the iterator object **/
  void init(const Config& config);
};

/** Proxy to set params via operator `[]`. */
struct ConfigProxy {
  ConfigProxy(Config& conf, std::string param)
      : conf(conf)
      , param(std::move(param)) {
  }

  template <typename T>
  ConfigProxy& operator=(const T& val);

  ConfigProxy& operator=(const char* val);

  ConfigProxy& operator=(const std::string& val);

  ConfigProxy operator[](const std::string& append);

  operator std::string();

  Config& conf;
  const std::string param;
};

}  // namespace impl

/**
 * Carries configuration parameters for a context.
 *
 * **Example:**
 *
 * @code{.cpp}
 * Config conf;
 * conf["vfs.s3.region"] = "us-east-1a";
 * conf["vfs.s3.use_virtual_addressing"] = "true";
 * Context ctx(conf);
 * // array operations with ctx
 * @endcode
 * */
class Config {
 public:
  using iterator = impl::ConfigIter;
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Config() {
    create_config();
  }

  /**
   * Constructor that takes as input a filename (URI) that stores the config
   * parameters. The file must have the following (text) format:
   *
   * `{parameter} {value}`
   *
   * Anything following a `#` character is considered a comment and, thus, is
   * ignored.
   *
   * See `Config::set` for the various TileDB config parameters and allowed
   * values.
   *
   * @param filename The name of the file where the parameters will be read
   *     from.
   */
  explicit Config(const std::string& filename) {
    create_config();
    tiledb_error_t* err;
    tiledb_config_load_from_file(config_.get(), filename.c_str(), &err);
    impl::check_config_error(err);
  }

  /** Constructor from a C config object. */
  explicit Config(tiledb_config_t** config) {
    if (config) {
      config_ = std::shared_ptr<tiledb_config_t>(*config, Config::free);
      *config = nullptr;
    }
  }

  /**
   * Constructor that takes as input a STL map that stores the config parameters
   *
   * @param config
   */
  explicit Config(const std::map<std::string, std::string>& config) {
    create_config();
    for (const auto& kv : config) {
      set(kv.first, kv.second);
    }
  }

  /**
   * Constructor that takes as input a STL unordered_map that stores the config
   * parameters
   *
   * @param config
   */
  explicit Config(const std::unordered_map<std::string, std::string>& config) {
    create_config();
    for (const auto& kv : config) {
      set(kv.first, kv.second);
    }
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Saves the config parameters to a (local) text file. */
  void save_to_file(const std::string filename) {
    tiledb_error_t* err;
    tiledb_config_save_to_file(config_.get(), filename.c_str(), &err);
    impl::check_config_error(err);
  }

  /** Compares configs for equality. */
  bool operator==(const Config& rhs) const {
    uint8_t equal;
    int rc = tiledb_config_compare(config_.get(), rhs.config_.get(), &equal);
    if (rc != TILEDB_OK)
      throw std::runtime_error(
          "[TileDB::C++API] Error: Failed to compare configurations");
    if (equal == 1)
      return true;
    else
      return false;
  }

  /** Compares configs for inequality. */
  bool operator!=(const Config& rhs) const {
    return !(*this == rhs);
  }

  /** Returns the pointer to the TileDB C config object. */
  std::shared_ptr<tiledb_config_t> ptr() const {
    return config_;
  }

  /** Sets a config parameter.
   * - `sm.allow_separate_attribute_writes` <br>
   *    **Experimental** <br>
   *    Allow separate attribute write queries.<br>
   *    **Default**: false
   * - `sm.allow_updates_experimental` <br>
   *    **Experimental** <br>
   *    Allow update queries. Experimental for testing purposes, do not use.<br>
   *    **Default**: false
   * - `sm.dedup_coords` <br>
   *    If `true`, cells with duplicate coordinates will be removed during
   *    sparse fragment writes. Note that ties during deduplication are broken
   *    arbitrarily. Also note that this check means that it will take longer to
   *    perform the write operation. <br>
   *    **Default**: false
   * - `sm.check_coord_dups` <br>
   *    This is applicable only if `sm.dedup_coords` is `false`.
   *    If `true`, an error will be thrown if there are cells with duplicate
   *    coordinates during sparse fragmnet writes. If `false` and there are
   *    duplicates, the duplicates will be written without errors. Note that
   *    this check is much ligher weight than the coordinate deduplication check
   *    enabled by `sm.dedup_coords`. <br>
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
   * - `sm.merge_overlapping_ranges_experimental` <br>
   *    If `true`, merge overlapping Subarray ranges. Else, overlapping ranges
   *    will not be merged and multiplicities will be returned.
   *    Experimental for testing purposes, do not use.<br>
   *    **Default**: true
   * - `sm.enable_signal_handlers` <br>
   *    Determines whether or not TileDB will install signal handlers. <br>
   *    **Default**: true
   * - `sm.compute_concurrency_level` <br>
   *    Upper-bound on number of threads to allocate for compute-bound tasks.
   *    <br>
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
   *    **Deprecated**
   *    The size (in bytes) of the attribute buffers used during
   *    consolidation. <br>
   *    **Default**: 50,000,000
   * - `sm.consolidation.max_fragment_size` <br>
   *    **Experimental** <br>
   *    The size (in bytes) of the maximum on-disk fragment size that will be
   *    created by consolidation. When it is reached, consolidation will
   *    continue the operation in a new fragment. The result will be a multiple
   *    fragments, but with seperate MBRs. <br>
   * - `sm.consolidation.steps` <br>
   *    The number of consolidation steps to be performed when executing
   *    the consolidation algorithm.<br>
   *    **Default**: UINT32_MAX
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
   * - `sm.encryption_key` <br>
   *    The key for encrypted arrays. <br>
   *    **Default**: ""
   * - `sm.encryption_type` <br>
   *    The type of encryption used for encrypted arrays. <br>
   *    **Default**: "NO_ENCRYPTION"
   * - `sm.enumerations_max_size` <br>
   *    Maximum in memory size for an enumeration. If the enumeration is <br>
   *    var sized, the size will include the data and the offsets. <br>
   *    **Default**: 10MB
   * - `sm.enumerations_max_total_size` <br>
   *    Maximum in memory size for all enumerations. If the enumeration <br>
   *    is var sized, the size will include the data and the offsets. <br>
   *    **Default**: 50MB
   * - `sm.max_tile_overlap_size` <br>
   *    Maximum size for the tile overlap structure which holds <br>
   *    information about which tiles are covered by ranges. Only used <br>
   *    in dense reads and legacy reads. <br>
   *    **Default**: 300MB
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
   * - `sm.query.dense.qc_coords_mode` <br>
   *    Dense configuration that allows to only return the coordinates of <br>
   *    the cells that match a query condition without any attribute data. <br>
   *    **Default**: "false"
   * - `sm.query.sparse_global_order.reader` <br>
   *    Which reader to use for sparse global order queries. "refactored"
   *    or "legacy".<br>
   *    **Default**: refactored
   * - `sm.query.sparse_global_order.preprocess_tile_merge` <br>
   *    **Experimental for testing purposes, do not use.**<br>
   *    Performance configuration for sparse global order read queries.
   *    If nonzero, prior to loading the first tiles, the reader will run
   *    a preprocessing step to arrange tiles from all fragments in a single
   *    globally ordered list. This is expected to improve performance when
   *    there are many fragments or when the distribution in space of the
   *    tiles amongst the fragments is skewed. The value of the parameter
   *    specifies the amount of work per parallel task.
   *    **Default**: "32768"
   * - `sm.query.sparse_unordered_with_dups.reader` <br>
   *    Which reader to use for sparse unordered with dups queries.
   *    "refactored" or "legacy".<br>
   *    **Default**: refactored
   * - `sm.skip_checksum_validation` <br>
   *    Skip checksum validation on reads for the md5 and sha256 filters. <br>
   *    **Default**: "false"
   * - `sm.mem.malloc_trim` <br>
   *    Should malloc_trim be called on context and query destruction? This
   *    might reduce residual memory usage. <br>
   *    **Default**: true
   * - `sm.mem.tile_upper_memory_limit` <br>
   *    **Experimental** <br>
   *    This is the upper memory limit that is used when loading tiles. For now
   *    it is only used in the dense reader but will be eventually used by all
   *    readers. The readers using this value will use it as a way to limit the
   *    amount of tile data that is brought into memory at once so that we don't
   *    incur performance penalties during memory movement operations. It is a
   *    soft limit that we might go over if a single tile doesn't fit into
   *    memory, we will allow to load that tile if it still fits within
   *    `sm.mem.total_budget`. <br>
   *    **Default**: 1GB
   * - `sm.mem.total_budget` <br>
   *    Memory budget for readers and writers. <br>
   *    **Default**: 10GB
   * - `sm.mem.consolidation.buffers_weight` <br>
   *    Weight used to split `sm.mem.total_budget` and assign to the
   *    consolidation buffers. The budget is split across 3 values,
   *    `sm.mem.consolidation.buffers_weight`,
   *    `sm.mem.consolidation.reader_weight` and
   *    `sm.mem.consolidation.writer_weight`. <br>
   *    **Default**: 1
   * - `sm.mem.consolidation.reader_weight` <br>
   *    Weight used to split `sm.mem.total_budget` and assign to the
   *    reader query. The budget is split across 3 values,
   *    `sm.mem.consolidation.buffers_weight`,
   *    `sm.mem.consolidation.reader_weight` and
   *    `sm.mem.consolidation.writer_weight`. <br>
   *    **Default**: 3
   * - `sm.mem.consolidation.writer_weight` <br>
   *    Weight used to split `sm.mem.total_budget` and assign to the
   *    writer query. The budget is split across 3 values,
   *    `sm.mem.consolidation.buffers_weight`,
   *    `sm.mem.consolidation.reader_weight` and
   *    `sm.mem.consolidation.writer_weight`. <br>
   *    **Default**: 2
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
   *    info, otherwise they will be loaded lazily when some info related to
   *    MBRs is requested by the user. <br>
   *    **Default**: false
   * - `sm.partial_tile_offset_loading`
   *    **Experimental** <br>
   *    If `true` tile offsets can be partially loaded and unloaded by the
   *    readers. <br>
   *    **Default**: false
   * - `ssl.ca_file` <br>
   *    The path to CA certificate to use when validating server certificates.
   *    Applies to all SSL/TLS connections. <br>
   *    This option might be ignored on platforms that have native certificate
   *    stores like Windows. <br>
   *    **Default**: ""
   * - `ssl.ca_path` <br>
   *    The path to a directory with CA certificates to use when validating
   *    server certificates. Applies to all SSL/TLS connections. <br>
   *    This option might be ignored on platforms that have native certificate
   *    stores like Windows. <br>
   *    **Default**: ""
   * - `ssl.verify` <br>
   *    Whether to verify the server's certificate. Applies to all SSL/TLS
   *    connections. <br>
   *    Disabling verification is insecure and should only used for testing
   *    purposes. <br>
   *    **Default**: true
   * - `vfs.read_ahead_cache_size` <br>
   *    The the total maximum size of the read-ahead cache, which is an LRU.
   *    <br>
   *    **Default**: 10485760
   * -  `vfs.log_operations` <br>
   *    Enables logging all VFS operations in trace mode. <br>
   *    **Default**: false
   * - `vfs.min_parallel_size` <br>
   *    The minimum number of bytes in a parallel VFS operation
   *    (except parallel S3 writes, which are controlled by
   *    `vfs.s3.multipart_part_size`). <br>
   *    **Default**: 10MB
   * - `vfs.max_batch_size` <br>
   *    The maximum number of bytes in a VFS read operation<br>
   *    **Default**: 100MB
   * - `vfs.min_batch_size` <br>
   *    The minimum number of bytes in a VFS read operation<br>
   *    **Default**: 20MB
   * - `vfs.min_batch_gap` <br>
   *    The minimum number of bytes between two VFS read batches.<br>
   *    **Default**: 500KB
   * - `vfs.read_logging_mode` <br>
   *    Log read operations at varying levels of verbosity.<br>
   *   **Default: ""**
   *    Possible values:<br>
   *    <ul>
   *     <li><pre>""</pre> An empty string disables read logging.</li>
   *     <li><pre>"fragments"</pre> Log each fragment read.</li>
   *     <li><pre>"fragment_files"</pre> Log each individual fragment file
   *         read.</li>
   *     <li><pre>"all_files"</pre> Log all files read.</li>
   *     <li><pre>"all_reads"</pre> Log all files with offset and length
   *         parameters.</li>
   *     <li><pre>"all_reads_always"</pre> Log all files with offset and length
   *         parameters on every read, not just the first read. On large arrays
   *         the read cache may get large so this trades of RAM usage vs
   *         increased log verbosity.</li>
   *   </ul>
   * - `vfs.file.posix_file_permissions` <br>
   *    Permissions to use for posix file system with file creation.<br>
   *    **Default**: 644
   * - `vfs.file.posix_directory_permissions` <br>
   *    Permissions to use for posix file system with directory creation.<br>
   *    **Default**: 755
   * - `vfs.azure.storage_account_name` <br>
   *    Set the name of the Azure Storage account to use. <br>
   *    **Default**: ""
   * - `vfs.azure.storage_account_key` <br>
   *    Set the Shared Key to authenticate to Azure Storage. <br>
   *    **Default**: ""
   * - `vfs.azure.storage_sas_token` <br>
   *    Set the Azure Storage SAS (shared access signature) token to use.
   *    If this option is set along with `vfs.azure.blob_endpoint`, the
   *    latter must not include a SAS token. <br>
   *    **Default**: ""
   * - `vfs.azure.blob_endpoint` <br>
   *    Set the default Azure Storage Blob endpoint. <br>
   *    If not specified, it will take a value of
   *    `https://<account-name>.blob.core.windows.net`, where `<account-name>`
   *    is the value of the `vfs.azure.storage_account_name` option. This means
   *    that at least one of these two options must be set (or both if shared
   *    key authentication is used). <br>
   *    **Default**: ""
   * - `vfs.azure.block_list_block_size` <br>
   *    The block size (in bytes) used in Azure blob block list writes.
   *    Any `uint64_t` value is acceptable. Note:
   *    `vfs.azure.block_list_block_size vfs.azure.max_parallel_ops` bytes will
   *    be buffered before issuing block uploads in parallel. <br>
   *    **Default**: "5242880"
   * - `vfs.azure.max_parallel_ops` <br>
   *    The maximum number of Azure backend parallel operations. <br>
   *    **Default**: `sm.io_concurrency_level`
   * - `vfs.azure.use_block_list_upload` <br>
   *    Determines if the Azure backend can use chunked block uploads. <br>
   *    **Default**: "true"
   * - `vfs.azure.max_retries` <br>
   *    The maximum number of times to retry an Azure network request. <br>
   *    **Default**: 5
   * -  `vfs.azure.retry_delay_ms` <br>
   *    The minimum permissible delay between Azure netwwork request retry
   *    attempts, in milliseconds.
   *    **Default**: 800
   * -  `vfs.azure.max_retry_delay_ms` <br>
   *    The maximum permissible delay between Azure netwwork request retry
   *    attempts, in milliseconds.
   *    **Default**: 60000
   * - `vfs.gcs.endpoint` <br>
   *    The GCS endpoint. <br>
   *    **Default**: ""
   * - `vfs.gcs.project_id` <br>
   *    Set the GCS project ID to create new buckets to. Not required unless you
   *    are going to use the VFS to create buckets. <br>
   *    **Default**: ""
   * - `vfs.gcs.service_account_key` <br>
   *    **Experimental** <br>
   *    Set the JSON string with GCS service account key. Takes precedence
   *    over `vfs.gcs.workload_identity_configuration` if both are specified. If
   *    neither is specified, Application Default Credentials will be used. <br>
   *    **Default**: ""
   * - `vfs.gcs.workload_identity_configuration` <br>
   *    **Experimental** <br>
   *    Set the JSON string with Workload Identity Federation configuration.
   *    `vfs.gcs.service_account_key` takes precedence over this if both are
   *    specified. If neither is specified, Application Default Credentials will
   *    be used. <br>
   *    **Default**: ""
   * - `vfs.gcs.impersonate_service_account` <br>
   *    **Experimental** <br>
   *    Set the GCS service account to impersonate. A chain of impersonated
   *    accounts can be formed by specifying many service accounts, separated by
   *    a comma. <br>
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
   * - `vfs.gcs.max_direct_upload_size` <br>
   *    The maximum size in bytes of a direct upload to GCS. Ignored
   *    if `vfs.gcs.use_multi_part_upload` is set to true. <br>
   *    **Default**: "10737418240"
   * - `vfs.s3.region` <br>
   *    The S3 region, if S3 is enabled. <br>
   *    If empty, the region will be determined by the AWS SDK using sources
   *    such as environment variables, profile configuration, or instance
   *    metadata. <br>
   *    **Default**: ""
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
   *    **Default**: 10800
   * - `vfs.s3.connect_max_tries` <br>
   *    The maximum tries for a connection. Any `long` value is acceptable. <br>
   *    **Default**: 5
   * - `vfs.s3.connect_scale_factor` <br>
   *    The scale factor for exponential backoff when connecting to S3.
   *    Any `long` value is acceptable. <br>
   *    **Default**: 25
   * - `vfs.s3.custom_headers.*` <br>
   *    (Optional) Prefix for custom headers on s3 requests. For each custom
   *    header, use "vfs.s3.custom_headers.header_key" = "header_value" <br>
   *    **Optional. No Default**
   * - `vfs.s3.logging_level` <br>
   *    The AWS SDK logging level. This is a process-global setting. The
   *    configuration of the most recently constructed context will set
   *    process state. Log files are written to the process working directory.
   *    **Default**: "Off"
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
   * - `vfs.s3.sse_kms_key_id` <br>
   *    The server-side encryption key to use if
   *    vfs.s3.sse == "kms" (AWS key management service). <br>
   *    **Default**: ""
   * - `vfs.s3.storage_class` <br>
   *    The storage class to use for the newly uploaded S3 objects. The set of
   *    accepted values is found in the Aws::S3::Model::StorageClass
   *    enumeration.
   *    "NOT_SET"
   *    "STANDARD"
   *    "REDUCED_REDUNDANCY"
   *    "STANDARD_IA"
   *    "ONEZONE_IA"
   *    "INTELLIGENT_TIERING"
   *    "GLACIER"
   *    "DEEP_ARCHIVE"
   *    "OUTPOSTS"
   *    "GLACIER_IR"
   *    "SNOW"
   *    "EXPRESS_ONEZONE" <br>
   *    **Default**: "NOT_SET"
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
   * - `vfs.s3.config_source` <br>
   *    Force S3 SDK to only load config options from a set source.
   *    The supported options are
   *    `auto` (TileDB config options are considered first,
   *    then SDK-defined precedence: env vars, config files, ec2 metadata),
   *    `config_files` (forces SDK to only consider options found in aws
   *    config files),
   *    `sts_profile_with_web_identity` (force SDK to consider assume roles/sts
   *    from config files with support for web tokens, commonly used by
   *    EKS/ECS).
   *    **Default**: auto
   *    <br>
   * - `vfs.s3.install_sigpipe_handler` <br>
   *    When set to `true`, the S3 SDK uses a handler that ignores SIGPIPE
   *    signals.
   *    **Default**: "true"
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
   * - `profile_name` <br>
   *    The name of the Profile to be used for REST configuration. <br>
   *    **Default**: ""
   * - `profile_dir` <br>
   *    The directory where the user profiles are stored. <br>
   *    **Default**: ""
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
   * - `rest.resubmit_incomplete` <br>
   *    If true, incomplete queries received from server are automatically
   *    resubmitted before returning to user control. <br>
   *    **Default**: "true"
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
   *    **Default**: 25
   * - `rest.retry_initial_delay_ms` <br>
   *    Initial delay in milliseconds to wait until retrying a REST request <br>
   *    **Default**: 500
   * - `rest.retry_delay_factor` <br>
   *    The delay factor to exponentially wait until further retries of a failed
   *    REST request <br>
   *    **Default**: 1.25
   * - `rest.curl.retry_errors` <br>
   *    If true any curl requests that returned an error will be retried <br>
   *    **Default**: true
   * - `rest.curl.verbose` <br>
   *    Set curl to run in verbose mode for REST requests <br>
   *    curl will print to stdout with this option
   *    **Default**: false
   * -  `rest.curl.tcp_keepalive` <br>
   *    Set curl to use TCP keepalive for REST requests <br>
   *    **Default**: true
   * - `rest.load_metadata_on_array_open` <br>
   *    If true, array metadata will be loaded and sent to server together with
   *    the open array <br>
   *    **Default**: true
   * - `rest.load_non_empty_domain_on_array_open` <br>
   *    If true, array non empty domain will be loaded and sent to server
   *    together with the open array <br>
   *    **Default**: true
   * - `rest.load_enumerations_on_array_open` <br>
   *    If true, enumerations will be loaded for the latest array schema and
   *    sent to server together with the open array.
   *    **Default**: false
   * - `rest.load_enumerations_on_array_open_all_schemas` <br>
   *    If true, enumerations will be loaded for all array schemas and sent to
   *    server together with the open array.
   *    **Default**: false
   * - `rest.use_refactored_array_open` <br>
   *    If true, the new REST routes and APIs for opening an array will be used
   *    <br>
   *    **Default**: true
   * - `rest.use_refactored_array_open_and_query_submit` <br>
   *    If true, the new REST routes and APIs for opening an array and
   *    submitting a query will be used <br>
   *    **Default**: true
   * - `rest.curl.buffer_size` <br>
   *    Set curl buffer size for REST requests <br>
   *    **Default**: 524288 (512KB)
   * - `rest.capnp_traversal_limit` <br>
   *    CAPNP traversal limit used in the deserialization of messages(bytes)
   *    <br>
   *    **Default**: 2147483648 (2GB)
   * - `rest.custom_headers.*` <br>
   *    (Optional) Prefix for custom headers on REST requests. For each custom
   *    header, use "rest.custom_headers.header_key" = "header_value" <br>
   *    **Optional. No Default**
   * - `rest.payer_namespace` <br>
   *    The namespace that should be charged for the request. <br>
   *    **Default**: no default set
   * - `filestore.buffer_size` <br>
   *    Specifies the size in bytes of the internal buffers used in the
   *    filestore API. The size should be bigger than the minimum tile size
   *    filestore currently supports, that is currently 1024bytes. <br>
   *    **Default**: 100MB
   */
  Config& set(const std::string& param, const std::string& value) {
    tiledb_error_t* err;
    tiledb_config_set(config_.get(), param.c_str(), value.c_str(), &err);
    impl::check_config_error(err);
    return *this;
  }

  /**
   * Get a parameter from the configuration by key.
   * @param param Name of configuration parameter
   * @return Value of configuration parameter
   * @throws TileDBError if the parameter does not exist
   */
  std::string get(const std::string& param) const {
    const char* val;
    tiledb_error_t* err;
    tiledb_config_get(config_.get(), param.c_str(), &val, &err);
    impl::check_config_error(err);

    if (val == nullptr)
      throw TileDBError("Config Error: Invalid parameter '" + param + "'");

    return val;
  }

  /**
   * Check if a configuration parameter exists.
   * @param param Name of configuration parameter
   * @return true if the parameter exists, false otherwise
   */
  bool contains(const std::string_view& param) const {
    const char* val;
    tiledb_error_t* err;
    tiledb_config_get(config_.get(), param.data(), &val, &err);

    return val != nullptr;
  }

  /**
   * Operator that enables setting parameters with `[]`.
   *
   * **Example:**
   *
   * @code{.cpp}
   * Config conf;
   * conf["vfs.s3.region"] = "us-east-1a";
   * conf["vfs.s3.use_virtual_addressing"] = "true";
   * Context ctx(conf);
   * @endcode
   *
   * @param param Name of parameter to set
   * @return "Proxy" object supporting assignment.
   */
  impl::ConfigProxy operator[](const std::string& param);

  /**
   * Resets a config parameter to its default value.
   *
   * @param param Name of parameter
   * @return Reference to this Config instance
   */
  Config& unset(const std::string& param) {
    tiledb_error_t* err;
    tiledb_config_unset(config_.get(), param.c_str(), &err);
    impl::check_config_error(err);

    return *this;
  }

  /**
   * Iterate over params starting with a prefix.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Config config;
   * for (auto it = config.begin("vfs"), ite = config.end(); it != ite; ++it) {
   *   std::string name = it->first, value = it->second;
   * }
   * @endcode
   *
   * @param prefix Prefix to iterate over
   * @return iterator
   */
  iterator begin(const std::string& prefix) {
    return iterator{*this, prefix, false};
  }

  /**
   * Iterate over all params.
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Config config;
   * for (auto it = config.begin(), ite = config.end(); it != ite; ++it) {
   *   std::string name = it->first, value = it->second;
   * }
   * @endcode
   *
   * @return iterator
   */
  iterator begin() {
    return iterator{*this, "", false};
  }

  /** End iterator. **/
  iterator end() {
    return iterator{*this, "", true};
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /** Wrapper function for freeing a config C object. */
  static void free(tiledb_config_t* config) {
    tiledb_config_free(&config);
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB C config object. */
  std::shared_ptr<tiledb_config_t> config_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Creates the TileDB C config object. */
  void create_config() {
    tiledb_config_t* config;
    tiledb_error_t* err;
    tiledb_config_alloc(&config, &err);
    impl::check_config_error(err);

    config_ = std::shared_ptr<tiledb_config_t>(config, Config::free);
  }
};

/* ********************************* */
/*            DEFINITIONS            */
/* ********************************* */

inline impl::ConfigProxy Config::operator[](const std::string& param) {
  return {*this, param};
}

namespace impl {

template <typename T>
inline ConfigProxy& impl::ConfigProxy::operator=(const T& val) {
  conf.set(param, std::to_string(val));
  return *this;
}

inline ConfigProxy& impl::ConfigProxy::operator=(const char* val) {
  conf.set(param, std::string(val));
  return *this;
}

inline ConfigProxy& impl::ConfigProxy::operator=(const std::string& val) {
  conf.set(param, val);
  return *this;
}

inline ConfigProxy impl::ConfigProxy::operator[](const std::string& append) {
  return {conf, param + append};
}

inline ConfigProxy::operator std::string() {
  return conf.get(param);
}

inline void ConfigIter::init(const Config& config) {
  tiledb_config_iter_t* iter;
  tiledb_error_t* err;
  const char* p = prefix_.size() ? prefix_.c_str() : nullptr;
  tiledb_config_iter_alloc(config.ptr().get(), p, &iter, &err);
  check_config_error(err);

  iter_ = std::shared_ptr<tiledb_config_iter_t>(iter, ConfigIter::free);

  // Get first param-value pair
  int done;
  tiledb_config_iter_done(iter_.get(), &done, &err);
  check_config_error(err);
  if (done == 1) {
    done_ = true;
  } else {
    const char *param, *value;
    tiledb_config_iter_here(iter_.get(), &param, &value, &err);
    check_config_error(err);
    here_ = std::pair<std::string, std::string>(param, value);
  }
}

inline ConfigIter& ConfigIter::operator++() {
  if (done_)
    return *this;
  int done;
  tiledb_error_t* err;

  tiledb_config_iter_next(iter_.get(), &err);
  check_config_error(err);

  tiledb_config_iter_done(iter_.get(), &done, &err);
  check_config_error(err);
  if (done == 1) {
    done_ = true;
    return *this;
  }

  const char *param, *value;
  tiledb_config_iter_here(iter_.get(), &param, &value, &err);
  check_config_error(err);
  here_ = std::pair<std::string, std::string>(param, value);
  return *this;
}

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_CPP_API_CONFIG_H
