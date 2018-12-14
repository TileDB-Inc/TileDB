/**
 * @file   config.h
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
 * This file defines class Config.
 */

#ifndef TILEDB_CONFIG_H
#define TILEDB_CONFIG_H

#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"

#include <map>
#include <set>
#include <thread>

namespace tiledb {
namespace sm {

/** This class manages the TileDB configuration options. */
class Config {
 public:
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /** Storage manager parameters. */
  struct SMParams {
    uint64_t array_schema_cache_size_;
    uint64_t fragment_metadata_cache_size_;
    bool enable_signal_handlers_;
    uint64_t num_async_threads_;
    uint64_t num_reader_threads_;
    uint64_t num_writer_threads_;
    int num_tbb_threads_;
    uint64_t tile_cache_size_;
    bool dedup_coords_;
    bool check_coord_dups_;
    bool check_coord_oob_;
    bool check_global_order_;

    SMParams() {
      array_schema_cache_size_ = constants::array_schema_cache_size;
      fragment_metadata_cache_size_ = constants::fragment_metadata_cache_size;
      enable_signal_handlers_ = constants::enable_signal_handlers;
      num_async_threads_ = constants::num_async_threads;
      num_reader_threads_ = constants::num_reader_threads;
      num_writer_threads_ = constants::num_writer_threads;
      num_tbb_threads_ = constants::num_tbb_threads;
      tile_cache_size_ = constants::tile_cache_size;
      dedup_coords_ = false;
      check_coord_dups_ = true;
      check_coord_oob_ = true;
      check_global_order_ = true;
    }
  };

  struct S3Params {
    std::string region_;
    std::string scheme_;
    std::string endpoint_override_;
    bool use_virtual_addressing_;
    uint64_t max_parallel_ops_;
    uint64_t multipart_part_size_;
    long connect_timeout_ms_;
    long connect_max_tries_;
    long connect_scale_factor_;
    long request_timeout_ms_;
    std::string proxy_scheme_;
    std::string proxy_host_;
    unsigned proxy_port_;
    std::string proxy_username_;
    std::string proxy_password_;
    std::string aws_access_key_id;
    std::string aws_secret_access_key;

    S3Params() {
      region_ = constants::s3_region;
      scheme_ = constants::s3_scheme;
      endpoint_override_ = constants::s3_endpoint_override;
      use_virtual_addressing_ = constants::s3_use_virtual_addressing;
      max_parallel_ops_ = constants::s3_max_parallel_ops;
      multipart_part_size_ = constants::s3_multipart_part_size;
      connect_timeout_ms_ = constants::s3_connect_timeout_ms;
      connect_max_tries_ = constants::s3_connect_max_tries;
      connect_scale_factor_ = constants::s3_connect_scale_factor;
      request_timeout_ms_ = constants::s3_request_timeout_ms;
      proxy_scheme_ = constants::s3_proxy_scheme;
      proxy_host_ = constants::s3_proxy_host;
      proxy_port_ = constants::s3_proxy_port;
      proxy_username_ = constants::s3_proxy_username;
      proxy_password_ = constants::s3_proxy_password;
      aws_access_key_id = constants::aws_access_key_id;
      aws_secret_access_key = constants::aws_secret_access_key;
    }
  };

  struct HDFSParams {
    std::string name_node_uri_;
    std::string username_;
    std::string kerb_ticket_cache_path_;

    HDFSParams() {
      kerb_ticket_cache_path_ = constants::hdfs_kerb_ticket_cache_path;
      name_node_uri_ = constants::hdfs_name_node_uri;
      username_ = constants::hdfs_username;
    }
  };

  struct FileParams {
    uint64_t max_parallel_ops_;

    FileParams() {
      max_parallel_ops_ = constants::vfs_file_max_parallel_ops;
    }
  };

  struct VFSParams {
    S3Params s3_params_;
    HDFSParams hdfs_params_;
    FileParams file_params_;
    uint64_t num_threads_;
    uint64_t min_parallel_size_;
    uint64_t max_batch_read_size_;
    float max_batch_read_amplification_;

    VFSParams() {
      num_threads_ = constants::vfs_num_threads;
      min_parallel_size_ = constants::vfs_min_parallel_size;
      max_batch_read_size_ = constants::vfs_max_batch_read_size;
      max_batch_read_amplification_ =
          constants::vfs_max_batch_read_amplification;
    }
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Config();

  /** Constructor. */
  Config(const VFSParams& vfs_params);

  /** Destructor. */
  ~Config();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Loads the config parameters from a configuration (local) file. */
  Status load_from_file(const std::string& filename);

  /** Saves the config parameters to a configuration (local) file. */
  Status save_to_file(const std::string& filename);

  /** Returns the storage manager parameters. */
  SMParams sm_params() const;

  /** Returns the VFS parameters. */
  VFSParams vfs_params() const;

  /** Returns the S3 parameters. */
  S3Params s3_params() const;

  /** Sets a config parameter.
   *
   * **Parameters**
   *
   * - `sm.dedup_coords` <br>
   *    If `true`, cells with duplicate coordinates will be removed during
   *    sparse array writes. Note that ties during deduplication are
   *    arbitrary. <br>
   *    **Default**: false
   * - `sm.check_coord_dups` <br>
   *    This is applicable only if `sm.dedup_coords` is `false`.
   *    If `true`, an error will be thrown if there are cells with duplicate
   *    coordinates during sparse array writes. If `false` and there are
   *    duplicates, the duplicates will be written without errors, but the
   *    TileDB behavior could be unpredictable. <br>
   *    **Default**: true
   * - `sm.check_coord_oob` <br>
   *    If `true`, an error will be thrown if there are cells with coordinates
   *    falling outside the array domain during sparse array writes. <br>
   *    **Default**: true
   * - `sm.check_global_order` <br>
   *    Checks if the coordinates obey the global array order. Applicable only
   *    to sparse writes in global order.
   *    **Default**: true
   * - `sm.tile_cache_size` <br>
   *    The tile cache size in bytes. Any `uint64_t` value is acceptable. <br>
   *    **Default**: 10,000,000
   * - `sm.array_schema_cache_size` <br>
   *    Array schema cache size in bytes. Any `uint64_t` value is acceptable.
   * <br>
   *    **Default**: 10,000,000
   * - `sm.fragment_metadata_cache_size` <br>
   *    The fragment metadata cache size in bytes. Any `uint64_t` value is
   *    acceptable. <br>
   *    **Default**: 10,000,000
   * - `sm.enable_signal_handlers` <br>
   *    Whether or not TileDB will install signal handlers. <br>
   *    **Default**: true
   * - `sm.num_async_threads` <br>
   *    The number of threads allocated for async queries. <br>
   *    **Default**: 1
   * - `sm.num_reader_threads` <br>
   *    The number of threads allocated for issuing reads to VFS in
   *    parallel. <br>
   *    **Default**: 1
   * - `sm.num_writer_threads` <br>
   *    The number of threads allocated for issuing writes to VFS in
   *    parallel.<br>
   *    **Default**: 1
   * - `sm.num_tbb_threads` <br>
   *    The number of threads allocated for the TBB thread pool (if TBB is
   *    enabled). Note: this is a whole-program setting. Usually this should not
   *    be modified from the default. See also the documentation for TBB's
   *    `task_scheduler_init` class.<br>
   *    **Default**: TBB automatic
   * - `vfs.num_threads` <br>
   *    The number of threads allocated for VFS operations (any backend), per
   *    VFS instance. <br>
   *    **Default**: number of cores
   * - `vfs.min_parallel_size` <br>
   *    The minimum number of bytes in a parallel VFS operation
   *    (except parallel S3 writes, which are controlled by
   *    `vfs.s3.multipart_part_size`.) <br>
   *    **Default**: 10MB
   * - `vfs.max_batch_read_size` <br>
   *    The maximum number of bytes in a batched VFS read operation. <br>
   *    **Default**: 100MB
   * - `vfs.max_batch_read_amplification` <br>
   *    The maximum amplification factor (>= 1.0) of batched VFS read
   *    operations. <br>
   *    **Default**: 1.0
   * - `vfs.file.max_parallel_ops` <br>
   *    The maximum number of parallel operations on objects with `file:///`
   *    URIs. <br>
   *    **Default**: `vfs.num_threads`
   * - `vfs.s3.region` <br>
   *    The S3 region, if S3 is enabled. <br>
   *    **Default**: us-east-1
   * - `vfs.s3.aws_access_key_id` <br>
   *    Set the AWS_ACCESS_KEY_ID <br>
   *    **Default**: ""
   * - `vfs.s3.aws_secret_access_key` <br>
   *    Set the AWS_SECRET_ACCESS_KEY <br>
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
   * - `vfs.s3.max_parallel_ops` <br>
   *    The maximum number of S3 backend parallel operations. <br>
   *    **Default**: `vfs.num_threads`
   * - `vfs.s3.multipart_part_size` <br>
   *    The part size (in bytes) used in S3 multipart writes.
   *    Any `uint64_t` value is acceptable. Note: `vfs.s3.multipart_part_size *
   *    vfs.s3.max_parallel_ops` bytes will be buffered before issuing multipart
   *    uploads in parallel. <br>
   *    **Default**: 5MB
   * - `vfs.s3.connect_timeout_ms` <br>
   *    The connection timeout in ms. Any `long` value is acceptable. <br>
   *    **Default**: 3000
   * - `vfs.s3.connect_max_tries` <br>
   *    The maximum tries for a connection. Any `long` value is acceptable. <br>
   *    **Default**: 5
   * - `vfs.s3.connect_scale_factor` <br>
   *    The scale factor for exponential backofff when connecting to S3.
   *    Any `long` value is acceptable. <br>
   *    **Default**: 25
   * - `vfs.s3.request_timeout_ms` <br>
   *    The request timeout in ms. Any `long` value is acceptable. <br>
   *    **Default**: 3000
   * - `vfs.s3.proxy_host` <br>
   *    The proxy host. <br>
   *    **Default**: ""
   * - `vfs.s3.proxy_port` <br>
   *    The proxy port. <br>
   *    **Default**: 0
   * - `vfs.s3.proxy_scheme` <br>
   *    The proxy scheme. <br>
   *    **Default**: "https"
   * - `vfs.s3.proxy_username` <br>
   *    The proxy username. Note: this parameter is not serialized by
   *    `tiledb_config_save_to_file`. <br>
   *    **Default**: ""
   * - `vfs.s3.proxy_password` <br>
   *    The proxy password. Note: this parameter is not serialized by
   *    `tiledb_config_save_to_file`. <br>
   *    **Default**: ""
   * - `vfs.hdfs.name_node"` <br>
   *    Name node for HDFS. <br>
   *    **Default**: ""
   * - `vfs.hdfs.username` <br>
   *    HDFS username. <br>
   *    **Default**: ""
   * - `vfs.hdfs.kerb_ticket_cache_path` <br>
   *    HDFS kerb ticket cache path. <br>
   *    **Default**: ""
   */
  Status set(const std::string& param, const std::string& value);

  /** Gets a config parameter value (`nullptr` if `param` does not exist). */
  Status get(const std::string& param, const char** value) const;

  /**
   * Returns a map with the param-value pairs in the config.
   *
   * @param prefix If equal to an empty string, all param-value pairs are
   *     retrieved. Otherwise, only those that start with `prefix.*` will
   *     be retrieved noting that the prefix will be stripped from the
   *     returned params.
   * @return A map with the param-value.
   */
  std::map<std::string, std::string> param_values(
      const std::string& prefix) const;

  /** Unsets a parameter (ignores it if it does not exist). */
  Status unset(const std::string& param);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Stores a map of param -> value. */
  std::map<std::string, std::string> param_values_;

  /** The storage manager parameters. */
  SMParams sm_params_;

  /** The VFS parameters. */
  VFSParams vfs_params_;

  /* ********************************* */
  /*          PRIVATE CONSTANTS        */
  /* ********************************* */

  /** Character indicating the start of a comment in a config file. */
  static const char COMMENT_START;

  /** Set of parameter names that are not serialized to file. */
  static const std::set<std::string> unserialized_params_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Populates `param_values_` map with the default values. */
  void set_default_param_values();

  /** Normalizes and parses a string boolean value **/
  Status parse_bool(const std::string& value, bool* result);

  /** Sets the dedup coordinates parameter. */
  Status set_sm_dedup_coords(const std::string& value);

  /** Sets the check for coordinates duplicates parameter. */
  Status set_sm_check_coord_dups(const std::string& value);

  /** Sets the check for out-of-bound coordinates parameter. */
  Status set_sm_check_coord_oob(const std::string& value);

  /** Sets the check for global order parameter. */
  Status set_sm_check_global_order(const std::string& value);

  /** Sets the array metadata cache size, properly parsing the input value. */
  Status set_sm_array_schema_cache_size(const std::string& value);

  /** Sets the fragment metadata cache size, properly parsing the input value.*/
  Status set_sm_fragment_metadata_cache_size(const std::string& value);

  /** Sets the enable signal handlers value, properly parsing the input value.*/
  Status set_sm_enable_signal_handlers(const std::string& value);

  /** Sets the number of threads, properly parsing the input value.*/
  Status set_sm_num_async_threads(const std::string& value);

  /** Sets the number of threads, properly parsing the input value.*/
  Status set_sm_num_reader_threads(const std::string& value);

  /** Sets the number of threads, properly parsing the input value.*/
  Status set_sm_num_writer_threads(const std::string& value);

  /** Sets the number of TBB threads, properly parsing the input value.*/
  Status set_sm_num_tbb_threads(const std::string& value);

  /** Sets the tile cache size, properly parsing the input value. */
  Status set_sm_tile_cache_size(const std::string& value);

  /** Sets the number of VFS threads. */
  Status set_vfs_num_threads(const std::string& value);

  /** Sets the min number of bytes of a VFS parallel operation. */
  Status set_vfs_min_parallel_size(const std::string& value);

  /** Sets the max number of bytes of a batched VFS read operation. */
  Status set_vfs_max_batch_read_size(const std::string& value);

  /** Sets the max read amplification for batched VFS read operations. */
  Status set_vfs_max_batch_read_amplification(const std::string& value);

  /** Sets the max number of allowed file:/// parallel operations. */
  Status set_vfs_file_max_parallel_ops(const std::string& value);

  /** Sets the S3 region. */
  Status set_vfs_s3_region(const std::string& value);

  /** Sets the S3 AWS_ACCESS_KEY_ID. */
  Status set_vfs_s3_aws_access_key_id(const std::string& value);

  /** Sets the S3 AWS_SECRET_ACCESS_KEY. */
  Status set_vfs_s3_aws_secret_access_key(const std::string& value);

  /** Sets the S3 scheme. */
  Status set_vfs_s3_scheme(const std::string& value);

  /** Sets the S3 endpoint override. */
  Status set_vfs_s3_endpoint_override(const std::string& value);

  /** Sets the S3 virtual addressing. */
  Status set_vfs_s3_use_virtual_addressing(const std::string& value);

  /** Sets the maximum number of parallel S3 operations. */
  Status set_vfs_s3_max_parallel_ops(const std::string& value);

  /** Sets the S3 multipart part size. */
  Status set_vfs_s3_multipart_part_size(const std::string& value);

  /** Sets the S3 connect timeout in milliseconds. */
  Status set_vfs_s3_connect_timeout_ms(const std::string& value);

  /** Sets the S3 connect maximum tries. */
  Status set_vfs_s3_connect_max_tries(const std::string& value);

  /** Sets the S3 connect scale factor for exponential backoff. */
  Status set_vfs_s3_connect_scale_factor(const std::string& value);

  /** Sets the S3 request timeout in milliseconds. */
  Status set_vfs_s3_request_timeout_ms(const std::string& value);

  /** Sets the S3 proxy scheme. */
  Status set_vfs_s3_proxy_scheme(const std::string& value);

  /** Sets the S3 proxy host. */
  Status set_vfs_s3_proxy_host(const std::string& value);

  /** Sets the S3 proxy port. */
  Status set_vfs_s3_proxy_port(const std::string& value);

  /** Sets the S3 proxy username. */
  Status set_vfs_s3_proxy_username(const std::string& value);

  /** Sets the S3 proxy password. */
  Status set_vfs_s3_proxy_password(const std::string& value);

  /** Sets the HDFS namenode hostname and port (uri) */
  Status set_vfs_hdfs_name_node(const std::string& value);

  /** Sets the HDFS username */
  Status set_vfs_hdfs_username(const std::string& value);

  /** Sets the Kerberos auth ticket cache path */
  Status set_vfs_hdfs_kerb_ticket_cache_path(const std::string& value);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONFIG_H
