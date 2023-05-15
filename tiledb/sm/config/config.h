/**
 * @file   config.h
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
 * This file defines class Config.
 */

#ifndef TILEDB_CONFIG_H
#define TILEDB_CONFIG_H

#include "tiledb/common/status.h"

#include <map>
#include <set>
#include <string>
#include <vector>

/*
 * C++14 introduced the attribute [[deprecated]], but no conditional syntax
 * for it. In order to turn it on only when we want it, we have to use the
 * preprocessor.
 */
#if defined(TILEDB_DEPRECATE_OLD_CONFIG_GET)
/*
 * Old versions of `Config::get` don't obey the "outputs on the left" principle,
 * or return `Status`, or both.
 */
#define TILEDB_DEPRECATE_CONFIG \
  [[deprecated(                 \
      "Instead use the single-argument version that returns `optional`")]]
#else
#define TILEDB_DEPRECATE_CONFIG
#endif

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * This class manages the TileDB configuration options.
 * It is implemented as a simple map from string to string.
 * Parsing to appropriate types happens on demand.
 */
class Config {
 public:
  /* ****************************** */
  /*        CONFIG DEFAULTS         */
  /* ****************************** */

  /** The default address for rest server. */
  static const std::string REST_SERVER_DEFAULT_ADDRESS;

  /** The default format for serialization. */
  static const std::string REST_SERIALIZATION_DEFAULT_FORMAT;

  /** The default compressor for http requests with the rest server. */
  static const std::string REST_SERVER_DEFAULT_HTTP_COMPRESSOR;

  /** The default http codes to automatically retry requests on. */
  static const std::string REST_RETRY_HTTP_CODES;

  /** The default number of attempts to retry a http request. */
  static const std::string REST_RETRY_COUNT;

  /** The default initial delay for retrying a http request in milliseconds. */
  static const std::string REST_RETRY_INITIAL_DELAY_MS;

  /** The default exponential delay factor for retrying a http request. */
  static const std::string REST_RETRY_DELAY_FACTOR;

  /** The default buffer size for curl reads used by REST. */
  static const std::string REST_CURL_BUFFER_SIZE;

  /** The default for Curl's verbose mode used by REST. */
  static const std::string REST_CURL_VERBOSE;

  /** If the array metadata should be loaded on array open */
  static const std::string REST_LOAD_METADATA_ON_ARRAY_OPEN;

  /** If the array non empty domain should be loaded on array open */
  static const std::string REST_LOAD_NON_EMPTY_DOMAIN_ON_ARRAY_OPEN;

  /** Refactored array open is disabled by default */
  static const std::string REST_USE_REFACTORED_ARRAY_OPEN;

  /** Refactored query submit is disabled by default */
  static const std::string REST_USE_REFACTORED_QUERY_SUBMIT;

  /** The prefix to use for checking for parameter environmental variables. */
  static const std::string CONFIG_ENVIRONMENT_VARIABLE_PREFIX;

  /**
   * The default logging level. It can be:
   * - `1` i.e. `error` if bootstrap flag --enable-verbose is given
   * - `0` i.e. `fatal` if this bootstrap flag is missing
   */
  static const std::string CONFIG_LOGGING_LEVEL;

  /** The default format for logging. */
  static const std::string CONFIG_LOGGING_DEFAULT_FORMAT;

  /** Allow separate attribute writes or not. */
  static const std::string SM_ALLOW_SEPARATE_ATTRIBUTE_WRITES;

  /** Allow updates or not. */
  static const std::string SM_ALLOW_UPDATES_EXPERIMENTAL;

  /**
   * The key for encrypted arrays.
   *  */
  static const std::string SM_ENCRYPTION_KEY;

  /**
   * The type of encryption used for encrypted arrays.
   *  */
  static const std::string SM_ENCRYPTION_TYPE;

  /** If `true`, this will deduplicate coordinates upon sparse writes. */
  static const std::string SM_DEDUP_COORDS;

  /**
   * If `true`, this will check for coordinate duplicates upon sparse
   * writes.
   */
  static const std::string SM_CHECK_COORD_DUPS;

  /**
   * If `true`, this will check for out-of-bound coordinates upon sparse
   * writes.
   */
  static const std::string SM_CHECK_COORD_OOB;

  /**
   * If `true`, this will check ranges for read with out-of-bounds on the
   * dimension domain's. If `false`, the ranges will be capped at the
   * dimension's domain and a warning logged
   */
  static const std::string SM_READ_RANGE_OOB;

  /**
   * If `true`, this will check if the cells upon writes in global order
   * are indeed provided in global order.
   */
  static const std::string SM_CHECK_GLOBAL_ORDER;

  /** If `true`, bypass partitioning on estimated result sizes. */
  static const std::string SM_SKIP_EST_SIZE_PARTITIONING;

  /** If `true`, bypass partitioning budget check for unary ranges. */
  static const std::string SM_SKIP_UNARY_PARTITIONING_BUDGET_CHECK;

  /**
   * The maximum memory budget for producing the result (in bytes)
   * for a fixed-sized attribute or the offsets of a var-sized attribute.
   */
  static const std::string SM_MEMORY_BUDGET;

  /**
   * The maximum memory budget for producing the result (in bytes)
   * for a var-sized attribute.
   */
  static const std::string SM_MEMORY_BUDGET_VAR;

  /** Set the dense reader in qc coords mode. */
  static const std::string SM_QUERY_DENSE_QC_COORDS_MODE;

  /** Which reader to use for dense queries. */
  static const std::string SM_QUERY_DENSE_READER;

  /** Which reader to use for sparse global order queries. */
  static const std::string SM_QUERY_SPARSE_GLOBAL_ORDER_READER;

  /** Which reader to use for sparse unordered with dups queries. */
  static const std::string SM_QUERY_SPARSE_UNORDERED_WITH_DUPS_READER;

  /** Should malloc_trim be called on query/ctx destructors. */
  static const std::string SM_MEM_MALLOC_TRIM;

  /** Maximum tile memory budget for readers. */
  static const std::string SM_UPPER_MEMORY_LIMIT;

  /** Maximum memory budget for readers and writers. */
  static const std::string SM_MEM_TOTAL_BUDGET;

  /** Ratio of the sparse global order reader budget used for coords. */
  static const std::string SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_COORDS;

  /** Ratio of the sparse global order reader budget used for tile ranges. */
  static const std::string SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_TILE_RANGES;

  /** Ratio of the sparse global order reader budget used for array data. */
  static const std::string SM_MEM_SPARSE_GLOBAL_ORDER_RATIO_ARRAY_DATA;

  /** Ratio of the sparse unordered with dups reader budget used for coords. */
  static const std::string SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_COORDS;

  /**
   * Ratio of the sparse unordered with dups reader budget used for tile
   * ranges.
   */
  static const std::string SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_TILE_RANGES;

  /**
   * Ratio of the sparse unordered with dups reader budget used for array
   * data.
   */
  static const std::string SM_MEM_SPARSE_UNORDERED_WITH_DUPS_RATIO_ARRAY_DATA;

  /** Whether or not the signal handlers are installed. */
  static const std::string SM_ENABLE_SIGNAL_HANDLERS;

  /** The maximum concurrency level for compute-bound operations. */
  static const std::string SM_COMPUTE_CONCURRENCY_LEVEL;

  /** The maximum concurrency level for io-bound operations. */
  static const std::string SM_IO_CONCURRENCY_LEVEL;

  /** If `true`, checksum validation will be skipped on reads. */
  static const std::string SM_SKIP_CHECKSUM_VALIDATION;

  /**
   * The factor by which the size of the dense fragment resulting
   * from consolidating a set of fragments (containing at least one
   * dense fragment) can be amplified. This is important when
   * the union of the non-empty domains of the fragments to be
   * consolidated have a lot of empty cells, which the consolidated
   * fragment will have to fill with the special fill value
   * (since the resulting fragment is dense).
   */
  static const std::string SM_CONSOLIDATION_AMPLIFICATION;

  /** The buffer size for each attribute used in consolidation. */
  static const std::string SM_CONSOLIDATION_BUFFER_SIZE;

  /** The maximum fragment size used in consolidation. */
  static const std::string SM_CONSOLIDATION_MAX_FRAGMENT_SIZE;

  /** Purge deleted cells or not. */
  static const std::string SM_CONSOLIDATION_PURGE_DELETED_CELLS;

  /** Number of steps in the consolidation algorithm. */
  static const std::string SM_CONSOLIDATION_STEPS;

  /** Minimum number of fragments to consolidate per step. */
  static const std::string SM_CONSOLIDATION_STEP_MIN_FRAGS;

  /** Maximum number of fragments to consolidate per step. */
  static const std::string SM_CONSOLIDATION_STEP_MAX_FRAGS;

  /**
   * Size ratio of two fragments to be considered for consolidation in a step.
   * This should be a value in [0.0, 1.0].
   * 0.0 means always consolidate and 1.0 consolidate only if sizes are equal.
   */
  static const std::string SM_CONSOLIDATION_STEP_SIZE_RATIO;

  /**
   * The consolidation mode. It can be one of:
   *     - "fragments": only the fragments will be consolidated
   *     - "fragment_meta": only the fragment metadata will be consolidated
   *     - "array_meta": only the array metadata will be consolidated
   *     - "commits": only the commit files will be consolidated
   */
  static const std::string SM_CONSOLIDATION_MODE;

  /**
   * An array will consolidate between this value and timestamp_end.
   */
  static const std::string SM_CONSOLIDATION_TIMESTAMP_START;

  /**
   * An array will consolidate between timestamp_start and this value.
   */
  static const std::string SM_CONSOLIDATION_TIMESTAMP_END;

  /**
   * The vacuum mode. It can be one of:
   *     - "fragments": only the fragments will be vacuumed
   *     - "fragment_meta": only the fragment metadata will be vacuumed
   *     - "array_meta": only the array metadata will be vacuumed
   *     - "commits": only the commit files will be vacuumed
   */
  static const std::string SM_VACUUM_MODE;

  /**
   * An array will vacuum between this value and timestamp_end.
   */
  static const std::string SM_VACUUM_TIMESTAMP_START;

  /**
   * An array will vacuum between timestamp_start and this value.
   */
  static const std::string SM_VACUUM_TIMESTAMP_END;

  /**
   * The size of offsets in bits to be used for offset buffers of var-sized
   * attributes
   */
  static const std::string SM_OFFSETS_BITSIZE;

  /**
   * If `true`, an extra element that points to the end of the values buffer
   * will be added to the end of the offsets buffer of var-sized attributes.
   */
  static const std::string SM_OFFSETS_EXTRA_ELEMENT;

  /**
   * The offset format mode to be used for variable-sized attributes. It can
   * be one of:
   *    - "bytes": express offsets in bytes
   *    - "elements": express offsets in number of elements
   */
  static const std::string SM_OFFSETS_FORMAT_MODE;

  /**
   * The maximum estimated size of the internal tile overlap structure.
   */
  static const std::string SM_MAX_TILE_OVERLAP_SIZE;

  /**
   * A group will open between this value and timestamp_end.
   */
  static const std::string SM_GROUP_TIMESTAMP_START;

  /**
   * An group will open between timestamp_start and this value.
   */
  static const std::string SM_GROUP_TIMESTAMP_END;

  /**
   * If `true` MBRs will be loaded at the same time as the rest of fragment
   * info, otherwise they will be loaded lazily when some info related to MBRs
   * is requested by the user
   */
  static const std::string SM_FRAGMENT_INFO_PRELOAD_MBRS;

  /** If `true` the readers might partially load/unload tile offsets. */
  static const std::string SM_PARTIAL_TILE_OFFSETS_LOADING;

  /** Certificate file path. */
  static const std::string SSL_CA_FILE;

  /** Certificate directory path. */
  static const std::string SSL_CA_PATH;

  /** Whether to verify SSL connections. */
  static const std::string SSL_VERIFY;

  /** The default minimum number of bytes in a parallel VFS operation. */
  static const std::string VFS_MIN_PARALLEL_SIZE;

  /** The default maximum number of bytes in a batched VFS read operation. */
  static const std::string VFS_MAX_BATCH_SIZE;

  /**
   * The default minimum number of bytes between two VFS read batches.
   */
  static const std::string VFS_MIN_BATCH_GAP;

  /** The default minimum number of bytes in a batched VFS read operation. */
  static const std::string VFS_MIN_BATCH_SIZE;

  /** The default posix permissions for file creations */
  static const std::string VFS_FILE_POSIX_FILE_PERMISSIONS;

  /** The default posix permissions for directory creations */
  static const std::string VFS_FILE_POSIX_DIRECTORY_PERMISSIONS;

  /** The default maximum number of parallel file:/// operations. */
  static const std::string VFS_FILE_MAX_PARALLEL_OPS;

  /** The maximum size (in bytes) to read-ahead in the VFS. */
  static const std::string VFS_READ_AHEAD_SIZE;

  /** The maximum size (in bytes) of the VFS read-ahead cache . */
  static const std::string VFS_READ_AHEAD_CACHE_SIZE;

  /** Azure storage account name. */
  static const std::string VFS_AZURE_STORAGE_ACCOUNT_NAME;

  /** Azure storage account key. */
  static const std::string VFS_AZURE_STORAGE_ACCOUNT_KEY;

  /** Azure storage account SAS (shared access signature) token. */
  static const std::string VFS_AZURE_STORAGE_SAS_TOKEN;

  /** Azure blob endpoint. */
  static const std::string VFS_AZURE_BLOB_ENDPOINT;

  /** Azure max parallel ops. */
  static const std::string VFS_AZURE_MAX_PARALLEL_OPS;

  /** Azure block list block size. */
  static const std::string VFS_AZURE_BLOCK_LIST_BLOCK_SIZE;

  /** Azure use block list upload. */
  static const std::string VFS_AZURE_USE_BLOCK_LIST_UPLOAD;

  /** Azure max retries. */
  static const std::string VFS_AZURE_MAX_RETRIES;

  /** Azure min retry delay. */
  static const std::string VFS_AZURE_RETRY_DELAY_MS;

  /** Azure max retry delay. */
  static const std::string VFS_AZURE_MAX_RETRY_DELAY_MS;

  /** GCS Endpoint. */
  static const std::string VFS_GCS_ENDPOINT;

  /** GCS project id. */
  static const std::string VFS_GCS_PROJECT_ID;

  /** GCS max parallel ops. */
  static const std::string VFS_GCS_MAX_PARALLEL_OPS;

  /** GCS multi part size. */
  static const std::string VFS_GCS_MULTI_PART_SIZE;

  /** GCS use multi part upload. */
  static const std::string VFS_GCS_USE_MULTI_PART_UPLOAD;

  /** GCS request timeout in milliseconds. */
  static const std::string VFS_GCS_REQUEST_TIMEOUT_MS;

  /** S3 region. */
  static const std::string VFS_S3_REGION;

  /** S3 aws access key id. */
  static const std::string VFS_S3_AWS_ACCESS_KEY_ID;

  /** S3 aws secret access key. */
  static const std::string VFS_S3_AWS_SECRET_ACCESS_KEY;

  /** S3 aws session token. */
  static const std::string VFS_S3_AWS_SESSION_TOKEN;

  /** S3 aws role arn. */
  static const std::string VFS_S3_AWS_ROLE_ARN;

  /** S3 aws external id. */
  static const std::string VFS_S3_AWS_EXTERNAL_ID;

  /** S3 aws load frequency. */
  static const std::string VFS_S3_AWS_LOAD_FREQUENCY;

  /** S3 aws session name. */
  static const std::string VFS_S3_AWS_SESSION_NAME;

  /** S3 scheme (http for local minio, https for AWS S3). */
  static const std::string VFS_S3_SCHEME;

  /** S3 endpoint override. */
  static const std::string VFS_S3_ENDPOINT_OVERRIDE;

  /** Use virtual addressing (false for minio, true for AWS S3). */
  static const std::string VFS_S3_USE_VIRTUAL_ADDRESSING;

  /** S3 skip init. */
  static const std::string VFS_S3_SKIP_INIT;

  /** Use virtual addressing (true). */
  static const std::string VFS_S3_USE_MULTIPART_UPLOAD;

  /** S3 max parallel operations. */
  static const std::string VFS_S3_MAX_PARALLEL_OPS;

  /** Size of parts used in the S3 multi-part uploads. */
  static const std::string VFS_S3_MULTIPART_PART_SIZE;

  /** Certificate file path. */
  static const std::string VFS_S3_CA_FILE;

  /** Certificate directory path. */
  static const std::string VFS_S3_CA_PATH;

  /** Connect timeout in milliseconds. */
  static const std::string VFS_S3_CONNECT_TIMEOUT_MS;

  /** Connect max tries. */
  static const std::string VFS_S3_CONNECT_MAX_TRIES;

  /** Connect scale factor for exponential backoff. */
  static const std::string VFS_S3_CONNECT_SCALE_FACTOR;

  /** S3 server-side encryption algorithm. */
  static const std::string VFS_S3_SSE;

  /** The S3 KMS key id for KMS server-side-encryption. */
  static const std::string VFS_S3_SSE_KMS_KEY_ID;

  /** Request timeout in milliseconds. */
  static const std::string VFS_S3_REQUEST_TIMEOUT_MS;

  /** Requester pays for the S3 access charges. */
  static const std::string VFS_S3_REQUESTER_PAYS;

  /** S3 proxy scheme. */
  static const std::string VFS_S3_PROXY_SCHEME;

  /** S3 proxy host. */
  static const std::string VFS_S3_PROXY_HOST;

  /** S3 proxy port. */
  static const std::string VFS_S3_PROXY_PORT;

  /** S3 proxy username. */
  static const std::string VFS_S3_PROXY_USERNAME;

  /** S3 proxy password. */
  static const std::string VFS_S3_PROXY_PASSWORD;

  /** S3 logging level. */
  static const std::string VFS_S3_LOGGING_LEVEL;

  /** Verify TLS/SSL certificates (true). */
  static const std::string VFS_S3_VERIFY_SSL;

  /** Force making an unsigned request to s3 (false). */
  static const std::string VFS_S3_NO_SIGN_REQUEST;

  /** HDFS default kerb ticket cache path. */
  static const std::string VFS_HDFS_KERB_TICKET_CACHE_PATH;

  /** HDFS default name node uri. */
  static const std::string VFS_HDFS_NAME_NODE_URI;

  /** HDFS default username. */
  static const std::string VFS_HDFS_USERNAME;

  /** S3 default bucket canned ACL */
  static const std::string VFS_S3_BUCKET_CANNED_ACL;

  /** S3 default object canned ACL */
  static const std::string VFS_S3_OBJECT_CANNED_ACL;

  /**
   * Force S3 SDK to only load config options from a set source.
   * The supported options are
   * - `auto` (TileDB config options are considered first,
   *    then SDK-defined precedence: env vars, config files, ec2 metadata),
   * - `config_files` (forces SDK to only consider options found in aws
   *    config files),
   *    `sts_profile_with_web_identity` (force SDK to consider assume roles/sts
   * from config files with support for web tokens, commonly used by EKS/ECS).
   */
  static const std::string VFS_S3_CONFIG_SOURCE;

  /**
   * Specifies the size in bytes of the internal buffers used in the filestore
   * API. The size should be bigger than the minimum tile size filestore
   * currently supports, that is currently 1024bytes. */
  static const std::string FILESTORE_BUFFER_SIZE;

  /* ****************************** */
  /*        OTHER CONSTANTS         */
  /* ****************************** */

  /** Marker class to enforce value is found with Config::get overload */
  class MustFindMarker {};
  static constexpr MustFindMarker must_find{};

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Config();

  /** Destructor. */
  ~Config();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Loads the config parameters from a configuration (local) file. */
  Status load_from_file(const std::string& filename);

  /** Saves the config parameters to a configuration (local) file. */
  Status save_to_file(const std::string& filename);

  /** Sets a config parameter. */
  Status set(const std::string& param, const std::string& value);

  /**
   * Retrieve the string value of a configuration parameter and convert it to
   * a designated type.
   *
   * @param key The name of the configuration parameter
   * @return If a configuration item is present, its value. If not, `nullopt`.
   */
  template <class T>
  [[nodiscard]] inline optional<T> get(const std::string& key) const {
    return get_internal<T, false>(key);
  }

  /**
   * Retrieves the value of the given parameter in the templated type.
   * Throws StatusException if config value could not be found
   *
   * @param key The name of the configuration parameter
   * @return The value of the configuration parameter
   */
  template <class T>
  inline T get(const std::string& key, const MustFindMarker&) const {
    return get_internal<T, true>(key).value();
  }

  /**
   * Returns the string representation of a config parameter value.
   * Sets `found` to `true` if found and `false` otherwise.
   */
  TILEDB_DEPRECATE_CONFIG
  std::string get(const std::string& param, bool* found) const;

  /** Gets a config parameter value (`nullptr` if `param` does not exist). */
  TILEDB_DEPRECATE_CONFIG
  Status get(const std::string& param, const char** value) const;

  /**
   * Retrieves the value of the given parameter in the templated type.
   * Sets `found` to `true` if found and `false` otherwise.
   */
  TILEDB_DEPRECATE_CONFIG
  template <class T>
  Status get(const std::string& param, T* value, bool* found) const;

  /**
   * Retrieves the value of the given parameter in the templated type.
   * Sets `found` to `true` if found and `false` otherwise.
   */
  template <class T>
  Status get_vector(
      const std::string& param, std::vector<T>* value, bool* found) const;

  /** Returns the param -> value map. */
  const std::map<std::string, std::string>& param_values() const;

  /** Gets the set parameters. */
  const std::set<std::string>& set_params() const;

  /**
   * Unsets a set parameter (ignores it if it does not exist).
   */
  Status unset(const std::string& param);

  /** Inherits the **set** parameters of the input `config`. */
  void inherit(const Config& config);

  /** Compares configs for equality. */
  bool operator==(const Config& rhs) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Stores a map of (param -> value) for the set parameters. */
  std::map<std::string, std::string> param_values_;

  /** Stores the parameters set by the user. */
  std::set<std::string> set_params_;

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

  /**
   * If `param` is one of the system configuration parameters, it checks
   * whether its `value` can be parsed in the correct datatype. If
   * `param` is not a system configuration parameter, the function is a noop.
   */
  Status sanity_check(const std::string& param, const std::string& value) const;

  /**
   * Convert a parameter in the form of "a.b.c" to expected env string of
   * "A_B_C"
   * @param param
   * @return converted string
   */
  std::string convert_to_env_param(const std::string& param) const;

  /**
   * Get an environmental variable
   * @param param to fetch
   * @param found pointer to bool to set if env parameter was found or not
   * @return parameter value if found or nullptr if not found
   */
  const char* get_from_env(const std::string& param, bool* found) const;

  /**
   * Get an parameter from config variable
   * @param param to fetch
   * @param found pointer to bool to set if parameter was found or not
   * @return parameter value if found or nullptr if not found
   */
  const char* get_from_config(const std::string& param, bool* found) const;

  /**
   * Get a configuration parameter from config object or environmental variables
   *
   * The order we look for values are
   * 1. user set config parameters
   * 2. env variables
   * 3. default config value
   *
   * @param param parameter to fetch
   * @param found pointer to bool to set if parameter was found or not
   * @return parameter value if found or empty string if not
   */
  const char* get_from_config_or_env(
      const std::string& param, bool* found) const;

  template <class T, bool must_find_>
  optional<T> get_internal(const std::string& key) const;

  template <bool must_find_>
  optional<std::string> get_internal_string(const std::string& key) const;
};

/**
 * An explicit specialization for `std::string`. It does not call a conversion
 * function and it is thus the same as `get_internal_string<false>`.
 */
template <>
[[nodiscard]] inline optional<std::string> Config::get<std::string>(
    const std::string& key) const {
  return get_internal_string<false>(key);
}

/**
 * An explicit specialization for `std::string`. It does not call a conversion
 * function and it is thus the same as `get_internal_string<true>`
 *
 * Will throw if value is not found for provided config key
 */
template <>
inline std::string Config::get<std::string>(
    const std::string& key, const Config::MustFindMarker&) const {
  return get_internal_string<true>(key).value();
}
}  // namespace tiledb::sm

#ifdef TILEDB_DEPRECATE_CONFIG
#undef TILEDB_DEPRECATE_CONFIG
#endif

#endif  // TILEDB_CONFIG_H
