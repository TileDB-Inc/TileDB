/**
 * @file   config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include "tiledb/sm/config/config.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/utils.h"

#include <fstream>
#include <iostream>
#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*        CONFIG DEFAULTS         */
/* ****************************** */

const std::string Config::REST_SERVER_DEFAULT_ADDRESS =
    "https://api.tiledb.com";
const std::string Config::REST_SERIALIZATION_DEFAULT_FORMAT = "CAPNP";
const std::string Config::REST_SERVER_DEFAULT_HTTP_COMPRESSOR = "any";
const std::string Config::CONFIG_ENVIRONMENT_VARIABLE_PREFIX = "TILEDB_";
const std::string Config::SM_DEDUP_COORDS = "false";
const std::string Config::SM_CHECK_COORD_DUPS = "true";
const std::string Config::SM_CHECK_COORD_OOB = "true";
const std::string Config::SM_CHECK_GLOBAL_ORDER = "true";
const std::string Config::SM_TILE_CACHE_SIZE = "10000000";
const std::string Config::SM_MEMORY_BUDGET = "5368709120";       // 5GB
const std::string Config::SM_MEMORY_BUDGET_VAR = "10737418240";  // 10GB;
const std::string Config::SM_SUB_PARTITIONER_MEMORY_BUDGET = "0";
const std::string Config::SM_ENABLE_SIGNAL_HANDLERS = "true";
const std::string Config::SM_COMPUTE_CONCURRENCY_LEVEL =
    utils::parse::to_str(std::thread::hardware_concurrency());
const std::string Config::SM_IO_CONCURRENCY_LEVEL =
    utils::parse::to_str(std::thread::hardware_concurrency());
#ifdef HAVE_TBB
const std::string Config::SM_NUM_TBB_THREADS =
    utils::parse::to_str((int)tbb::task_scheduler_init::automatic);
#else
const std::string Config::SM_NUM_TBB_THREADS = "-1";
#endif
const std::string Config::SM_SKIP_CHECKSUM_VALIDATION = "false";
const std::string Config::SM_CONSOLIDATION_AMPLIFICATION = "1.0";
const std::string Config::SM_CONSOLIDATION_BUFFER_SIZE = "50000000";
const std::string Config::SM_CONSOLIDATION_STEPS = "4294967295";
const std::string Config::SM_CONSOLIDATION_STEP_MIN_FRAGS = "4294967295";
const std::string Config::SM_CONSOLIDATION_STEP_MAX_FRAGS = "4294967295";
const std::string Config::SM_CONSOLIDATION_STEP_SIZE_RATIO = "0.0";
const std::string Config::SM_CONSOLIDATION_MODE = "fragments";
const std::string Config::SM_VACUUM_MODE = "fragments";
const std::string Config::VFS_MIN_PARALLEL_SIZE = "10485760";
const std::string Config::VFS_MIN_BATCH_GAP = "512000";
const std::string Config::VFS_MIN_BATCH_SIZE = "20971520";
const std::string Config::VFS_FILE_POSIX_FILE_PERMISSIONS = "644";
const std::string Config::VFS_FILE_POSIX_DIRECTORY_PERMISSIONS = "755";
const std::string Config::VFS_FILE_MAX_PARALLEL_OPS =
    Config::SM_IO_CONCURRENCY_LEVEL;
const std::string Config::VFS_FILE_ENABLE_FILELOCKS = "true";
const std::string Config::VFS_READ_AHEAD_SIZE = "102400";          // 100KiB
const std::string Config::VFS_READ_AHEAD_CACHE_SIZE = "10485760";  // 10MiB;
const std::string Config::VFS_AZURE_STORAGE_ACCOUNT_NAME = "";
const std::string Config::VFS_AZURE_STORAGE_ACCOUNT_KEY = "";
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
const std::string Config::VFS_S3_REGION = "us-east-1";
const std::string Config::VFS_S3_AWS_ACCESS_KEY_ID = "";
const std::string Config::VFS_S3_AWS_SECRET_ACCESS_KEY = "";
const std::string Config::VFS_S3_AWS_SESSION_TOKEN = "";
const std::string Config::VFS_S3_AWS_ROLE_ARN = "";
const std::string Config::VFS_S3_AWS_EXTERNAL_ID = "";
const std::string Config::VFS_S3_AWS_LOAD_FREQUENCY = "";
const std::string Config::VFS_S3_SCHEME = "https";
const std::string Config::VFS_S3_ENDPOINT_OVERRIDE = "";
const std::string Config::VFS_S3_USE_VIRTUAL_ADDRESSING = "true";
const std::string Config::VFS_S3_USE_MULTIPART_UPLOAD = "true";
const std::string Config::VFS_S3_MAX_PARALLEL_OPS =
    Config::SM_IO_CONCURRENCY_LEVEL;
const std::string Config::VFS_S3_MULTIPART_PART_SIZE = "5242880";
const std::string Config::VFS_S3_CA_FILE = "";
const std::string Config::VFS_S3_CA_PATH = "";
const std::string Config::VFS_S3_CONNECT_TIMEOUT_MS = "3000";
const std::string Config::VFS_S3_CONNECT_MAX_TRIES = "5";
const std::string Config::VFS_S3_CONNECT_SCALE_FACTOR = "25";
const std::string Config::VFS_S3_REQUEST_TIMEOUT_MS = "3000";
const std::string Config::VFS_S3_PROXY_SCHEME = "http";
const std::string Config::VFS_S3_PROXY_HOST = "";
const std::string Config::VFS_S3_PROXY_PORT = "0";
const std::string Config::VFS_S3_PROXY_USERNAME = "";
const std::string Config::VFS_S3_PROXY_PASSWORD = "";
const std::string Config::VFS_S3_LOGGING_LEVEL = "Off";
const std::string Config::VFS_S3_VERIFY_SSL = "true";
const std::string Config::VFS_HDFS_KERB_TICKET_CACHE_PATH = "";
const std::string Config::VFS_HDFS_NAME_NODE_URI = "";
const std::string Config::VFS_HDFS_USERNAME = "";

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
    "rest.username",
    "rest.password",
    "rest.token",
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Config::Config() {
  // Set config values
  param_values_["rest.server_address"] = REST_SERVER_DEFAULT_ADDRESS;
  param_values_["rest.server_serialization_format"] =
      REST_SERIALIZATION_DEFAULT_FORMAT;
  param_values_["rest.http_compressor"] = REST_SERVER_DEFAULT_HTTP_COMPRESSOR;
  param_values_["config.env_var_prefix"] = CONFIG_ENVIRONMENT_VARIABLE_PREFIX;
  param_values_["sm.dedup_coords"] = SM_DEDUP_COORDS;
  param_values_["sm.check_coord_dups"] = SM_CHECK_COORD_DUPS;
  param_values_["sm.check_coord_oob"] = SM_CHECK_COORD_OOB;
  param_values_["sm.check_global_order"] = SM_CHECK_GLOBAL_ORDER;
  param_values_["sm.tile_cache_size"] = SM_TILE_CACHE_SIZE;
  param_values_["sm.memory_budget"] = SM_MEMORY_BUDGET;
  param_values_["sm.memory_budget_var"] = SM_MEMORY_BUDGET_VAR;
  param_values_["sm.sub_partitioner_memory_budget"] =
      SM_SUB_PARTITIONER_MEMORY_BUDGET;
  param_values_["sm.enable_signal_handlers"] = SM_ENABLE_SIGNAL_HANDLERS;
  param_values_["sm.compute_concurrency_level"] = SM_COMPUTE_CONCURRENCY_LEVEL;
  param_values_["sm.io_concurrency_level"] = SM_IO_CONCURRENCY_LEVEL;
  param_values_["sm.num_tbb_threads"] = SM_NUM_TBB_THREADS;
  param_values_["sm.skip_checksum_validation"] = SM_SKIP_CHECKSUM_VALIDATION;
  param_values_["sm.consolidation.amplification"] =
      SM_CONSOLIDATION_AMPLIFICATION;
  param_values_["sm.consolidation.buffer_size"] = SM_CONSOLIDATION_BUFFER_SIZE;
  param_values_["sm.consolidation.step_min_frags"] =
      SM_CONSOLIDATION_STEP_MIN_FRAGS;
  param_values_["sm.consolidation.step_max_frags"] =
      SM_CONSOLIDATION_STEP_MAX_FRAGS;
  param_values_["sm.consolidation.step_size_ratio"] =
      SM_CONSOLIDATION_STEP_SIZE_RATIO;
  param_values_["sm.consolidation.steps"] = SM_CONSOLIDATION_STEPS;
  param_values_["sm.consolidation.mode"] = SM_CONSOLIDATION_MODE;
  param_values_["sm.vacuum.mode"] = SM_VACUUM_MODE;
  param_values_["vfs.min_parallel_size"] = VFS_MIN_PARALLEL_SIZE;
  param_values_["vfs.min_batch_gap"] = VFS_MIN_BATCH_GAP;
  param_values_["vfs.min_batch_size"] = VFS_MIN_BATCH_SIZE;
  param_values_["vfs.read_ahead_size"] = VFS_READ_AHEAD_SIZE;
  param_values_["vfs.read_ahead_cache_size"] = VFS_READ_AHEAD_CACHE_SIZE;
  param_values_["vfs.file.posix_file_permissions"] =
      VFS_FILE_POSIX_FILE_PERMISSIONS;
  param_values_["vfs.file.posix_directory_permissions"] =
      VFS_FILE_POSIX_DIRECTORY_PERMISSIONS;
  param_values_["vfs.file.max_parallel_ops"] = VFS_FILE_MAX_PARALLEL_OPS;
  param_values_["vfs.file.enable_filelocks"] = VFS_FILE_ENABLE_FILELOCKS;
  param_values_["vfs.azure.storage_account_name"] =
      VFS_AZURE_STORAGE_ACCOUNT_NAME;
  param_values_["vfs.azure.storage_account_key"] =
      VFS_AZURE_STORAGE_ACCOUNT_KEY;
  param_values_["vfs.azure.blob_endpoint"] = VFS_AZURE_BLOB_ENDPOINT;
  param_values_["vfs.azure.use_https"] = VFS_AZURE_USE_HTTPS;
  param_values_["vfs.azure.max_parallel_ops"] = VFS_AZURE_MAX_PARALLEL_OPS;
  param_values_["vfs.azure.block_list_block_size"] =
      VFS_AZURE_BLOCK_LIST_BLOCK_SIZE;
  param_values_["vfs.azure.use_block_list_upload"] =
      VFS_AZURE_USE_BLOCK_LIST_UPLOAD;
  param_values_["vfs.gcs.project_id"] = VFS_GCS_PROJECT_ID;
  param_values_["vfs.gcs.max_parallel_ops"] = VFS_GCS_MAX_PARALLEL_OPS;
  param_values_["vfs.gcs.multi_part_size"] = VFS_GCS_MULTI_PART_SIZE;
  param_values_["vfs.gcs.use_multi_part_upload"] =
      VFS_GCS_USE_MULTI_PART_UPLOAD;
  param_values_["vfs.s3.region"] = VFS_S3_REGION;
  param_values_["vfs.s3.aws_access_key_id"] = VFS_S3_AWS_ACCESS_KEY_ID;
  param_values_["vfs.s3.aws_secret_access_key"] = VFS_S3_AWS_SECRET_ACCESS_KEY;
  param_values_["vfs.s3.aws_session_token"] = VFS_S3_AWS_SESSION_TOKEN;
  param_values_["vfs.s3.aws_role_arn"] = VFS_S3_AWS_ROLE_ARN;
  param_values_["vfs.s3.aws_external_id"] = VFS_S3_AWS_EXTERNAL_ID;
  param_values_["vfs.s3.aws_load_frequency"] = VFS_S3_AWS_LOAD_FREQUENCY;
  param_values_["vfs.s3.scheme"] = VFS_S3_SCHEME;
  param_values_["vfs.s3.endpoint_override"] = VFS_S3_ENDPOINT_OVERRIDE;
  param_values_["vfs.s3.use_virtual_addressing"] =
      VFS_S3_USE_VIRTUAL_ADDRESSING;
  param_values_["vfs.s3.use_multipart_upload"] = VFS_S3_USE_MULTIPART_UPLOAD;
  param_values_["vfs.s3.max_parallel_ops"] = VFS_S3_MAX_PARALLEL_OPS;
  param_values_["vfs.s3.multipart_part_size"] = VFS_S3_MULTIPART_PART_SIZE;
  param_values_["vfs.s3.ca_file"] = VFS_S3_CA_FILE;
  param_values_["vfs.s3.ca_path"] = VFS_S3_CA_PATH;
  param_values_["vfs.s3.connect_timeout_ms"] = VFS_S3_CONNECT_TIMEOUT_MS;
  param_values_["vfs.s3.connect_max_tries"] = VFS_S3_CONNECT_MAX_TRIES;
  param_values_["vfs.s3.connect_scale_factor"] = VFS_S3_CONNECT_SCALE_FACTOR;
  param_values_["vfs.s3.request_timeout_ms"] = VFS_S3_REQUEST_TIMEOUT_MS;
  param_values_["vfs.s3.proxy_scheme"] = VFS_S3_PROXY_SCHEME;
  param_values_["vfs.s3.proxy_host"] = VFS_S3_PROXY_HOST;
  param_values_["vfs.s3.proxy_port"] = VFS_S3_PROXY_PORT;
  param_values_["vfs.s3.proxy_username"] = VFS_S3_PROXY_USERNAME;
  param_values_["vfs.s3.proxy_password"] = VFS_S3_PROXY_PASSWORD;
  param_values_["vfs.s3.logging_level"] = VFS_S3_LOGGING_LEVEL;
  param_values_["vfs.s3.verify_ssl"] = VFS_S3_VERIFY_SSL;
  param_values_["vfs.hdfs.name_node_uri"] = VFS_HDFS_NAME_NODE_URI;
  param_values_["vfs.hdfs.username"] = VFS_HDFS_USERNAME;
  param_values_["vfs.hdfs.kerb_ticket_cache_path"] =
      VFS_HDFS_KERB_TICKET_CACHE_PATH;
}

Config::~Config() = default;

/* ****************************** */
/*                API             */
/* ****************************** */

Status Config::load_from_file(const std::string& filename) {
  // Do nothing if filename is empty
  if (filename.empty())
    return LOG_STATUS(
        Status::ConfigError("Cannot load from file; Invalid filename"));

  std::ifstream ifs(filename);
  if (!ifs.is_open()) {
    std::stringstream msg;
    msg << "Failed to open config file '" << filename << "'";
    return LOG_STATUS(Status::ConfigError(msg.str()));
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
      return LOG_STATUS(Status::ConfigError(msg.str()));
    }

    // Parse extra
    line_ss >> extra;
    if (!extra.empty() && extra[0] != COMMENT_START) {
      std::stringstream msg;
      msg << "Failed to parse config file '" << filename << "'; ";
      msg << "Invalid line format (line: " << linenum << ")";
      return LOG_STATUS(Status::ConfigError(msg.str()));
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
        Status::ConfigError("Cannot save to file; Invalid filename"));

  std::ofstream ofs(filename);
  if (!ofs.is_open()) {
    std::stringstream msg;
    msg << "Failed to open config file '" << filename << "' for writing";
    return LOG_STATUS(Status::ConfigError(msg.str()));
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

Status Config::get(const std::string& param, const char** value) const {
  bool found;
  const char* val = get_from_config_or_env(param, &found);
  *value = found ? val : nullptr;

  return Status::Ok();
}

std::string Config::get(const std::string& param, bool* found) const {
  const char* val = get_from_config_or_env(param, found);
  return found ? val : "";
}

template <class T>
Status Config::get(const std::string& param, T* value, bool* found) const {
  // Check if parameter exists
  const char* val = get_from_config_or_env(param, found);
  if (!*found)
    return Status::Ok();

  // Parameter found, retrieve value
  return utils::parse::convert(val, value);
}

const std::map<std::string, std::string>& Config::param_values() const {
  return param_values_;
}

const std::set<std::string>& Config::set_params() const {
  return set_params_;
}

Status Config::unset(const std::string& param) {
  // Set back to default
  if (param == "rest.server_address") {
    param_values_["rest.server_address"] = REST_SERVER_DEFAULT_ADDRESS;
  } else if (param == "rest.server_serialization_format") {
    param_values_["rest.server_serialization_format"] =
        REST_SERIALIZATION_DEFAULT_FORMAT;
  } else if (param == "rest.http_compressor") {
    param_values_["rest.http_compressor"] = REST_SERVER_DEFAULT_HTTP_COMPRESSOR;
  } else if (param == "config.env_var_prefix") {
    param_values_["config.env_var_prefix"] = CONFIG_ENVIRONMENT_VARIABLE_PREFIX;
  } else if (param == "sm.dedup_coords") {
    param_values_["sm.dedup_coords"] = SM_DEDUP_COORDS;
  } else if (param == "sm.check_coord_dups") {
    param_values_["sm.check_coord_dups"] = SM_CHECK_COORD_DUPS;
  } else if (param == "sm.check_coord_oob") {
    param_values_["sm.check_coord_oob"] = SM_CHECK_COORD_OOB;
  } else if (param == "sm.check_global_order") {
    param_values_["sm.check_global_order"] = SM_CHECK_GLOBAL_ORDER;
  } else if (param == "sm.tile_cache_size") {
    param_values_["sm.tile_cache_size"] = SM_TILE_CACHE_SIZE;
  } else if (param == "sm.memory_budget") {
    param_values_["sm.memory_budget"] = SM_MEMORY_BUDGET;
  } else if (param == "sm.memory_budget_var") {
    param_values_["sm.memory_budget_var"] = SM_MEMORY_BUDGET_VAR;
  } else if (param == "sm.memory_budget") {
    param_values_["sm.sub_partitioner_memory_budget"] =
        SM_SUB_PARTITIONER_MEMORY_BUDGET;
  } else if (param == "sm.enable_signal_handlers") {
    param_values_["sm.enable_signal_handlers"] = SM_ENABLE_SIGNAL_HANDLERS;
  } else if (param == "sm.compute_concurrency_level") {
    param_values_["sm.compute_concurrency_level"] =
        SM_COMPUTE_CONCURRENCY_LEVEL;
  } else if (param == "sm.io_concurrency_level") {
    param_values_["sm.io_concurrency_level"] = SM_IO_CONCURRENCY_LEVEL;
  } else if (param == "sm.num_tbb_threads") {
    param_values_["sm.num_tbb_threads"] = SM_NUM_TBB_THREADS;
  } else if (param == "sm.consolidation.amplification") {
    param_values_["sm.consolidation.amplification"] =
        SM_CONSOLIDATION_AMPLIFICATION;
  } else if (param == "sm.consolidation.buffer_size") {
    param_values_["sm.consolidation.buffer_size"] =
        SM_CONSOLIDATION_BUFFER_SIZE;
  } else if (param == "sm.consolidation.steps") {
    param_values_["sm.consolidation.steps"] = SM_CONSOLIDATION_STEPS;
  } else if (param == "sm.consolidation.step_min_frags") {
    param_values_["sm.consolidation.step_min_frags"] =
        SM_CONSOLIDATION_STEP_MIN_FRAGS;
  } else if (param == "sm.consolidation.step_max_frags") {
    param_values_["sm.consolidation.step_max_frags"] =
        SM_CONSOLIDATION_STEP_MAX_FRAGS;
  } else if (param == "sm.consolidation.step_size_ratio") {
    param_values_["sm.consolidation.step_size_ratio"] =
        SM_CONSOLIDATION_STEP_SIZE_RATIO;
  } else if (param == "sm.consolidation.mode") {
    param_values_["sm.consolidation.mode"] = SM_CONSOLIDATION_MODE;
  } else if (param == "sm.vacuum.mode") {
    param_values_["sm.vacuum.mode"] = SM_VACUUM_MODE;
  } else if (param == "vfs.min_parallel_size") {
    param_values_["vfs.min_parallel_size"] = VFS_MIN_PARALLEL_SIZE;
  } else if (param == "vfs.min_batch_gap") {
    param_values_["vfs.min_batch_gap"] = VFS_MIN_BATCH_GAP;
  } else if (param == "vfs.min_batch_size") {
    param_values_["vfs.min_batch_size"] = VFS_MIN_BATCH_SIZE;
  } else if (param == "vfs.read_ahead_size") {
    param_values_["vfs.read_ahead_size"] = VFS_READ_AHEAD_SIZE;
  } else if (param == "vfs.read_ahead_cache_size") {
    param_values_["vfs.read_ahead_cache_size"] = VFS_READ_AHEAD_CACHE_SIZE;
  } else if (param == "vfs.file.posix_file_permissions") {
    param_values_["vfs.file.posix_file_permissions"] =
        VFS_FILE_POSIX_FILE_PERMISSIONS;
  } else if (param == "vfs.file.posix_directory_permissions") {
    param_values_["vfs.file.posix_directory_permissions"] =
        VFS_FILE_POSIX_DIRECTORY_PERMISSIONS;
  } else if (param == "vfs.file.max_parallel_ops") {
    param_values_["vfs.file.max_parallel_ops"] = VFS_FILE_MAX_PARALLEL_OPS;
  } else if (param == "vfs.file.enable_filelocks") {
    param_values_["vfs.file.enable_filelocks"] = VFS_FILE_ENABLE_FILELOCKS;
  } else if (param == "vfs.azure.storage_account_name") {
    param_values_["vfs.azure.storage_account_name"] =
        VFS_AZURE_STORAGE_ACCOUNT_NAME;
  } else if (param == "vfs.azure.storage_account_key") {
    param_values_["vfs.azure.storage_account_key"] =
        VFS_AZURE_STORAGE_ACCOUNT_KEY;
  } else if (param == "vfs.azure.blob_endpoint") {
    param_values_["vfs.azure.blob_endpoint"] = VFS_AZURE_BLOB_ENDPOINT;
  } else if (param == "vfs.azure.use_https") {
    param_values_["vfs.azure.use_https"] = VFS_AZURE_USE_HTTPS;
  } else if (param == "vfs.azure.max_parallel_ops") {
    param_values_["vfs.azure.max_parallel_ops"] = VFS_AZURE_MAX_PARALLEL_OPS;
  } else if (param == "vfs.azure.block_list_block_size") {
    param_values_["vfs.azure.block_list_block_size"] =
        VFS_AZURE_BLOCK_LIST_BLOCK_SIZE;
  } else if (param == "vfs.azure.use_block_list_upload") {
    param_values_["vfs.azure.use_block_list_upload"] =
        VFS_AZURE_USE_BLOCK_LIST_UPLOAD;
  } else if (param == "vfs.gcs.project_id") {
    param_values_["vfs.gcs.project_id"] = VFS_GCS_PROJECT_ID;
  } else if (param == "vfs.gcs.max_parallel_ops") {
    param_values_["vfs.gcs.max_parallel_ops"] = VFS_GCS_MAX_PARALLEL_OPS;
  } else if (param == "vfs.gcs.multi_part_size") {
    param_values_["vfs.gcs.multi_part_size"] = VFS_GCS_MULTI_PART_SIZE;
  } else if (param == "vfs.gcs.use_multi_part_upload") {
    param_values_["vfs.gcs.use_multi_part_upload"] =
        VFS_GCS_USE_MULTI_PART_UPLOAD;
  } else if (param == "vfs.s3.region") {
    param_values_["vfs.s3.region"] = VFS_S3_REGION;
  } else if (param == "vfs.s3.aws_access_key_id") {
    param_values_["vfs.s3.aws_access_key_id"] = VFS_S3_AWS_ACCESS_KEY_ID;
  } else if (param == "vfs.s3.aws_secret_access_key") {
    param_values_["vfs.s3.aws_secret_access_key"] =
        VFS_S3_AWS_SECRET_ACCESS_KEY;
  } else if (param == "vfs.s3.aws_session_token") {
    param_values_["vfs.s3.aws_session_token"] = VFS_S3_AWS_SESSION_TOKEN;
  } else if (param == "vfs.s3.logging_level") {
    param_values_["vfs.s3.logging_level"] = VFS_S3_LOGGING_LEVEL;
  } else if (param == "vfs.s3.scheme") {
    param_values_["vfs.s3.scheme"] = VFS_S3_SCHEME;
  } else if (param == "vfs.s3.endpoint_override") {
    param_values_["vfs.s3.endpoint_override"] = VFS_S3_ENDPOINT_OVERRIDE;
  } else if (param == "vfs.s3.use_virtual_addressing") {
    param_values_["vfs.s3.use_virtual_addressing"] =
        VFS_S3_USE_VIRTUAL_ADDRESSING;
  } else if (param == "vfs.s3.use_multipart_upload") {
    param_values_["vfs.s3.use_multipart_upload"] = VFS_S3_USE_MULTIPART_UPLOAD;
  } else if (param == "vfs.s3.max_parallel_ops") {
    param_values_["vfs.s3.max_parallel_ops"] = VFS_S3_MAX_PARALLEL_OPS;
  } else if (param == "vfs.s3.multipart_part_size") {
    param_values_["vfs.s3.multipart_part_size"] = VFS_S3_MULTIPART_PART_SIZE;
  } else if (param == "vfs.s3.ca_file") {
    param_values_["vfs.s3.ca_file"] = VFS_S3_CA_FILE;
  } else if (param == "vfs.s3.ca_path") {
    param_values_["vfs.s3.ca_path"] = VFS_S3_CA_PATH;
  } else if (param == "vfs.s3.connect_timeout_ms") {
    param_values_["vfs.s3.connect_timeout_ms"] = VFS_S3_CONNECT_TIMEOUT_MS;
  } else if (param == "vfs.s3.connect_max_tries") {
    param_values_["vfs.s3.connect_max_tries"] = VFS_S3_CONNECT_MAX_TRIES;
  } else if (param == "vfs.s3.connect_scale_factor") {
    param_values_["vfs.s3.connect_scale_factor"] = VFS_S3_CONNECT_SCALE_FACTOR;
  } else if (param == "vfs.s3.request_timeout_ms") {
    param_values_["vfs.s3.request_timeout_ms"] = VFS_S3_REQUEST_TIMEOUT_MS;
  } else if (param == "vfs.s3.proxy_scheme") {
    param_values_["vfs.s3.proxy_scheme"] = VFS_S3_PROXY_SCHEME;
  } else if (param == "vfs.s3.proxy_host") {
    param_values_["vfs.s3.proxy_host"] = VFS_S3_PROXY_HOST;
  } else if (param == "vfs.s3.proxy_port") {
    param_values_["vfs.s3.proxy_port"] = VFS_S3_PROXY_PORT;
  } else if (param == "vfs.s3.proxy_username") {
    param_values_["vfs.s3.proxy_username"] = VFS_S3_PROXY_USERNAME;
  } else if (param == "vfs.s3.proxy_password") {
    param_values_["vfs.s3.proxy_password"] = VFS_S3_PROXY_PASSWORD;
  } else if (param == "vfs.s3.verify_ssl") {
    param_values_["vfs.s3.verify_ssl"] = VFS_S3_VERIFY_SSL;
  } else if (param == "vfs.hdfs.name_node") {
    param_values_["vfs.hdfs.name_node_uri"] = VFS_HDFS_NAME_NODE_URI;
  } else if (param == "vfs.hdfs.username") {
    param_values_["vfs.hdfs.username"] = VFS_HDFS_USERNAME;
  } else if (param == "vfs.hdfs.kerb_ticket_cache_path") {
    param_values_["vfs.hdfs.kerb_ticket_cache_path"] =
        VFS_HDFS_KERB_TICKET_CACHE_PATH;
  }

  // Remove from the set parameters
  set_params_.erase(param);

  return Status::Ok();
}

void Config::inherit(const Config& config) {
  bool found;
  auto set_params = config.set_params();
  for (const auto& p : set_params) {
    auto v = config.get(p, &found);
    assert(found);
    set(p, v);
  }
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
  int vint = 0;

  if (param == "rest.server_serialization_format") {
    SerializationType serialization_type;
    RETURN_NOT_OK(serialization_type_enum(value, &serialization_type));
  } else if (param == "sm.dedup_coords") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.check_coord_dups") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.check_coord_oob") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.check_global_order") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.tile_cache_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.memory_budget") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.memory_budget_var") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.sub_partitioner_memory_budget") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.enable_signal_handlers") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "sm.compute_concurrency_level") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.io_concurrency_level") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.num_tbb_threads") {
    RETURN_NOT_OK(utils::parse::convert(value, &vint));
  } else if (param == "sm.consolidation.amplification") {
    RETURN_NOT_OK(utils::parse::convert(value, &vf));
  } else if (param == "sm.consolidation.buffer_size") {
    RETURN_NOT_OK(utils::parse::convert(value, &vuint64));
  } else if (param == "sm.consolidation.steps") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "sm.consolidation.step_min_frags") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "sm.consolidation.step_max_frags") {
    RETURN_NOT_OK(utils::parse::convert(value, &v32));
  } else if (param == "sm.consolidation.step_size_ratio") {
    RETURN_NOT_OK(utils::parse::convert(value, &vf));
  } else if (param == "vfs.min_parallel_size") {
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
  } else if (param == "vfs.file.enable_filelocks") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
  } else if (param == "vfs.s3.scheme") {
    if (value != "http" && value != "https")
      return LOG_STATUS(
          Status::ConfigError("Invalid S3 scheme parameter value"));
  } else if (param == "vfs.s3.use_virtual_addressing") {
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
  } else if (param == "vfs.s3.proxy_port") {
    RETURN_NOT_OK(utils::parse::convert(value, &vint64));
  } else if (param == "vfs.s3.verify_ssl") {
    RETURN_NOT_OK(utils::parse::convert(value, &v));
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

  // Check env if not found in config or if it was found in the config but is a
  // default value
  const char* value_env = get_from_env(param, found);
  if (*found)
    return value_env;

  // At this point the value was not found to be user set in the config or an
  // environmental variable so return any default value from the config or
  // indicate it was not found
  *found = found_config;
  return *found ? value_config : "";
}

// Explicit template instantiations
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

}  // namespace sm
}  // namespace tiledb
