/**
 * @file   config.cc
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
 * This file implements class Config.
 */

#include "tiledb/sm/storage_manager/config.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

#include <fstream>
#include <iostream>
#include <sstream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*        PRIVATE CONSTANTS       */
/* ****************************** */

const char Config::COMMENT_START = '#';

const std::set<std::string> Config::unserialized_params_ = {
    "vfs.s3.proxy_username",
    "vfs.s3.proxy_password",
    "vfs.s3.aws_access_key_id",
    "vfs.s3.aws_secret_access_key",
    "vfs.s3.aws_session_token"};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Config::Config() {
  set_default_param_values();
}

Config::Config(const VFSParams& vfs_params)
    : vfs_params_(vfs_params) {
  set_default_param_values();
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
    RETURN_NOT_OK(set(param, value));

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

Config::SMParams Config::sm_params() const {
  return sm_params_;
}

Config::ConsolidationParams Config::consolidation_params() const {
  return sm_params_.consolidation_params_;
}

Config::VFSParams Config::vfs_params() const {
  return vfs_params_;
}

Config::S3Params Config::s3_params() const {
  return vfs_params_.s3_params_;
}

Status Config::set(const std::string& param, const std::string& value) {
  param_values_[param] = value;

  if (param == "sm.dedup_coords") {
    RETURN_NOT_OK(set_sm_dedup_coords(value));
  } else if (param == "sm.check_coord_dups") {
    RETURN_NOT_OK(set_sm_check_coord_dups(value));
  } else if (param == "sm.check_coord_oob") {
    RETURN_NOT_OK(set_sm_check_coord_oob(value));
  } else if (param == "sm.check_global_order") {
    RETURN_NOT_OK(set_sm_check_global_order(value));
  } else if (param == "sm.tile_cache_size") {
    RETURN_NOT_OK(set_sm_tile_cache_size(value));
  } else if (param == "sm.memory_budget") {
    RETURN_NOT_OK(set_sm_memory_budget(value));
  } else if (param == "sm.memory_budget_var") {
    RETURN_NOT_OK(set_sm_memory_budget_var(value));
  } else if (param == "sm.consolidation.amplification") {
    RETURN_NOT_OK(set_consolidation_amplification(value));
  } else if (param == "sm.consolidation.buffer_size") {
    RETURN_NOT_OK(set_consolidation_buffer_size(value));
  } else if (param == "sm.array_schema_cache_size") {
    RETURN_NOT_OK(set_sm_array_schema_cache_size(value));
  } else if (param == "sm.fragment_metadata_cache_size") {
    RETURN_NOT_OK(set_sm_fragment_metadata_cache_size(value));
  } else if (param == "sm.enable_signal_handlers") {
    RETURN_NOT_OK(set_sm_enable_signal_handlers(value));
  } else if (param == "sm.num_async_threads") {
    RETURN_NOT_OK(set_sm_num_async_threads(value));
  } else if (param == "sm.num_reader_threads") {
    RETURN_NOT_OK(set_sm_num_reader_threads(value));
  } else if (param == "sm.num_writer_threads") {
    RETURN_NOT_OK(set_sm_num_writer_threads(value));
  } else if (param == "sm.num_tbb_threads") {
    RETURN_NOT_OK(set_sm_num_tbb_threads(value));
  } else if (param == "sm.consolidation.steps") {
    RETURN_NOT_OK(set_consolidation_steps(value));
  } else if (param == "sm.consolidation.step_min_frags") {
    RETURN_NOT_OK(set_consolidation_step_min_frags(value));
  } else if (param == "sm.consolidation.step_max_frags") {
    RETURN_NOT_OK(set_consolidation_step_max_frags(value));
  } else if (param == "sm.consolidation.step_size_ratio") {
    RETURN_NOT_OK(set_consolidation_step_size_ratio(value));
  } else if (param == "vfs.num_threads") {
    RETURN_NOT_OK(set_vfs_num_threads(value));
  } else if (param == "vfs.min_parallel_size") {
    RETURN_NOT_OK(set_vfs_min_parallel_size(value));
  } else if (param == "vfs.max_batch_read_size") {
    RETURN_NOT_OK(set_vfs_max_batch_read_size(value));
  } else if (param == "vfs.max_batch_read_amplification") {
    RETURN_NOT_OK(set_vfs_max_batch_read_amplification(value));
  } else if (param == "vfs.file.max_parallel_ops") {
    RETURN_NOT_OK(set_vfs_file_max_parallel_ops(value));
  } else if (param == "vfs.s3.region") {
    RETURN_NOT_OK(set_vfs_s3_region(value));
  } else if (param == "vfs.s3.aws_access_key_id") {
    RETURN_NOT_OK(set_vfs_s3_aws_access_key_id(value));
  } else if (param == "vfs.s3.aws_secret_access_key") {
    RETURN_NOT_OK(set_vfs_s3_aws_secret_access_key(value));
  } else if (param == "vfs.s3.aws_session_token") {
    RETURN_NOT_OK(set_vfs_s3_aws_session_token(value));
  } else if (param == "vfs.s3.scheme") {
    RETURN_NOT_OK(set_vfs_s3_scheme(value));
  } else if (param == "vfs.s3.endpoint_override") {
    RETURN_NOT_OK(set_vfs_s3_endpoint_override(value));
  } else if (param == "vfs.s3.use_virtual_addressing") {
    RETURN_NOT_OK(set_vfs_s3_use_virtual_addressing(value));
  } else if (param == "vfs.s3.max_parallel_ops") {
    RETURN_NOT_OK(set_vfs_s3_max_parallel_ops(value));
  } else if (param == "vfs.s3.multipart_part_size") {
    RETURN_NOT_OK(set_vfs_s3_multipart_part_size(value));
  } else if (param == "vfs.s3.connect_timeout_ms") {
    RETURN_NOT_OK(set_vfs_s3_connect_timeout_ms(value));
  } else if (param == "vfs.s3.connect_max_tries") {
    RETURN_NOT_OK(set_vfs_s3_connect_max_tries(value));
  } else if (param == "vfs.s3.connect_scale_factor") {
    RETURN_NOT_OK(set_vfs_s3_connect_scale_factor(value));
  } else if (param == "vfs.s3.request_timeout_ms") {
    RETURN_NOT_OK(set_vfs_s3_request_timeout_ms(value));
  } else if (param == "vfs.s3.proxy_scheme") {
    RETURN_NOT_OK(set_vfs_s3_proxy_scheme(value));
  } else if (param == "vfs.s3.proxy_host") {
    RETURN_NOT_OK(set_vfs_s3_proxy_host(value));
  } else if (param == "vfs.s3.proxy_port") {
    RETURN_NOT_OK(set_vfs_s3_proxy_port(value));
  } else if (param == "vfs.s3.proxy_username") {
    RETURN_NOT_OK(set_vfs_s3_proxy_username(value));
  } else if (param == "vfs.s3.proxy_password") {
    RETURN_NOT_OK(set_vfs_s3_proxy_password(value));
  } else if (param == "vfs.hdfs.name_node") {
    RETURN_NOT_OK(set_vfs_hdfs_name_node(value));
  } else if (param == "vfs.hdfs.username") {
    RETURN_NOT_OK(set_vfs_hdfs_username(value));
  } else if (param == "vfs.hdfs.kerb_ticket_cache_path") {
    RETURN_NOT_OK(set_vfs_hdfs_kerb_ticket_cache_path(value))
  }

  // If param does not exist, it is ignored

  return Status::Ok();
}

Status Config::get(const std::string& param, const char** value) const {
  auto it = param_values_.find(param);
  *value = (it == param_values_.end()) ? nullptr : it->second.c_str();
  return Status::Ok();
}

std::map<std::string, std::string> Config::param_values(
    const std::string& prefix) const {
  if (prefix.empty())
    return param_values_;

  std::map<std::string, std::string> ret;
  for (auto& pv : param_values_) {
    if (pv.first.find(prefix) == 0)
      ret[pv.first.substr(prefix.size())] = pv.second;
  }

  return ret;
};

Status Config::unset(const std::string& param) {
  std::stringstream value;

  // Set back to default
  if (param == "sm.dedup_coords") {
    sm_params_.dedup_coords_ = constants::dedup_coords;
    value << (sm_params_.dedup_coords_ ? "true" : "false");
    param_values_["sm.dedup_coords"] = value.str();
    value.str(std::string());
  } else if (param == "sm.check_coord_dups") {
    sm_params_.check_coord_dups_ = constants::check_coord_dups;
    value << (sm_params_.check_coord_dups_ ? "true" : "false");
    param_values_["sm.check_coord_dups"] = value.str();
    value.str(std::string());
  } else if (param == "sm.check_coord_oob") {
    sm_params_.check_coord_oob_ = constants::check_coord_oob;
    value << (sm_params_.check_coord_oob_ ? "true" : "false");
    param_values_["sm.check_coord_oob"] = value.str();
    value.str(std::string());
  } else if (param == "sm.check_global_order") {
    sm_params_.check_global_order_ = constants::check_coord_dups;
    value << (sm_params_.check_global_order_ ? "true" : "false");
    param_values_["sm.check_global_order"] = value.str();
    value.str(std::string());
  } else if (param == "sm.tile_cache_size") {
    sm_params_.tile_cache_size_ = constants::tile_cache_size;
    value << sm_params_.tile_cache_size_;
    param_values_["sm.tile_cache_size"] = value.str();
    value.str(std::string());
  } else if (param == "sm.memory_budget") {
    sm_params_.memory_budget_ = constants::memory_budget_fixed;
    value << sm_params_.memory_budget_;
    param_values_["sm.memory_budget"] = value.str();
    value.str(std::string());
  } else if (param == "sm.memory_budget_var") {
    sm_params_.memory_budget_var_ = constants::memory_budget_var;
    value << sm_params_.memory_budget_var_;
    param_values_["sm.memory_budget_var"] = value.str();
    value.str(std::string());
  } else if (param == "sm.consolidation.amplification") {
    sm_params_.consolidation_params_.amplification_ =
        constants::consolidation_amplification;
    value << sm_params_.consolidation_params_.amplification_;
    param_values_["sm.consolidation.amplification"] = value.str();
    value.str(std::string());
  } else if (param == "sm.consolidation.buffer_size") {
    sm_params_.consolidation_params_.buffer_size_ =
        constants::consolidation_buffer_size;
    value << sm_params_.consolidation_params_.buffer_size_;
    param_values_["sm.consolidation.buffer_size"] = value.str();
    value.str(std::string());
  } else if (param == "sm.array_schema_cache_size") {
    sm_params_.array_schema_cache_size_ = constants::array_schema_cache_size;
    value << sm_params_.array_schema_cache_size_;
    param_values_["sm.array_schema_cache_size"] = value.str();
    value.str(std::string());
  } else if (param == "sm.fragment_metadata_cache_size") {
    sm_params_.fragment_metadata_cache_size_ =
        constants::fragment_metadata_cache_size;
    value << sm_params_.fragment_metadata_cache_size_;
    param_values_["sm.fragment_metadata_cache_size"] = value.str();
    value.str(std::string());
  } else if (param == "sm.enable_signal_handlers") {
    sm_params_.enable_signal_handlers_ = constants::enable_signal_handlers;
    value << (sm_params_.enable_signal_handlers_ ? "true" : "false");
    param_values_["sm.enable_signal_handlers"] = value.str();
    value.str(std::string());
  } else if (param == "sm.num_async_threads") {
    sm_params_.num_async_threads_ = constants::num_async_threads;
    value << sm_params_.num_async_threads_;
    param_values_["sm.num_async_threads"] = value.str();
    value.str(std::string());
  } else if (param == "sm.num_reader_threads") {
    sm_params_.num_reader_threads_ = constants::num_reader_threads;
    value << sm_params_.num_reader_threads_;
    param_values_["sm.num_reader_threads"] = value.str();
    value.str(std::string());
  } else if (param == "sm.num_writer_threads") {
    sm_params_.num_writer_threads_ = constants::num_writer_threads;
    value << sm_params_.num_writer_threads_;
    param_values_["sm.num_writer_threads"] = value.str();
    value.str(std::string());
  } else if (param == "sm.num_tbb_threads") {
    sm_params_.num_tbb_threads_ = constants::num_tbb_threads;
    value << sm_params_.num_tbb_threads_;
    param_values_["sm.num_tbb_threads"] = value.str();
    value.str(std::string());
  } else if (param == "sm.consolidation.steps") {
    sm_params_.consolidation_params_.steps_ = constants::consolidation_steps;
    value << sm_params_.consolidation_params_.steps_;
    param_values_["sm.consolidation.steps"] = value.str();
    value.str(std::string());
  } else if (param == "sm.consolidation.step_min_frags") {
    sm_params_.consolidation_params_.step_min_frags_ =
        constants::consolidation_step_min_frags;
    value << sm_params_.consolidation_params_.step_min_frags_;
    param_values_["sm.consolidation.step_min_frags"] = value.str();
    value.str(std::string());
  } else if (param == "sm.consolidation.step_max_frags") {
    sm_params_.consolidation_params_.step_max_frags_ =
        constants::consolidation_step_max_frags;
    value << sm_params_.consolidation_params_.step_max_frags_;
    param_values_["sm.consolidation.step_max_frags"] = value.str();
    value.str(std::string());
  } else if (param == "sm.consolidation.step_size_ratio") {
    sm_params_.consolidation_params_.step_size_ratio_ =
        constants::consolidation_step_size_ratio;
    value << sm_params_.consolidation_params_.step_size_ratio_;
    param_values_["sm.consolidation.step_size_ratio"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.num_threads") {
    vfs_params_.num_threads_ = constants::vfs_num_threads;
    value << vfs_params_.num_threads_;
    param_values_["vfs.num_threads"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.min_parallel_size") {
    vfs_params_.min_parallel_size_ = constants::vfs_min_parallel_size;
    value << vfs_params_.min_parallel_size_;
    param_values_["vfs.min_parallel_size"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.max_batch_read_size") {
    vfs_params_.max_batch_read_size_ = constants::vfs_max_batch_read_size;
    value << vfs_params_.max_batch_read_size_;
    param_values_["vfs.max_batch_read_size"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.max_batch_read_amplification") {
    vfs_params_.max_batch_read_amplification_ =
        constants::vfs_max_batch_read_amplification;
    value << vfs_params_.max_batch_read_amplification_;
    param_values_["vfs.max_batch_read_amplification"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.file.max_parallel_ops") {
    vfs_params_.file_params_.max_parallel_ops_ =
        constants::vfs_file_max_parallel_ops;
    value << vfs_params_.file_params_.max_parallel_ops_;
    param_values_["vfs.file.max_parallel_ops"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.region") {
    vfs_params_.s3_params_.region_ = constants::s3_region;
    value << vfs_params_.s3_params_.region_;
    param_values_["vfs.s3.region"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.aws_access_key_id") {
    vfs_params_.s3_params_.aws_access_key_id = "";
    value << vfs_params_.s3_params_.aws_access_key_id;
    param_values_["vfs.s3.aws_access_key_id"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.aws_secret_access_key") {
    vfs_params_.s3_params_.aws_secret_access_key = "";
    value << vfs_params_.s3_params_.aws_secret_access_key;
    param_values_["vfs.s3.aws_secret_access_key"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.aws_session_token") {
    vfs_params_.s3_params_.aws_session_token = "";
    value << vfs_params_.s3_params_.aws_session_token;
    param_values_["vfs.s3.aws_session_token"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.scheme") {
    vfs_params_.s3_params_.scheme_ = constants::s3_scheme;
    value << vfs_params_.s3_params_.scheme_;
    param_values_["vfs.s3.scheme"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.endpoint_override") {
    vfs_params_.s3_params_.endpoint_override_ = constants::s3_endpoint_override;
    value << vfs_params_.s3_params_.endpoint_override_;
    param_values_["vfs.s3.endpoint_override"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.use_virtual_addressing") {
    vfs_params_.s3_params_.use_virtual_addressing_ =
        constants::s3_use_virtual_addressing;
    value
        << ((vfs_params_.s3_params_.use_virtual_addressing_) ? "true" :
                                                               "false");
    param_values_["vfs.s3.use_virtual_addressing"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.max_parallel_ops") {
    vfs_params_.s3_params_.max_parallel_ops_ = constants::s3_max_parallel_ops;
    value << vfs_params_.s3_params_.max_parallel_ops_;
    param_values_["vfs.s3.max_parallel_ops"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.multipart_part_size") {
    vfs_params_.s3_params_.multipart_part_size_ =
        constants::s3_multipart_part_size;
    value << vfs_params_.s3_params_.multipart_part_size_;
    param_values_["vfs.s3.multipart_part_size"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.connect_timeout_ms") {
    vfs_params_.s3_params_.connect_timeout_ms_ =
        constants::s3_connect_timeout_ms;
    value << vfs_params_.s3_params_.connect_timeout_ms_;
    param_values_["vfs.s3.connect_timeout_ms"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.connect_max_tries") {
    vfs_params_.s3_params_.connect_max_tries_ = constants::s3_connect_max_tries;
    value << vfs_params_.s3_params_.connect_max_tries_;
    param_values_["vfs.s3.connect_max_tries"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.connect_scale_factor") {
    vfs_params_.s3_params_.connect_scale_factor_ =
        constants::s3_connect_scale_factor;
    value << vfs_params_.s3_params_.connect_scale_factor_;
    param_values_["vfs.s3.connect_scale_factor"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.request_timeout_ms") {
    vfs_params_.s3_params_.request_timeout_ms_ =
        constants::s3_request_timeout_ms;
    value << vfs_params_.s3_params_.request_timeout_ms_;
    param_values_["vfs.s3.request_timeout_ms"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.proxy_scheme") {
    vfs_params_.s3_params_.proxy_scheme_ = constants::s3_proxy_scheme;
    value << vfs_params_.s3_params_.proxy_scheme_;
    param_values_["vfs.s3.proxy_scheme"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.proxy_host") {
    vfs_params_.s3_params_.proxy_host_ = constants::s3_proxy_host;
    value << vfs_params_.s3_params_.proxy_host_;
    param_values_["vfs.s3.proxy_host"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.proxy_port") {
    vfs_params_.s3_params_.proxy_port_ = constants::s3_proxy_port;
    value << vfs_params_.s3_params_.proxy_port_;
    param_values_["vfs.s3.proxy_port"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.proxy_username") {
    vfs_params_.s3_params_.proxy_username_ = constants::s3_proxy_username;
    value << vfs_params_.s3_params_.proxy_username_;
    param_values_["vfs.s3.proxy_username"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.s3.proxy_password") {
    vfs_params_.s3_params_.proxy_password_ = constants::s3_proxy_password;
    value << vfs_params_.s3_params_.proxy_password_;
    param_values_["vfs.s3.proxy_password"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.hdfs.name_node") {
    vfs_params_.hdfs_params_.name_node_uri_ = constants::hdfs_name_node_uri;
    value << vfs_params_.hdfs_params_.name_node_uri_;
    param_values_["vfs.hdfs.name_node_uri"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.hdfs.username") {
    vfs_params_.hdfs_params_.username_ = constants::hdfs_username;
    value << vfs_params_.hdfs_params_.username_;
    param_values_["vfs.hdfs.username"] = value.str();
    value.str(std::string());
  } else if (param == "vfs.hdfs.kerb_ticket_cache_path") {
    vfs_params_.hdfs_params_.kerb_ticket_cache_path_ =
        constants::hdfs_kerb_ticket_cache_path;
    value << vfs_params_.hdfs_params_.kerb_ticket_cache_path_;
    param_values_["vfs.hdfs.kerb_ticket_cache_path"] = value.str();
    value.str(std::string());
  }

  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Config::set_default_param_values() {
  std::stringstream value;

  value << (sm_params_.dedup_coords_ ? "true" : "false");
  param_values_["sm.dedup_coords"] = value.str();
  value.str(std::string());

  value << (sm_params_.check_coord_dups_ ? "true" : "false");
  param_values_["sm.check_coord_dups"] = value.str();
  value.str(std::string());

  value << (sm_params_.check_coord_oob_ ? "true" : "false");
  param_values_["sm.check_coord_oob"] = value.str();
  value.str(std::string());

  value << (sm_params_.check_global_order_ ? "true" : "false");
  param_values_["sm.check_global_order"] = value.str();
  value.str(std::string());

  value << sm_params_.tile_cache_size_;
  param_values_["sm.tile_cache_size"] = value.str();
  value.str(std::string());

  value << sm_params_.memory_budget_;
  param_values_["sm.memory_budget"] = value.str();
  value.str(std::string());

  value << sm_params_.memory_budget_var_;
  param_values_["sm.memory_budget_var"] = value.str();
  value.str(std::string());

  value << sm_params_.consolidation_params_.amplification_;
  param_values_["sm.consolidation.amplification"] = value.str();
  value.str(std::string());

  value << sm_params_.consolidation_params_.buffer_size_;
  param_values_["sm.consolidation.buffer_size"] = value.str();
  value.str(std::string());

  value << sm_params_.array_schema_cache_size_;
  param_values_["sm.array_schema_cache_size"] = value.str();
  value.str(std::string());

  value << sm_params_.fragment_metadata_cache_size_;
  param_values_["sm.fragment_metadata_cache_size"] = value.str();
  value.str(std::string());

  value << (sm_params_.enable_signal_handlers_ ? "true" : "false");
  param_values_["sm.enable_signal_handlers"] = value.str();
  value.str(std::string());

  value << sm_params_.num_async_threads_;
  param_values_["sm.num_async_threads"] = value.str();
  value.str(std::string());

  value << sm_params_.num_reader_threads_;
  param_values_["sm.num_reader_threads"] = value.str();
  value.str(std::string());

  value << sm_params_.num_writer_threads_;
  param_values_["sm.num_writer_threads"] = value.str();
  value.str(std::string());

  value << sm_params_.num_tbb_threads_;
  param_values_["sm.num_tbb_threads"] = value.str();
  value.str(std::string());

  value << sm_params_.consolidation_params_.steps_;
  param_values_["sm.consolidation.steps"] = value.str();
  value.str(std::string());

  value << sm_params_.consolidation_params_.step_min_frags_;
  param_values_["sm.consolidation.step_min_frags"] = value.str();
  value.str(std::string());

  value << sm_params_.consolidation_params_.step_max_frags_;
  param_values_["sm.consolidation.step_max_frags"] = value.str();
  value.str(std::string());

  value << sm_params_.consolidation_params_.step_size_ratio_;
  param_values_["sm.consolidation.step_size_ratio"] = value.str();
  value.str(std::string());

  value << vfs_params_.num_threads_;
  param_values_["vfs.num_threads"] = value.str();
  value.str(std::string());

  value << vfs_params_.min_parallel_size_;
  param_values_["vfs.min_parallel_size"] = value.str();
  value.str(std::string());

  value << vfs_params_.max_batch_read_size_;
  param_values_["vfs.max_batch_read_size"] = value.str();
  value.str(std::string());

  value << vfs_params_.max_batch_read_amplification_;
  param_values_["vfs.max_batch_read_amplification"] = value.str();
  value.str(std::string());

  value << vfs_params_.file_params_.max_parallel_ops_;
  param_values_["vfs.file.max_parallel_ops"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.region_;
  param_values_["vfs.s3.region"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.aws_access_key_id;
  param_values_["vfs.s3.aws_access_key_id"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.aws_secret_access_key;
  param_values_["vfs.s3.aws_secret_access_key"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.scheme_;
  param_values_["vfs.s3.scheme"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.endpoint_override_;
  param_values_["vfs.s3.endpoint_override"] = value.str();
  value.str(std::string());

  value
      << ((vfs_params_.s3_params_.use_virtual_addressing_) ? "true" : "false");
  param_values_["vfs.s3.use_virtual_addressing"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.max_parallel_ops_;
  param_values_["vfs.s3.max_parallel_ops"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.multipart_part_size_;
  param_values_["vfs.s3.multipart_part_size"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.connect_timeout_ms_;
  param_values_["vfs.s3.connect_timeout_ms"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.connect_max_tries_;
  param_values_["vfs.s3.connect_max_tries"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.connect_scale_factor_;
  param_values_["vfs.s3.connect_scale_factor"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.request_timeout_ms_;
  param_values_["vfs.s3.request_timeout_ms"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.proxy_scheme_;
  param_values_["vfs.s3.proxy_scheme"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.proxy_host_;
  param_values_["vfs.s3.proxy_host"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.proxy_port_;
  param_values_["vfs.s3.proxy_port"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.proxy_username_;
  param_values_["vfs.s3.proxy_username"] = value.str();
  value.str(std::string());

  value << vfs_params_.s3_params_.proxy_password_;
  param_values_["vfs.s3.proxy_password"] = value.str();
  value.str(std::string());

  value << vfs_params_.hdfs_params_.name_node_uri_;
  param_values_["vfs.hdfs.name_node_uri"] = value.str();
  value.str(std::string());

  value << vfs_params_.hdfs_params_.username_;
  param_values_["vfs.hdfs.username"] = value.str();
  value.str(std::string());

  value << vfs_params_.hdfs_params_.kerb_ticket_cache_path_;
  param_values_["vfs.hdfs.kerb_ticket_cache_path"] = value.str();
  value.str(std::string());
}

Status Config::parse_bool(const std::string& value, bool* result) {
  std::string lvalue = value;
  std::transform(lvalue.begin(), lvalue.end(), lvalue.begin(), ::tolower);
  if (lvalue == "true") {
    *result = true;
  } else if (lvalue == "false") {
    *result = false;
  } else {
    return Status::ConfigError("cannot parse boolean value: " + value);
  }
  return Status::Ok();
}

Status Config::set_sm_dedup_coords(const std::string& value) {
  bool v = false;
  if (!parse_bool(value, &v).ok()) {
    return LOG_STATUS(Status::ConfigError(
        "Cannot set parameter; Invalid dedup coords value"));
  }
  sm_params_.dedup_coords_ = v;
  return Status::Ok();
}

Status Config::set_sm_check_coord_dups(const std::string& value) {
  bool v = false;
  if (!parse_bool(value, &v).ok()) {
    return LOG_STATUS(Status::ConfigError(
        "Cannot set parameter; Invalid check coords duplicates value"));
  }
  sm_params_.check_coord_dups_ = v;
  return Status::Ok();
}

Status Config::set_sm_check_coord_oob(const std::string& value) {
  bool v = false;
  if (!parse_bool(value, &v).ok()) {
    return LOG_STATUS(Status::ConfigError(
        "Cannot set parameter; Invalid check out-of-bounds coords value"));
  }
  sm_params_.check_coord_oob_ = v;
  return Status::Ok();
}

Status Config::set_sm_check_global_order(const std::string& value) {
  bool v = false;
  if (!parse_bool(value, &v).ok()) {
    return LOG_STATUS(Status::ConfigError(
        "Cannot set parameter; Invalid check global order value"));
  }
  sm_params_.check_global_order_ = v;
  return Status::Ok();
}

Status Config::set_sm_array_schema_cache_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.array_schema_cache_size_ = v;

  return Status::Ok();
}

Status Config::set_sm_fragment_metadata_cache_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.fragment_metadata_cache_size_ = v;

  return Status::Ok();
}

Status Config::set_sm_enable_signal_handlers(const std::string& value) {
  bool v;
  RETURN_NOT_OK(parse_bool(value, &v));
  sm_params_.enable_signal_handlers_ = v;

  return Status::Ok();
}

Status Config::set_sm_num_async_threads(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.num_async_threads_ = v;

  return Status::Ok();
}

Status Config::set_sm_num_reader_threads(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.num_reader_threads_ = v;

  return Status::Ok();
}

Status Config::set_sm_num_writer_threads(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.num_writer_threads_ = v;

  return Status::Ok();
}

Status Config::set_sm_num_tbb_threads(const std::string& value) {
  int v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.num_tbb_threads_ = v;

  return Status::Ok();
}

Status Config::set_consolidation_steps(const std::string& value) {
  uint32_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.consolidation_params_.steps_ = v;

  return Status::Ok();
}

Status Config::set_consolidation_step_min_frags(const std::string& value) {
  uint32_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.consolidation_params_.step_min_frags_ = v;

  return Status::Ok();
}

Status Config::set_consolidation_step_max_frags(const std::string& value) {
  uint32_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.consolidation_params_.step_max_frags_ = v;

  return Status::Ok();
}

Status Config::set_consolidation_step_size_ratio(const std::string& value) {
  float v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.consolidation_params_.step_size_ratio_ = v;

  return Status::Ok();
}

Status Config::set_sm_tile_cache_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.tile_cache_size_ = v;

  return Status::Ok();
}

Status Config::set_sm_memory_budget(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.memory_budget_ = v;

  return Status::Ok();
}

Status Config::set_sm_memory_budget_var(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.memory_budget_var_ = v;

  return Status::Ok();
}

Status Config::set_consolidation_amplification(const std::string& value) {
  float v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.consolidation_params_.amplification_ = v;

  return Status::Ok();
}

Status Config::set_consolidation_buffer_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  sm_params_.consolidation_params_.buffer_size_ = v;

  return Status::Ok();
}

Status Config::set_vfs_num_threads(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.num_threads_ = v;

  return Status::Ok();
}

Status Config::set_vfs_min_parallel_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.min_parallel_size_ = v;

  return Status::Ok();
}

Status Config::set_vfs_max_batch_read_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.max_batch_read_size_ = v;

  return Status::Ok();
}

Status Config::set_vfs_max_batch_read_amplification(const std::string& value) {
  float v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.max_batch_read_amplification_ = v;

  return Status::Ok();
}

Status Config::set_vfs_file_max_parallel_ops(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.file_params_.max_parallel_ops_ = v;

  return Status::Ok();
}

Status Config::set_vfs_s3_region(const std::string& value) {
  vfs_params_.s3_params_.region_ = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_aws_access_key_id(const std::string& value) {
  vfs_params_.s3_params_.aws_access_key_id = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_aws_secret_access_key(const std::string& value) {
  vfs_params_.s3_params_.aws_secret_access_key = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_aws_session_token(const std::string& value) {
  vfs_params_.s3_params_.aws_session_token = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_scheme(const std::string& value) {
  if (value != "http" && value != "https")
    return LOG_STATUS(
        Status::ConfigError("Cannot set parameter; Invalid S3 scheme"));
  vfs_params_.s3_params_.scheme_ = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_endpoint_override(const std::string& value) {
  vfs_params_.s3_params_.endpoint_override_ = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_use_virtual_addressing(const std::string& value) {
  bool v = false;
  if (!parse_bool(value, &v).ok()) {
    return LOG_STATUS(Status::ConfigError(
        "Cannot set parameter; Invalid S3 virtual addressing value"));
  }
  vfs_params_.s3_params_.use_virtual_addressing_ = v;
  return Status::Ok();
}

Status Config::set_vfs_s3_max_parallel_ops(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.s3_params_.max_parallel_ops_ = v;

  return Status::Ok();
}

Status Config::set_vfs_s3_multipart_part_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.s3_params_.multipart_part_size_ = v;

  return Status::Ok();
}

Status Config::set_vfs_s3_connect_timeout_ms(const std::string& value) {
  long v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.s3_params_.connect_timeout_ms_ = v;
  return Status::Ok();
}

Status Config::set_vfs_s3_connect_max_tries(const std::string& value) {
  long v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.s3_params_.connect_max_tries_ = v;
  return Status::Ok();
}

Status Config::set_vfs_s3_connect_scale_factor(const std::string& value) {
  long v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.s3_params_.connect_scale_factor_ = v;
  return Status::Ok();
}

Status Config::set_vfs_s3_request_timeout_ms(const std::string& value) {
  long v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.s3_params_.request_timeout_ms_ = v;

  return Status::Ok();
}

Status Config::set_vfs_s3_proxy_scheme(const std::string& value) {
  vfs_params_.s3_params_.proxy_scheme_ = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_proxy_host(const std::string& value) {
  vfs_params_.s3_params_.proxy_host_ = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_proxy_port(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  vfs_params_.s3_params_.proxy_port_ = static_cast<unsigned>(v);
  return Status::Ok();
}

Status Config::set_vfs_s3_proxy_username(const std::string& value) {
  vfs_params_.s3_params_.proxy_username_ = value;
  return Status::Ok();
}

Status Config::set_vfs_s3_proxy_password(const std::string& value) {
  vfs_params_.s3_params_.proxy_password_ = value;
  return Status::Ok();
}

Status Config::set_vfs_hdfs_name_node(const std::string& value) {
  vfs_params_.hdfs_params_.name_node_uri_ = value;
  return Status::Ok();
}

Status Config::set_vfs_hdfs_username(const std::string& value) {
  vfs_params_.hdfs_params_.username_ = value;
  return Status::Ok();
}

Status Config::set_vfs_hdfs_kerb_ticket_cache_path(const std::string& value) {
  vfs_params_.hdfs_params_.kerb_ticket_cache_path_ = value;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
