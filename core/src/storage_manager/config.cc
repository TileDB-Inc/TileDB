/**
 * @file   config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
#include "constants.h"
#include "logger.h"
#include "utils.h"

#include <fstream>
#include <iostream>
#include <sstream>

namespace tiledb {

/* ****************************** */
/*        PRIVATE CONSTANTS       */
/* ****************************** */

const char Config::COMMENT_START = '#';

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Config::Config() {
  set_default_tiledb_params();
#ifdef HAVE_S3
  set_default_tiledb_s3_params();
#endif
}

Config::~Config() = default;

/* ****************************** */
/*                API             */
/* ****************************** */

uint64_t Config::get_tiledb_array_metadata_cache_size() const {
  return tiledb_params_.array_metadata_cache_size_;
}

uint64_t Config::get_tiledb_fragment_metadata_cache_size() const {
  return tiledb_params_.fragment_metadata_cache_size_;
}

uint64_t Config::get_tiledb_tile_cache_size() const {
  return tiledb_params_.tile_cache_size_;
}

#ifdef HAVE_S3
std::string Config::get_tiledb_s3_region() const {
  return tiledb_s3_params_.region_;
}

std::string Config::get_tiledb_s3_scheme() const {
  return tiledb_s3_params_.scheme_;
}

std::string Config::get_tiledb_s3_endpoint_override() const {
  return tiledb_s3_params_.endpoint_override_;
}

bool Config::get_tiledb_s3_use_virtual_addressing() const {
  return tiledb_s3_params_.use_virtual_addressing_;
}

uint64_t Config::get_tiledb_s3_file_buffer_size() const {
  return tiledb_s3_params_.file_buffer_size_;
}

long Config::get_tiledb_s3_connect_timeout_ms() const {
  return tiledb_s3_params_.connect_timeout_ms_;
}

long Config::get_tiledb_s3_request_timeout_ms() const {
  return tiledb_s3_params_.request_timeout_ms_;
}
#endif

Status Config::init() {
  RETURN_NOT_OK(set_from_file());

  for (auto& pv : param_values_) {
    if (pv.first == "tiledb.tile_cache_size") {
      RETURN_NOT_OK(set_tiledb_tile_cache_size(pv.second));
    } else if (pv.first == "tiledb.array_metadata_cache_size") {
      RETURN_NOT_OK(set_tiledb_array_metadata_cache_size(pv.second));
    } else if (pv.first == "tiledb.fragment_metadata_cache_size") {
      RETURN_NOT_OK(set_tiledb_fragment_metadata_cache_size(pv.second));
#ifdef HAVE_S3
    } else if (pv.first == "tiledb.s3.region") {
      RETURN_NOT_OK(set_tiledb_s3_region(pv.second));
    } else if (pv.first == "tiledb.s3.scheme") {
      RETURN_NOT_OK(set_tiledb_s3_scheme(pv.second));
    } else if (pv.first == "tiledb.s3.endpoint_override") {
      RETURN_NOT_OK(set_tiledb_s3_endpoint_override(pv.second));
    } else if (pv.first == "tiledb.s3.use_virtual_addressing") {
      RETURN_NOT_OK(set_tiledb_s3_use_virtual_addressing(pv.second));
    } else if (pv.first == "tiledb.s3.file_buffer_size") {
      RETURN_NOT_OK(set_tiledb_s3_file_buffer_size(pv.second));
    } else if (pv.first == "tiledb.s3.connect_timeout_ms") {
      RETURN_NOT_OK(set_tiledb_s3_connect_timeout_ms(pv.second));
    } else if (pv.first == "tiledb.s3.request_timeout_ms") {
      RETURN_NOT_OK(set_tiledb_s3_request_timeout_ms(pv.second));
#endif
    } else {
      return LOG_STATUS(Status::ConfigError(
          std::string("Invalid config parameter '") + pv.first + "'"));
    }
  }

  return Status::Ok();
}

Status Config::set(const std::string& param, const std::string& value) {
  param_values_[param] = value;
  return Status::Ok();
}

Status Config::set_config_filename(const std::string& filename) {
  config_filename_ = filename;
  return Status::Ok();
}

Status Config::set_tiledb_array_metadata_cache_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  tiledb_params_.array_metadata_cache_size_ = v;

  return Status::Ok();
}

Status Config::set_tiledb_fragment_metadata_cache_size(
    const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  tiledb_params_.fragment_metadata_cache_size_ = v;

  return Status::Ok();
}

Status Config::set_tiledb_tile_cache_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  tiledb_params_.tile_cache_size_ = v;

  return Status::Ok();
}

#ifdef HAVE_S3
Status Config::set_tiledb_s3_region(const std::string& value) {
  tiledb_s3_params_.region_ = value;
  return Status::Ok();
}

Status Config::set_tiledb_s3_scheme(const std::string& value) {
  if (value != "http" && value != "https")
    return LOG_STATUS(
        Status::ConfigError("Cannot set parameter; Invalid S3 scheme"));
  tiledb_s3_params_.scheme_ = value;
  return Status::Ok();
}

Status Config::set_tiledb_s3_endpoint_override(const std::string& value) {
  tiledb_s3_params_.endpoint_override_ = value;
  return Status::Ok();
}

Status Config::set_tiledb_s3_use_virtual_addressing(const std::string& value) {
  if (value != "true" && value != "false")
    return LOG_STATUS(Status::ConfigError(
        "Cannot set parameter; Invalid S3 use virtual addressing"));

  tiledb_s3_params_.use_virtual_addressing_ = (value == "true");
  return Status::Ok();
}

Status Config::set_tiledb_s3_file_buffer_size(const std::string& value) {
  uint64_t v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  tiledb_s3_params_.file_buffer_size_ = v;

  return Status::Ok();
}

Status Config::set_tiledb_s3_connect_timeout_ms(const std::string& value) {
  long v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  tiledb_s3_params_.connect_timeout_ms_ = v;
  return Status::Ok();
}

Status Config::set_tiledb_s3_request_timeout_ms(const std::string& value) {
  long v;
  RETURN_NOT_OK(utils::parse::convert(value, &v));
  tiledb_s3_params_.request_timeout_ms_ = v;

  return Status::Ok();
}
#endif

Status Config::unset(const std::string& param) {
  param_values_.erase(param);
  return Status::Ok();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Config::set_default_tiledb_params() {
  tiledb_params_.array_metadata_cache_size_ =
      constants::array_metadata_cache_size;
  tiledb_params_.fragment_metadata_cache_size_ =
      constants::fragment_metadata_cache_size;
  tiledb_params_.tile_cache_size_ = constants::tile_cache_size;
}

#ifdef HAVE_S3
void Config::set_default_tiledb_s3_params() {
  tiledb_s3_params_.region_ = constants::s3_region;
  tiledb_s3_params_.scheme_ = constants::s3_scheme;
  tiledb_s3_params_.endpoint_override_ = constants::s3_endpoint_override;
  tiledb_s3_params_.use_virtual_addressing_ =
      constants::s3_use_virtual_addressing;
  tiledb_s3_params_.file_buffer_size_ = constants::s3_file_buffer_size;
  tiledb_s3_params_.connect_timeout_ms_ = constants::s3_connect_timeout_ms;
  tiledb_s3_params_.request_timeout_ms_ = constants::s3_request_timeout_ms;
}
#endif

Status Config::set_from_file() {
  // Do nothing if file not set
  if (config_filename_.empty())
    return Status::Ok();

  std::ifstream ifs(config_filename_);
  if (!ifs.is_open())
    return LOG_STATUS(Status::ConfigError("Failed to open config file"));

  std::string param, value, extra;
  for (std::string line; std::getline(ifs, line);) {
    std::stringstream line_ss(line);

    // Parse parameter
    line_ss >> param;
    if (param.empty() || param[0] == COMMENT_START)
      continue;

    // Parse value
    line_ss >> value;
    if (value.empty())
      return LOG_STATUS(Status::ConfigError(
          "Failed to parse config file; Missing parameter value"));

    // Parse extra
    line_ss >> extra;
    if (!extra.empty() && extra[0] != COMMENT_START)
      return LOG_STATUS(Status::ConfigError(
          "Failed to parse config file; Invalid line format"));

    // Set the param/value pair, only if it does not exist
    if (param_values_.find(param) == param_values_.end())
      param_values_[param] = value;
  }

  return Status::Ok();
}

}  // namespace tiledb