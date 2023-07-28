/**
 * @file ssl_config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file includes definitions of the SSLConfig class.
 */

#include "tiledb/sm/filesystem/ssl_config.h"
#include "tiledb/common/logger.h"
#include "tiledb/platform/cert_file.h"

namespace tiledb::sm {

SSLConfig::SSLConfig()
    : ca_file_("")
    , ca_path_("")
    , verify_(true) {
}

SSLConfig::SSLConfig(const Config& cfg)
    : ca_file_("")
    , ca_path_("")
    , verify_(true) {
  // Look up our ca_file and ca_path configuration options
  auto ca_file = cfg.get<std::string>("ssl.ca_file");
  if (ca_file.has_value()) {
    ca_file_ = ca_file.value();
  }

  auto ca_path = cfg.get<std::string>("ssl.ca_path");
  if (ca_path.has_value()) {
    ca_path_ = ca_path.value();
  }

  if constexpr (tiledb::platform::PlatformCertFile::enabled) {
    // If neither ca_file or ca_path are set, we look for a system default
    // CA file on Linux platforms.
    if (ca_file_.empty() && ca_path_.empty()) {
      ca_file_ = tiledb::platform::PlatformCertFile::get();
    }
  }

  auto verify = cfg.get<bool>("ssl.verify");
  if (verify.has_value()) {
    verify_ = verify.value();
  }
}

S3SSLConfig::S3SSLConfig(const Config& cfg)
    : SSLConfig(cfg) {
  // Support the old s3 configuration values if they are
  // configured by the user.

  // Only set ca_file_ if vfs.s3.ca_file is a non-empty string
  auto ca_file = cfg.get<std::string>("vfs.s3.ca_file");
  if (ca_file.has_value() && !ca_file.value().empty()) {
    LOG_WARN(
        "The 'vfs.s3.ca_file' configuration option has been replaced "
        "with 'ssl.ca_file'. Make sure that you update your configuration "
        "because 'vfs.s3.ca_file' will eventually be removed.");
    ca_file_ = ca_file.value();
  }

  // Only set ca_path_ if vfs.s3.ca_path is a non-empty string
  auto ca_path = cfg.get<std::string>("vfs.s3.ca_path");
  if (ca_path.has_value() && !ca_path.value().empty()) {
    LOG_WARN(
        "The 'vfs.s3.ca_path' configuration option has been replaced "
        "with 'ssl.ca_path'. Make sure that you update your configuration "
        "because 'vfs.s3.ca_path' will eventually be removed.");
    ca_path_ = ca_path.value();
  }

  // Only override what was found in `ssl.verify` if `vfs.s3.verify_ssl` is
  // set to false (i.e., non-default). Otherwise this will always ignore the
  // ssl.verify value.
  auto verify = cfg.get<bool>("vfs.s3.verify_ssl");
  if (verify.has_value() && !verify.value()) {
    LOG_WARN(
        "The 'vfs.s3.verify_ssl' configuration option has been replaced "
        "with 'ssl.verify'. Make sure that you update your configuration "
        "because 'vfs.s3.verify_ssl' will eventually be removed.");
    verify_ = verify.value();
  }
}

RestSSLConfig::RestSSLConfig(const Config& cfg)
    : SSLConfig(cfg) {
  // Only override what was found in `ssl.verify` if
  // `rest.ignore_ssl_verification` is non-default (i.e., true, the naming here
  // is backwards from all the other ssl verification key names)
  auto skip_verify = cfg.get<bool>("rest.ignore_ssl_validation");
  if (skip_verify.has_value() && skip_verify.value()) {
    LOG_WARN(
        "The 'rest.ignore_ssl_validation = false' configuration option "
        "has been replaced with 'ssl.verify = true'. Make sure that you update "
        "your configuration because 'rest.ignore_ssl_validation' will "
        "eventually be removed.");
    verify_ = false;
  }
}

}  // namespace tiledb::sm
