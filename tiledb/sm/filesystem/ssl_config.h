/**
 * @file ssl_config.h
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
 * This file defines the SSLConfig class.
 */

#ifndef TILEDB_SSL_CONFIG_H
#define TILEDB_SSL_CONFIG_H

#include "tiledb/sm/config/config.h"

namespace tiledb::sm {

class SSLConfig {
 public:
  SSLConfig();
  SSLConfig(const Config& cfg);

  /** Return the CAFile config value. */
  inline const std::string& ca_file() const {
    return ca_file_;
  }

  /** Return the CAPath config value. */
  inline const std::string& ca_path() const {
    return ca_path_;
  }

  /** Return whether or not SSL verification should be performed. */
  inline bool verify() const {
    return verify_;
  }

 protected:
  /** Stores a (maybe empty) path to the configured CAFile path. */
  std::string ca_file_;

  /** Stores a (maybe empty) path to the configured CAPath directory. */
  std::string ca_path_;

  /** Stores whether we want to verify SSL connections or not. */
  bool verify_;
};

class S3SSLConfig : public SSLConfig {
 public:
  S3SSLConfig(const Config& cfg);
};

class RestSSLConfig : public SSLConfig {
 public:
  RestSSLConfig(const Config& cfg);
};

}  // namespace tiledb::sm

#endif  // TILEDB_SSL_CONFIG_H
