/**
 * @file tiledb/api/c_api/config/config_api_internal.h
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
 * This file declares the configuration section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_CONFIG_INTERNAL_H
#define TILEDB_CAPI_CONFIG_INTERNAL_H

#include <string>
#include "../error/error_api_internal.h"
#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/config/config_iter.h"

/**
 * Handle `struct` for API configuration objects.
 */
struct tiledb_config_handle_t : public tiledb::api::CAPIHandle {
  using config_type = tiledb::sm::Config;
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"configuration"};

 private:
  config_type config_;

 public:
  explicit tiledb_config_handle_t(config_type&& config)
      : config_(config) {
  }
  explicit tiledb_config_handle_t(const config_type& config)
      : config_(config) {
  }

  [[nodiscard]] inline config_type& config() {
    return config_;
  }
  [[nodiscard]] inline const config_type& config() const {
    return config_;
  }
};

/**
 * Handle `struct` for API configuration-iterator objects.
 */
struct tiledb_config_iter_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"configuration iterator"};

 private:
  using config_iter_type = tiledb::sm::ConfigIter;
  config_iter_type config_iter_;

 public:
  explicit tiledb_config_iter_handle_t(
      const tiledb::sm::Config& config, const std::string& prefix)
      : config_iter_{config, prefix} {
  }
  [[nodiscard]] inline config_iter_type& config_iter() {
    return config_iter_;
  }
  [[nodiscard]] inline const config_iter_type& config_iter() const {
    return config_iter_;
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param config A possibly-valid configuration handle
 */
inline void ensure_config_is_valid(const tiledb_config_handle_t* config) {
  ensure_handle_is_valid(config);
}

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param config A possibly-valid configuration handle
 */
inline void ensure_config_is_valid_if_present(
    const tiledb_config_handle_t* config) {
  if (config == nullptr) {
    return;
  }
  ensure_handle_is_valid(config);
}

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param config-iter A possibly-valid configuration-iterator handle
 */
inline void ensure_config_iter_is_valid(
    const tiledb_config_iter_handle_t* config_iter) {
  ensure_handle_is_valid(config_iter);
}

}  // namespace tiledb::api
#endif  // TILEDB_CAPI_CONFIG_INTERNAL_H
