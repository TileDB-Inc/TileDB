/**
 * @file tiledb/api/c_api/config/config_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
 * This file defines C API functions for the config section.
 */

#include "../../c_api_support/c_api_support.h"
#include "../error/error_api_internal.h"
#include "config_api_external.h"
#include "config_api_internal.h"

namespace tiledb::api {

void ensure_param_argument_is_valid(const char* param) {
  if (param == nullptr) {
    throw CAPIStatusException("argument `param` may not be nullptr");
  }
}

capi_return_t tiledb_config_alloc(tiledb_config_handle_t** config) {
  ensure_output_pointer_is_valid(config);
  *config = tiledb_config_handle_t::make_handle(tiledb::sm::Config());
  return TILEDB_OK;
}

void tiledb_config_free(tiledb_config_handle_t** config) {
  ensure_output_pointer_is_valid(config);
  ensure_config_is_valid(*config);
  tiledb_config_handle_t::break_handle(*config);
}

capi_return_t tiledb_config_set(
    tiledb_config_handle_t* config, const char* param, const char* value) {
  ensure_config_is_valid(config);
  // Validate param & value arguments before they're converted to std::string
  ensure_param_argument_is_valid(param);
  if (value == nullptr) {
    throw CAPIStatusException("argument `value` may not be nullptr");
  }
  throw_if_not_ok(config->config().set(param, value));
  return TILEDB_OK;
}

capi_return_t tiledb_config_get(
    tiledb_config_handle_t* config, const char* param, const char** value) {
  ensure_config_is_valid(config);
  ensure_param_argument_is_valid(param);
  ensure_output_pointer_is_valid(value);
  throw_if_not_ok(config->config().get(param, value));
  return TILEDB_OK;
}

capi_return_t tiledb_config_unset(tiledb_config_t* config, const char* param) {
  ensure_config_is_valid(config);
  ensure_param_argument_is_valid(param);
  throw_if_not_ok(config->config().unset(param));
  return TILEDB_OK;
}

capi_return_t tiledb_config_load_from_file(
    tiledb_config_handle_t* config, const char* filename) {
  ensure_config_is_valid(config);
  if (filename == nullptr) {
    throw CAPIStatusException("Cannot load from file; null filename");
  }
  throw_if_not_ok(config->config().load_from_file(filename));
  return TILEDB_OK;
}

capi_return_t tiledb_config_save_to_file(
    tiledb_config_t* config, const char* filename) {
  ensure_config_is_valid(config);
  if (filename == nullptr) {
    throw CAPIStatusException("Cannot save to file; null filename");
  }
  throw_if_not_ok(config->config().save_to_file(filename));
  return TILEDB_OK;
}

capi_return_t tiledb_config_compare(
    tiledb_config_t* lhs, tiledb_config_t* rhs, uint8_t* equal) {
  ensure_config_is_valid(lhs);
  ensure_config_is_valid(rhs);
  ensure_output_pointer_is_valid(equal);
  *equal = (lhs->config() == rhs->config()) ? 1 : 0;
  return TILEDB_OK;
}

capi_return_t tiledb_config_iter_alloc(
    tiledb_config_t* config,
    const char* prefix,
    tiledb_config_iter_t** config_iter) {
  ensure_config_is_valid(config);
  ensure_output_pointer_is_valid(config_iter);
  std::string prefix_str = (prefix == nullptr) ? "" : std::string(prefix);
  *config_iter =
      tiledb_config_iter_handle_t::make_handle(config->config(), prefix_str);
  return TILEDB_OK;
}

capi_return_t tiledb_config_iter_reset(
    tiledb_config_t* config,
    tiledb_config_iter_t* config_iter,
    const char* prefix) {
  ensure_config_is_valid(config);
  ensure_config_iter_is_valid(config_iter);
  std::string prefix_str = (prefix == nullptr) ? "" : std::string(prefix);
  config_iter->config_iter().reset(config->config(), prefix_str);
  return TILEDB_OK;
}

void tiledb_config_iter_free(tiledb_config_iter_t** config_iter) {
  ensure_output_pointer_is_valid(config_iter);
  ensure_config_iter_is_valid(*config_iter);
  tiledb_config_iter_handle_t::break_handle(*config_iter);
}

capi_return_t tiledb_config_iter_here(
    tiledb_config_iter_t* config_iter, const char** param, const char** value) {
  ensure_config_iter_is_valid(config_iter);
  ensure_output_pointer_is_valid(param);
  ensure_output_pointer_is_valid(value);
  if (config_iter->config_iter().end()) {
    *param = nullptr;
    *value = nullptr;
  } else {
    *param = config_iter->config_iter().param().c_str();
    *value = config_iter->config_iter().value().c_str();
  }
  return TILEDB_OK;
}

capi_return_t tiledb_config_iter_next(tiledb_config_iter_t* config_iter) {
  ensure_config_iter_is_valid(config_iter);
  config_iter->config_iter().next();
  return TILEDB_OK;
}

capi_return_t tiledb_config_iter_done(
    tiledb_config_iter_t* config_iter, int32_t* done) {
  ensure_config_iter_is_valid(config_iter);
  ensure_output_pointer_is_valid(done);
  *done = (int32_t)config_iter->config_iter().end();
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_error;

CAPI_INTERFACE(config_alloc, tiledb_config_t** config, tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_alloc>(error, config);
}

/*
 * API Audit: Void return means no possible signal for an error. No channel that
 * can return an error.
 * Possible errors: `config` may be null or an invalid handle.
 */
CAPI_INTERFACE_VOID(config_free, tiledb_config_t** config) {
  tiledb::api::api_entry_void<tiledb::api::tiledb_config_free>(config);
}

CAPI_INTERFACE(
    config_set,
    tiledb_config_t* config,
    const char* param,
    const char* value,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_set>(
      error, config, param, value);
}

CAPI_INTERFACE(
    config_get,
    tiledb_config_t* config,
    const char* param,
    const char** value,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_get>(
      error, config, param, value);
}

CAPI_INTERFACE(
    config_unset,
    tiledb_config_t* config,
    const char* param,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_unset>(
      error, config, param);
}

CAPI_INTERFACE(
    config_load_from_file,
    tiledb_config_t* config,
    const char* filename,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_load_from_file>(
      error, config, filename);
}

CAPI_INTERFACE(
    config_save_to_file,
    tiledb_config_t* config,
    const char* filename,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_save_to_file>(
      error, config, filename);
}

/*
 * API Audit: No channel that can return an error.
 * Possible errors: Both `lhs` and `rhs` may be null or an invalid handle.
 * `equal` may be a null pointer
 */
CAPI_INTERFACE(
    config_compare,
    tiledb_config_t* lhs,
    tiledb_config_t* rhs,
    uint8_t* equal) {
  return tiledb::api::api_entry_plain<tiledb::api::tiledb_config_compare>(
      lhs, rhs, equal);
}

CAPI_INTERFACE(
    config_iter_alloc,
    tiledb_config_t* config,
    const char* prefix,
    tiledb_config_iter_t** config_iter,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_iter_alloc>(
      error, config, prefix, config_iter);
}

CAPI_INTERFACE(
    config_iter_reset,
    tiledb_config_t* config,
    tiledb_config_iter_t* config_iter,
    const char* prefix,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_iter_reset>(
      error, config, config_iter, prefix);
}

/*
 * API Audit: Void return means no possible signal for an error. No channel that
 * can return an error.
 * Possible errors: `config` may be null or an invalid handle.
 */
CAPI_INTERFACE_VOID(config_iter_free, tiledb_config_iter_t** config_iter) {
  tiledb::api::api_entry_void<tiledb::api::tiledb_config_iter_free>(
      config_iter);
}

CAPI_INTERFACE(
    config_iter_here,
    tiledb_config_iter_t* config_iter,
    const char** param,
    const char** value,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_iter_here>(
      error, config_iter, param, value);
}

CAPI_INTERFACE(
    config_iter_next,
    tiledb_config_iter_t* config_iter,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_iter_next>(
      error, config_iter);
}

CAPI_INTERFACE(
    config_iter_done,
    tiledb_config_iter_t* config_iter,
    int32_t* done,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_config_iter_done>(
      error, config_iter, done);
}
