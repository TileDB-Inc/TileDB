/**
 * @file tiledb/api/c_api/profile/profile_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file defines C API functions for the profile section.
 */

#include "../error/error_api_internal.h"
#include "../string/string_api_internal.h"
#include "profile_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

capi_return_t tiledb_profile_alloc(
    const char* name, const char* dir, tiledb_profile_t** profile) {
  ensure_output_pointer_is_valid(profile);

  std::optional<std::string> name_str =
      name ? std::make_optional(name) : std::nullopt;
  std::optional<std::string> dir_str =
      dir ? std::make_optional(dir) : std::nullopt;

  *profile = tiledb_profile_t::make_handle(name_str, dir_str);

  return TILEDB_OK;
}

void tiledb_profile_free(tiledb_profile_t** profile) {
  ensure_output_pointer_is_valid(profile);
  ensure_profile_is_valid(*profile);
  tiledb_profile_t::break_handle(*profile);
}

capi_return_t tiledb_profile_get_name(
    tiledb_profile_t* profile, tiledb_string_handle_t** name) {
  ensure_profile_is_valid(profile);
  ensure_output_pointer_is_valid(name);
  *name = tiledb_string_handle_t::make_handle(profile->profile()->name());
  return TILEDB_OK;
}

capi_return_t tiledb_profile_get_dir(
    tiledb_profile_t* profile, tiledb_string_handle_t** dir) {
  ensure_profile_is_valid(profile);
  ensure_output_pointer_is_valid(dir);
  *dir = tiledb_string_handle_t::make_handle(profile->profile()->dir());
  return TILEDB_OK;
}

capi_return_t tiledb_profile_set_param(
    tiledb_profile_t* profile, const char* param, const char* value) {
  ensure_profile_is_valid(profile);
  if (!param) {
    throw CAPIException("[tiledb_profile_set_param] Parameter cannot be null.");
  }
  profile->profile()->set_param(param, value ? value : "");
  return TILEDB_OK;
}

capi_return_t tiledb_profile_get_param(
    tiledb_profile_t* profile,
    const char* param,
    tiledb_string_handle_t** value) {
  ensure_profile_is_valid(profile);
  ensure_output_pointer_is_valid(value);
  if (!param) {
    throw CAPIException("[tiledb_profile_get_param] Parameter cannot be null.");
  }

  const std::string* param_value = profile->profile()->get_param(param);

  *value = param_value != nullptr ?
               tiledb_string_handle_t::make_handle(*param_value) :
               nullptr;

  return TILEDB_OK;
}

capi_return_t tiledb_profile_save(tiledb_profile_t* profile) {
  ensure_profile_is_valid(profile);
  profile->profile()->save_to_file();
  return TILEDB_OK;
}

capi_return_t tiledb_profile_load(tiledb_profile_t* profile) {
  ensure_profile_is_valid(profile);
  profile->profile()->load_from_file();
  return TILEDB_OK;
}

capi_return_t tiledb_profile_remove(const char* name, const char* dir) {
  std::optional<std::string> name_opt =
      (name == nullptr) ? std::nullopt : std::make_optional(std::string(name));
  std::optional<std::string> dir_opt =
      (dir == nullptr) ? std::nullopt : std::make_optional(std::string(dir));
  tiledb::sm::RestProfile::remove_profile(name_opt, dir_opt);
  return TILEDB_OK;
}

capi_return_t tiledb_profile_dump_str(
    tiledb_profile_t* profile, tiledb_string_handle_t** out) {
  ensure_profile_is_valid(profile);
  ensure_output_pointer_is_valid(out);
  *out = tiledb_string_handle_t::make_handle(profile->profile()->dump());
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_error;
using tiledb::api::api_entry_void;

CAPI_INTERFACE(
    profile_alloc,
    const char* name,
    const char* dir,
    tiledb_profile_t** profile,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_alloc>(
      error, name, dir, profile);
}

CAPI_INTERFACE_VOID(profile_free, tiledb_profile_t** profile) {
  return api_entry_void<tiledb::api::tiledb_profile_free>(profile);
}

CAPI_INTERFACE(
    profile_get_name,
    tiledb_profile_t* profile,
    tiledb_string_handle_t** name,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_get_name>(
      error, profile, name);
}

CAPI_INTERFACE(
    profile_get_dir,
    tiledb_profile_t* profile,
    tiledb_string_handle_t** dir,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_get_dir>(
      error, profile, dir);
}

CAPI_INTERFACE(
    profile_set_param,
    tiledb_profile_t* profile,
    const char* param,
    const char* value,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_set_param>(
      error, profile, param, value);
}

CAPI_INTERFACE(
    profile_get_param,
    tiledb_profile_t* profile,
    const char* param,
    tiledb_string_handle_t** value,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_get_param>(
      error, profile, param, value);
}

CAPI_INTERFACE(
    profile_save, tiledb_profile_t* profile, tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_save>(error, profile);
}

CAPI_INTERFACE(
    profile_load, tiledb_profile_t* profile, tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_load>(error, profile);
}

CAPI_INTERFACE(
    profile_remove, const char* name, const char* dir, tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_remove>(error, name, dir);
}

CAPI_INTERFACE(
    profile_dump_str,
    tiledb_profile_t* profile,
    tiledb_string_handle_t** out,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_dump_str>(
      error, profile, out);
}
