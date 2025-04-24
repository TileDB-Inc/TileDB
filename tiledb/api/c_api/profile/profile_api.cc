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
    const char* name, const char* homedir, tiledb_profile_t** profile) {
  ensure_output_pointer_is_valid(profile);
  std::string name_str;
  if (!name) {
    // Passing nullptr resolves to the default case.
    name_str = tiledb::sm::RestProfile::DEFAULT_NAME;
  } else {
    name_str = name;
    if (name_str.empty()) {
      throw CAPIException("[tiledb_profile_alloc] Name cannot be empty.");
    }
  }

  std::string homedir_str;
  if (!homedir) {
    // Passing nullptr resolves to the default case.
    homedir_str = tiledb::common::filesystem::home_directory();
  } else {
    homedir_str = homedir;
    if (homedir_str.empty()) {
      throw CAPIException("[tiledb_profile_alloc] Homedir cannot be empty.");
    }
  }

  *profile = tiledb_profile_t::make_handle(name_str, homedir_str);

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

capi_return_t tiledb_profile_get_homedir(
    tiledb_profile_t* profile, tiledb_string_handle_t** homedir) {
  ensure_profile_is_valid(profile);
  ensure_output_pointer_is_valid(homedir);
  *homedir = tiledb_string_handle_t::make_handle(profile->profile()->homedir());
  return TILEDB_OK;
}

capi_return_t tiledb_profile_set_param(
    tiledb_profile_t* profile, const char* param, const char* value) {
  ensure_profile_is_valid(profile);
  if (!param || !value) {
    throw CAPIException(
        "[tiledb_profile_set_param] Parameter or value cannot be null.");
  }
  profile->profile()->set_param(param, value);
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
  *value =
      tiledb_string_handle_t::make_handle(profile->profile()->get_param(param));
  return TILEDB_OK;
}

capi_return_t tiledb_profile_save(tiledb_profile_t* profile) {
  ensure_profile_is_valid(profile);
  profile->profile()->save_to_file();
  return TILEDB_OK;
}

capi_return_t tiledb_profile_remove(tiledb_profile_t* profile) {
  ensure_profile_is_valid(profile);
  profile->profile()->remove_from_file();
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_error;
using tiledb::api::api_entry_void;

CAPI_INTERFACE(
    profile_alloc,
    const char* name,
    const char* homedir,
    tiledb_profile_t** profile,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_alloc>(
      error, name, homedir, profile);
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
    profile_get_homedir,
    tiledb_profile_t* profile,
    tiledb_string_handle_t** homedir,
    tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_get_homedir>(
      error, profile, homedir);
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
    profile_remove, tiledb_profile_t* profile, tiledb_error_t** error) {
  return api_entry_error<tiledb::api::tiledb_profile_remove>(error, profile);
}
