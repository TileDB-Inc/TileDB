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

#include "profile_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

inline char* copy_string(const std::string& str) {
  auto ret = static_cast<char*>(std::malloc(str.size() + 1));
  if (ret == nullptr) {
    return nullptr;
  }

  std::memcpy(ret, str.data(), str.size());
  ret[str.size()] = '\0';

  return ret;
}

capi_return_t tiledb_profile_alloc(
    const char* name, const char* homedir, tiledb_profile_t** profile) {
  ensure_output_pointer_is_valid(profile);
  if (!name) {
    // Passing nullptr resolves to the default case.
    name = tiledb::sm::RestProfile::DEFAULT_NAME.c_str();
  }
  if (name[0] == '\0') {
    throw CAPIException("[tiledb_profile_alloc] Name cannot be empty.");
  }

  if (homedir) {
    if (homedir[0] == '\0') {
      throw CAPIException(
          "[tiledb_profile_alloc] $HOME directory cannot be empty.");
    }
    *profile =
        tiledb_profile_t::make_handle(std::string(name), std::string(homedir));
  } else {
    *profile = tiledb_profile_t::make_handle(std::string(name));
  }

  return TILEDB_OK;
}

capi_return_t tiledb_profile_free(tiledb_profile_t** profile) {
  ensure_output_pointer_is_valid(profile);
  ensure_profile_is_valid(*profile);
  tiledb_profile_t::break_handle(*profile);
  return TILEDB_OK;
}

capi_return_t tiledb_profile_get_name(
    tiledb_profile_t* profile, const char** name) {
  ensure_profile_is_valid(profile);
  ensure_output_pointer_is_valid(name);
  *name = profile->profile()->name().c_str();
  return TILEDB_OK;
}

capi_return_t tiledb_profile_get_homedir(
    tiledb_profile_t* profile, const char** homedir) {
  ensure_profile_is_valid(profile);
  ensure_output_pointer_is_valid(homedir);
  *homedir = profile->profile()->homedir().c_str();
  return TILEDB_OK;
}

capi_return_t tiledb_profile_set_param(
    tiledb_profile_t* profile, const char* param, const char* value) {
  ensure_profile_is_valid(profile);
  ensure_param_argument_is_valid(param);
  if (value == nullptr) {
    throw CAPIStatusException("argument `value` may not be nullptr");
  }
  profile->profile()->set_param(param, value);
  return TILEDB_OK;
}

capi_return_t tiledb_profile_get_param(
    tiledb_profile_t* profile, const char* param, const char** value) {
  ensure_profile_is_valid(profile);
  ensure_param_argument_is_valid(param);
  ensure_output_pointer_is_valid(value);

  const std::string str = profile->profile()->get_param(param);
  *value = copy_string(str);
  if (*value == nullptr) {
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;

CAPI_INTERFACE(
    profile_alloc,
    const char* name,
    const char* homedir,
    tiledb_profile_t** profile) {
  return api_entry_plain<tiledb::api::tiledb_profile_alloc>(
      name, homedir, profile);
}

CAPI_INTERFACE(profile_free, tiledb_profile_t** profile) {
  return api_entry_plain<tiledb::api::tiledb_profile_free>(profile);
}

CAPI_INTERFACE(profile_get_name, tiledb_profile_t* profile, const char** name) {
  return api_entry_plain<tiledb::api::tiledb_profile_get_name>(profile, name);
}

CAPI_INTERFACE(
    profile_get_homedir, tiledb_profile_t* profile, const char** homedir) {
  return api_entry_plain<tiledb::api::tiledb_profile_get_homedir>(
      profile, homedir);
}

CAPI_INTERFACE(
    profile_set_param,
    tiledb_profile_t* profile,
    const char* param,
    const char* value) {
  return api_entry_plain<tiledb::api::tiledb_profile_set_param>(
      profile, param, value);
}

CAPI_INTERFACE(
    profile_get_param,
    tiledb_profile_t* profile,
    const char* param,
    const char** value) {
  return api_entry_plain<tiledb::api::tiledb_profile_get_param>(
      profile, param, value);
}
