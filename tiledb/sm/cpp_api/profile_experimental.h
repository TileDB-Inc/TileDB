/**
 * @file   profile_experimental.h
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
 * This file declares the experimental profile C++ API.
 */

#ifndef TILEDB_CPP_PROFILE_EXPERIMENTAL_H
#define TILEDB_CPP_PROFILE_EXPERIMENTAL_H

#include "capi_string.h"
#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "tiledb_experimental.h"

#include <memory>
#include <string>

namespace tiledb {

class ProfileException : public TileDBError {
 public:
  explicit ProfileException(const std::string& msg)
      : TileDBError(std::string("[TileDB::C++ API][Profile]: " + msg)) {
  }
};

class Profile {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Default Constructor.
   *
   * @note Use of `homedir` is intended primarily for testing purposes, to
   * preserve local files from in-test changes. Users may pass their own
   * `homedir`, but are encouraged to use `nullptr`, the default case.
   *
   * @param name The profile name if you want to override the default.
   * @param homedir The local $HOME directory path if you want to override the
   * default.
   */
  explicit Profile(
      std::optional<std::string> name = std::nullopt,
      std::optional<std::string> homedir = std::nullopt) {
    const char* n = name.has_value() ? name->c_str() : nullptr;
    const char* h = homedir.has_value() ? homedir->c_str() : nullptr;

    tiledb_profile_t* capi_profile;
    tiledb_error_t* capi_error = nullptr;

    int rc = tiledb_profile_alloc(n, h, &capi_profile, &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }

    profile_ = std::shared_ptr<tiledb_profile_t>(capi_profile, Profile::free);
  }

  Profile(const Profile&) = default;
  Profile(Profile&&) = default;
  Profile& operator=(const Profile&) = default;
  Profile& operator=(Profile&&) = default;

  /** Destructor. */
  ~Profile() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Factory function to load a profile from the local file.
   *
   * @param name The profile name if you want to override the default.
   * @param homedir The local $HOME directory path if you want to override the
   * @return A Profile object loaded from the file.
   */
  static Profile load(
      std::optional<std::string> name = std::nullopt,
      std::optional<std::string> homedir = std::nullopt) {
    // create a profile object
    Profile profile(name, homedir);

    // load the profile
    tiledb_error_t* capi_error = nullptr;
    int rc = tiledb_profile_load(profile.profile_.get(), &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }
    return profile;
  }

  /** Returns the C TileDB profile object. */
  std::shared_ptr<tiledb_profile_t> ptr() const {
    return profile_;
  }

  /** Retrieves the name of the profile. */
  std::string get_name() const {
    tiledb_error_t* capi_error = nullptr;
    tiledb_string_t* name;

    int rc = tiledb_profile_get_name(profile_.get(), &name, &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }

    // Convert string handle to a std::string
    const char* name_ptr;
    size_t name_len;
    tiledb_string_view(name, &name_ptr, &name_len);
    std::string ret(name_ptr, name_len);

    // Release the string handle
    tiledb_string_free(&name);

    return ret;
  }

  /** Retrieves the homedir of the profile. */
  std::string get_homedir() const {
    tiledb_error_t* capi_error = nullptr;
    tiledb_string_t* homedir;
    int rc = tiledb_profile_get_homedir(profile_.get(), &homedir, &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }

    // Convert string handle to a std::string
    const char* homedir_ptr;
    size_t homedir_length;
    tiledb_string_view(homedir, &homedir_ptr, &homedir_length);
    std::string ret(homedir_ptr, homedir_length);

    // Release the string handle
    tiledb_string_free(&homedir);

    return ret;
  }

  /** Sets a parameter in the profile. */
  void set_param(const std::string& param, const std::string& value) {
    tiledb_error_t* capi_error = nullptr;
    int rc = tiledb_profile_set_param(
        profile_.get(), param.c_str(), value.c_str(), &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }
  }

  /** Retrieves a parameter value from the profile. */
  std::string get_param(const std::string& param) const {
    tiledb_error_t* capi_error = nullptr;
    tiledb_string_t* value;
    int rc = tiledb_profile_get_param(
        profile_.get(), param.c_str(), &value, &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }

    // Convert string handle to a std::string
    const char* value_ptr;
    size_t value_len;
    tiledb_string_view(value, &value_ptr, &value_len);
    std::string ret(value_ptr, value_len);

    // Release the string handle
    tiledb_string_free(&value);

    return ret;
  }

  /** Saves the profile to the local file. */
  void save() {
    tiledb_error_t* capi_error = nullptr;
    int rc = tiledb_profile_save(profile_.get(), &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }
  }

  /** Removes the profile from the local file. */
  void remove() {
    tiledb_error_t* capi_error = nullptr;
    int rc = tiledb_profile_remove(profile_.get(), &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }
  }

  /** Dumps the profile in ASCII format. */
  std::string dump() const {
    tiledb_error_t* capi_error = nullptr;
    tiledb_string_t* str;
    int rc = tiledb_profile_dump_str(profile_.get(), &str, &capi_error);
    if (rc != TILEDB_OK) {
      const char* msg_cstr;
      tiledb_error_message(capi_error, &msg_cstr);
      std::string msg = msg_cstr;
      tiledb_error_free(&capi_error);
      throw ProfileException(msg);
    }

    return impl::convert_to_string(&str).value();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Pointer to the TileDB C profile object. */
  std::shared_ptr<tiledb_profile_t> profile_;

  /** Wrapper function for freeing a profile C object. */
  static void free(tiledb_profile_t* profile) {
    tiledb_profile_free(&profile);
  }
};

}  // namespace tiledb

#endif  // TILEDB_CPP_PROFILE_EXPERIMENTAL_H
