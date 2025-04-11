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

#include "context.h"
#include "deleter.h"
#include "exception.h"
#include "tiledb_experimental.h"

#include "tiledb/api/c_api/profile/profile_api_internal.h"

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

  /** Default constructor. */
  Profile()
      : Profile(nullptr, nullptr) {
  }

  /**
   * Constructor.
   *
   * @note Use of `homedir` is intended primarily for testing purposes, to
   * preserve local files from in-test changes. Users may pass their own
   * `homedir`, but are encouraged to use `nullptr`, the default case.
   *
   * @param name The profile name, or `nullptr` for default.
   * @param homedir The local $HOME directory path, or `nullptr` for default.
   */
  explicit Profile(const std::string& name, const std::string& homedir) {
    const char *n = nullptr, *h = nullptr;
    if (name[0] != '\0') {
      n = name.c_str();
    }
    if (homedir[0] != '\0') {
      h = name.c_str();
    }

    tiledb_profile_t* capi_profile;
    try {
      tiledb_profile_alloc(n, h, &capi_profile);
    } catch (std::exception& e) {
      throw ProfileException(
          "Failed to create Profile due to an internal error: " +
          std::string(e.what()));
    }

    profile_ = std::shared_ptr<tiledb_profile_t>(capi_profile, Profile::free);
  }

  /** Copy and move constructors are deleted. Profile objects are immutable. */
  Profile(const Profile&) = delete;
  Profile(Profile&&) = delete;
  Profile& operator=(const Profile&) = delete;
  Profile& operator=(Profile&&) = delete;

  /** Destructor. */
  ~Profile() = default;

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /** Wrapper function for freeing a profile C object. */
  static void free(tiledb_profile_t* profile) {
    tiledb_profile_free(&profile);
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the C TileDB profile object. */
  std::shared_ptr<tiledb_profile_t> ptr() const {
    return profile_;
  }

  /** Retrieves the name of the profile. */
  std::string get_name() {
    return profile_->profile()->name();
  }

  /** Retrieves the homedir of the profile. */
  std::string get_homedir() {
    return profile_->profile()->homedir();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB C profile object. */
  std::shared_ptr<tiledb_profile_t> profile_;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_PROFILE_EXPERIMENTAL_H
