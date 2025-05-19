/**
 * @file   rest_profile.h
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
 * This file defines class RestProfile.
 */

#ifndef TILEDB_REST_PROFILE_H
#define TILEDB_REST_PROFILE_H

#include <fstream>
#include <map>
#include <set>
#include <string>

#include "external/include/nlohmann/json.hpp"
#include "tiledb/common/exception/exception.h"
#include "tiledb/common/filesystem/home_directory.h"
#include "tiledb/sm/misc/constants.h"

using json = nlohmann::json;
using namespace tiledb::common;

namespace tiledb::sm {

class RestProfileException : public StatusException {
 public:
  explicit RestProfileException(const std::string& message)
      : StatusException("RestProfile", message) {
  }
};

class RestProfile {
  /** Make the internals of class RestProfile available to class Config. */
  friend class Config;

 public:
  /* ****************************** */
  /*       PARAMETER DEFAULTS       */
  /* ****************************** */

  /** The default name of a RestProfile. */
  static const std::string DEFAULT_NAME;

  /** The user's REST password. */
  static const std::string DEFAULT_PASSWORD;

  /** The namespace that should be charged for the request. */
  static const std::string DEFAULT_PAYER_NAMESPACE;

  /** The user's REST token. */
  static const std::string DEFAULT_TOKEN;

  /** The default address for REST server. */
  static const std::string DEFAULT_SERVER_ADDRESS;

  /** The user's REST username. */
  static const std::string DEFAULT_USERNAME;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param name The name of the RestProfile. Defaulted to "default".
   */
  RestProfile(const std::string& name = RestProfile::DEFAULT_NAME);

  /**
   * Constructor. Intended for testing purposes only, to preserve the user's
   * $HOME path and their profiles from in-test changes.
   *
   * @param name The name of the RestProfile.
   * @param homedir The user's $HOME directory, or desired in-test path.
   */
  RestProfile(const std::string& name, const std::string& homedir);

  /** Destructor. */
  ~RestProfile() = default;

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /**
   * Factory function to load a profile from the local file.
   *
   * @note This API will _not_ parse the `cloud.json` path. This method is
   * expected to be used _primarily_ by a `Config` object inheriting
   * written-parameters off of a `RestProfile`.
   *
   * @param name The name of the profile to load.
   * @param homedir The user's $HOME directory, or desired in-test path.
   * @return The RestProfile.
   */
  static RestProfile load_profile(
      const std::optional<std::string>& name = std::nullopt,
      const std::optional<std::string>& homedir = std::nullopt);

  /**
   * Returns the name of this profile.
   *
   * @return std::string The name of this profile.
   */
  inline const std::string& name() const {
    return name_;
  }

  inline const std::map<std::string, std::string>& param_values() const {
    return param_values_;
  }

  inline std::optional<bool> get_verify_ssl() const {
    return verify_ssl_;
  }

  /**
   * Returns the path to the local $HOME directory.
   *
   * @return std::string The path to the local $HOME directory.
   */
  inline const std::string& homedir() const {
    return homedir_;
  }

  /**
   * Sets the given parameter to the given value.
   *
   * @param param The parameter to set.
   * @param value The value to set on the given parameter.
   */
  void set_param(const std::string& param, const std::string& value);

  /**
   * Retrieves the value of the given parameter.
   *
   * @param param The parameter to fetch.
   * @return The value of the given parameter.
   */
  const std::string& get_param(const std::string& param) const;

  /**
   * Saves this profile to the local file.
   *
   * @param overwrite If true, overwrite the existing profile with the same
   * name.
   */
  void save_to_file(const bool overwrite = false);

  /**
   * Loads this profile from the local file.
   *
   * @param check_old_filepath If true, check the old filepath for the
   * profile.
   */
  void load_from_file(const bool check_old_filepath = true);

  /** Removes this profile from the local file. */
  void remove_from_file();

  /**
   * Exports this profile's parameters and their values to a json object.
   *
   * @return A json object of this RestProfile's parameter : value mapping.
   */
  json to_json();

  /**
   * Dumps the parameter : value mapping in json object format.
   *
   * @return This RestProfile's parameter : value mapping in json object format.
   */
  std::string dump();

 private:
  /* ********************************* */
  /*            PRIVATE API            */
  /* ********************************* */

  /**
   * Loads the profile parameters from the given json file, if present.
   *
   * @param filepath The path of the file to load.
   */
  void load_from_json_file(const std::string& filepath);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The version of this class. */
  format_version_t version_;

  /** The name of this RestProfile. */
  std::string name_;

  /** The path to the local $HOME directory. */
  std::string homedir_;

  /** The path to the local file which stores all profiles. */
  std::string filepath_;

  /** The path to the old local file which previously stored a profile. */
  std::string old_filepath_;

  /** Stores a map of <param, value> for the set-parameters. */
  std::map<std::string, std::string> param_values_ = {
      std::make_pair("rest.password", RestProfile::DEFAULT_PASSWORD),
      std::make_pair(
          "rest.payer_namespace", RestProfile::DEFAULT_PAYER_NAMESPACE),
      std::make_pair("rest.token", RestProfile::DEFAULT_TOKEN),
      std::make_pair(
          "rest.server_address", RestProfile::DEFAULT_SERVER_ADDRESS),
      std::make_pair("rest.username", RestProfile::DEFAULT_USERNAME)};

  /**
   * Flag which tracks the `Config::verify_ssl` parameter inherited from
   * `cloud.json`.
   *
   * @note This is a temporary workaround, to be removed once the cloud API
   * is updated to no longer use `cloud.json`. The config parameter will
   * still be stored in `Config`, inheriting this value as a third fallback.
   */
  std::optional<bool> verify_ssl_{std::nullopt};
};

}  // namespace tiledb::sm

#endif  // TILEDB_REST_PROFILE_H
