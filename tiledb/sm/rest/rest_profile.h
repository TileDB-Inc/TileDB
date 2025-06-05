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

  /** The default REST password of a RestProfile. */
  static const std::string DEFAULT_PASSWORD;

  /** The default namespace that should be charged for the request. */
  static const std::string DEFAULT_PAYER_NAMESPACE;

  /** The default name of a RestProfile. */
  static const std::string DEFAULT_PROFILE_NAME;

  /** The default REST token of a RestProfile. */
  static const std::string DEFAULT_TOKEN;

  /** The default REST server address of a RestProfile. */
  static const std::string DEFAULT_SERVER_ADDRESS;

  /** The default REST username of a RestProfile. */
  static const std::string DEFAULT_USERNAME;

  /** A vector of the REST parameters that can be set. */
  static const std::vector<std::string> REST_PARAMETERS;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param name The name of the RestProfile. If `std::nullopt`, the default
   * name is used.
   * @param dir The directory path that the profile will be stored. If
   * `std::nullopt`, the home directory is used.
   */
  RestProfile(
      const std::optional<std::string>& name = std::nullopt,
      const std::optional<std::string>& dir = std::nullopt);

  /** Destructor. */
  ~RestProfile() = default;

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  /**
   * Returns the name of the profile.
   *
   * @return std::string The name of the profile.
   */
  inline const std::string& name() const {
    return name_;
  }

  /**
   * Returns the directory path that stores the profiles file.
   *
   * @return std::string The directory path that stores the profiles file.
   */
  inline const std::string& dir() const {
    return dir_;
  }

  /**
   * Sets the given parameter to the given value.
   *
   * @param param The parameter to set.
   * @param value The value to set on the given parameter.
   */
  void set_param(const std::string& param, const std::string& value);

  /**
   * Returns true if the given parameter can be handled by RestProfile.
   */
  static bool can_have_parameter(std::string_view param);

  /**
   * Retrieves a pointer to the value of the given parameter.
   *
   * @param param The parameter to fetch.
   * @return A pointer to the value of the parameter, or nullptr if the
   * parameter is not set.
   */
  const std::string* get_param(const std::string& param) const;

  inline const std::map<std::string, std::string>& param_values() const {
    return param_values_;
  }

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
   */
  void load_from_file();

  /**
   * Removes the profile with the given name from profiles file of the given
   * directory.
   *
   * @param name The name of the profile to remove. If `std::nullopt`, the
   * default name is used.
   * @param dir The directory path that contains the profiles file. If
   * `std::nullopt`, the home directory is used.
   */
  static void remove_profile(
      const std::optional<std::string>& name = std::nullopt,
      const std::optional<std::string>& dir = std::nullopt);

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

  /**
   * Helper function to remove a profile from the profiles file.
   *
   * @param name The name of the profile to remove.
   * @param filepath The directory path that contains the profiles file.
   */
  static void remove_profile_from_file(
      const std::string& name, const std::string& filepath);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The version of this class. */
  format_version_t version_;

  /** The name of this RestProfile. */
  std::string name_;

  /** The directory path that stores the profiles file. */
  std::string dir_;

  /** The path to the file that stores the profiles. */
  std::string filepath_;

  /** Stores a map of <param, value> for the set-parameters. */
  std::map<std::string, std::string> param_values_ = {
      std::make_pair("rest.password", RestProfile::DEFAULT_PASSWORD),
      std::make_pair(
          "rest.payer_namespace", RestProfile::DEFAULT_PAYER_NAMESPACE),
      std::make_pair("rest.token", RestProfile::DEFAULT_TOKEN),
      std::make_pair(
          "rest.server_address", RestProfile::DEFAULT_SERVER_ADDRESS),
      std::make_pair("rest.username", RestProfile::DEFAULT_USERNAME)};
};
}  // namespace tiledb::sm

#endif  // TILEDB_REST_PROFILE_H
