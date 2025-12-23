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

#include <nlohmann/json_fwd.hpp>

#include "tiledb/common/exception/exception.h"
#include "tiledb/common/filesystem/home_directory.h"
#include "tiledb/sm/misc/constants.h"

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
  static const std::string DEFAULT_PROFILE_NAME;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param name The name of the RestProfile. If `std::nullopt`, the default
   * name is used.
   * @param dir The directory path on which the profile will be stored. If
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
   * Retrieves a pointer to the value of the given parameter.
   *
   * @param param The parameter to fetch.
   * @return The value of the parameter, or std::nullopt if the parameter is not
   * set
   */
  std::optional<std::string_view> get_param(const std::string& param) const;

  inline const std::map<std::string, std::string>& param_values() const {
    return param_values_;
  }

  /**
   * @return true if there is a regular file at the file path expected for this
   */
  bool file_exists() const;

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
  nlohmann::json to_json();

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
  std::map<std::string, std::string> param_values_;
};
}  // namespace tiledb::sm

#endif  // TILEDB_REST_PROFILE_H
