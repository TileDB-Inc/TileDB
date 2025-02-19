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
  RestProfile(std::string name = RestProfile::DEFAULT_NAME);

  /** Destructor. */
  ~RestProfile() = default;

  /* ****************************** */
  /*              API               */
  /* ****************************** */

  inline std::string name() {
    return name_;
  }

  /**
   * Sets the given parameter to the given value.
   *
   * @param param The parameter to set.
   * @param value The value to set on the given parameter.
   */
  void set(const std::string& param, const std::string& value);

  /**
   * Retrieves the value of the given parameter.
   *
   * @param param The parameter to fetch.
   * @return The value of the given parameter.
   */
  std::string get(const std::string& param) const;

  /** Saves this profile to the local file. */
  void save();

  /** Removes this profile from the local file. */
  void remove();

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

  /** Loads the profile parameters from the local json file, if present. */
  inline void load_from_json_file(const std::string& filename) {
    if (filename.empty() ||
        (strcmp(filename.c_str(), filepath_.c_str()) != 0 &&
         strcmp(filename.c_str(), old_filepath_.c_str()) != 0)) {
      throw RestProfileException("Cannot load from file; invalid filename.");
    }

    if (!std::filesystem::exists(filename)) {
      throw RestProfileException("Cannot load from file; file does not exist.");
    }

    // Load the file into a json object.
    std::ifstream file(filename);
    json data;
    try {
      file >> data;
    } catch (...) {
      throw RestProfileException("Error parsing json file.");
    }

    // If possible, load (overwrite) the parameters from the local file
    if (strcmp(filename.c_str(), old_filepath_.c_str()) == 0) {
      // Parse the old file and load the parameters
      if (data.contains("api_key") &&
          data["api_key"].contains("X-TILEDB-REST-API-KEY")) {
        param_values_["rest.token"] = data["api_key"]["X-TILEDB-REST-API-KEY"];
      }
      if (data.contains("host")) {
        param_values_["rest.server_address"] = data["host"];
      }
      if (data.contains("password")) {
        param_values_["rest.password"] = data["password"];
      }
      if (data.contains("username")) {
        param_values_["rest.username"] = data["username"];
      }
    } else {
      json profile = data[name_];
      if (!profile.is_null()) {
        for (auto it = profile.begin(); it != profile.end(); ++it) {
          param_values_[it.key()] = profile[it.key()];
        }
      }
    }
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The version of this class. */
  int version_;

  /** The name of this RestProfile. */
  std::string name_;

  /** The path to the local $HOME directory. */
  std::string homedir_;

  /** The path to the local file which stores all profiles. */
  std::string filepath_;

  /** The path to the old local file which previously stored a profile. */
  std::string old_filepath_;

  /** Stores a map of <param, value> for the set-parameters. */
  std::map<std::string, std::string> param_values_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_REST_PROFILE_H
