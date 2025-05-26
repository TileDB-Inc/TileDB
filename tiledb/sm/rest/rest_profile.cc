/**
 * @file   rest_profile.cc
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
 * This file implements class RestProfile.
 */

#include <iostream>

#include "rest_profile.h"
#include "tiledb/common/random/random_label.h"

using namespace tiledb::common;
using namespace tiledb::common::filesystem;

namespace tiledb::sm {

/* ****************************** */
/*           STATIC API           */
/* ****************************** */

/**
 * Read the given file and return its contents as a json object.
 *
 * @param filepath The path of the file to load.
 * @return The contents of the file, as a json object.
 */
static json read_file(const std::string& filepath) {
  json data;
  {
    std::ifstream file(filepath);
    try {
      file >> data;
    } catch (...) {
      throw RestProfileException("Error parsing file '" + filepath + "'.");
    }
  }
  return data;
}

/**
 * Write the given json to the given file.
 *
 * @param data The json data to write to the file.
 * @param filepath The path of the file to which the data is written.
 */
static void write_file(json data, const std::string& filepath) {
  // Temporarily append filepath with a random label to guarantee atomicity.
  std::string temp_filepath = filepath + "." + random_label();

  // Load the json object contents into the file.
  std::ofstream file(temp_filepath);
  try {
    file << std::setw(2) << data;
    file.flush();
    file.close();
  } catch (...) {
    throw RestProfileException(
        "Failed to write file due to an internal error during write.");
  }

  // Remove the random label from the filepath.
  try {
    std::filesystem::rename(temp_filepath.c_str(), filepath.c_str());
  } catch (std::filesystem::filesystem_error& e) {
    throw RestProfileException(
        "Failed to write file due to internal error: " + std::string(e.what()));
  }
}

/* ****************************** */
/*       PARAMETER DEFAULTS       */
/* ****************************** */

const std::string RestProfile::DEFAULT_NAME{"default"};
const std::string RestProfile::DEFAULT_PASSWORD{""};
const std::string RestProfile::DEFAULT_PAYER_NAMESPACE{""};
const std::string RestProfile::DEFAULT_TOKEN{""};
const std::string RestProfile::DEFAULT_SERVER_ADDRESS{"https://api.tiledb.com"};
const std::string RestProfile::DEFAULT_USERNAME{""};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

RestProfile::RestProfile(const std::string& name, const std::string& homedir)
    : version_(constants::rest_profile_version)
    , name_(name)
    , homedir_(homedir.empty() ? home_directory() : homedir)
    , filepath_(homedir_ + constants::rest_profile_filepath)
    , old_filepath_(homedir_ + constants::cloud_profile_filepath) {
  if (name_.empty()) {
    throw RestProfileException(
        "Failed to create RestProfile: name cannot be empty.");
  }
}

RestProfile::RestProfile(const std::string& name) {
  /**
   * Ensure the user's $HOME is found.
   * There's an edge case in which `sudo` does not always preserve the path to
   * `$HOME`. In this case, the home_directory() API does not throw, but instead
   * returns an empty string. As such, we can check for a value in the returned
   * path of the home_directory and throw an error to the user accordingly,
   * so they may decide the proper course of action: set the $HOME path,
   * or perhaps stop using `sudo`.
   */
  auto homedir = home_directory();
  if (homedir.empty()) {
    throw RestProfileException(
        "Failed to create RestProfile; $HOME is not set.");
  }

  RestProfile(name, homedir);
}

/* ****************************** */
/*              API               */
/* ****************************** */

RestProfile RestProfile::load_profile(
    const std::optional<std::string>& name,
    const std::optional<std::string>& homedir) {
  // Create a profile object
  RestProfile profile =
      !name.has_value()   ? RestProfile(RestProfile::DEFAULT_NAME) :
      homedir.has_value() ? RestProfile(name.value(), homedir.value()) :
                            RestProfile(name.value());

  // Load the profile
  try {
    profile.load_from_file(false);
    return profile;
  } catch (const RestProfileException& e) {
    throw RestProfileException(
        "Failed to load profile; " + std::string(e.what()));
  }
}

void RestProfile::set_param(
    const std::string& param, const std::string& value) {
  // Validate incoming parameter name
  auto it = param_values_.find(param);
  if (it == param_values_.end()) {
    throw RestProfileException(
        "Failed to set parameter of invalid name '" + param + "'");
  }
  param_values_[param] = value;
}

const std::string& RestProfile::get_param(const std::string& param) const {
  auto it = param_values_.find(param);
  if (it == param_values_.end()) {
    throw RestProfileException("Failed to retrieve parameter '" + param + "'");
  }
  return it->second;
}

/**
 * @note The `version_` will always be listed toward the end of the local file.
 * `nlohmann::json` does not preserve the structure of the original, top-level
 * json object, but rather sorts its elements alphabetically.
 * See issue [#727](https://github.com/nlohmann/json/issues/727) for details.
 */
void RestProfile::save_to_file(const bool overwrite) {
  // Validate that the profile is complete (if username is set, so is password)
  if ((param_values_["rest.username"] == RestProfile::DEFAULT_USERNAME) !=
      (param_values_["rest.password"] == RestProfile::DEFAULT_PASSWORD)) {
    throw RestProfileException(
        "Failed to save profile: 'rest.username' and 'rest.password' must "
        "either both be set or both remain unset. Mixing a default username "
        "with a custom password (or vice versa) is not allowed.");
  }
  // Fstream cannot create directories. If `homedir/.tiledb/` DNE, create it.
  std::filesystem::create_directories(homedir_ + ".tiledb");

  // If the file already exists, load it into a json object.
  json data;
  if (std::filesystem::exists(filepath_)) {
    // Read the file into the json object.
    data = read_file(filepath_);

    // If the file is outdated, throw an error. This behavior will evolve.
    if (data["version"] < version_) {
      throw RestProfileException(
          "The version of your local profile.json file is out of date.");
    }

    // Check that this profile hasn't already been saved.
    if (data.contains(name_)) {
      if (overwrite) {
        // If the user wants to overwrite, remove the old profile.
        data.erase(name_);
      } else {
        // If the user doesn't want to overwrite, throw an error.
        throw RestProfileException(
            "Failed to save '" + name_ +
            "'; This profile has already been saved "
            "and must be explicitly removed in order to be replaced.");
      }
    }
  } else {
    // Write the version number iff this is the first time opening the file.
    data.push_back(json::object_t::value_type("version", version_));
  }

  // Add this profile to the json object.
  data.push_back(json::object_t::value_type(name_, to_json()));

  // Write to the file, which will be created if it does not yet exist.
  write_file(data, filepath_);
}

void RestProfile::load_from_file(const bool check_old_filepath) {
  if (std::filesystem::exists(filepath_)) {
    // If the local file exists, load the profile with the given name.
    load_from_json_file(filepath_);
  } else if (check_old_filepath && std::filesystem::exists(old_filepath_)) {
    // If the old version of the file exists, load the profile from there
    load_from_json_file(old_filepath_);
  } else {
    // If the file does not exist, throw an error.
    throw RestProfileException("Failed to load profile; file does not exist.");
  }
}

void RestProfile::remove_from_file() {
  if (!std::filesystem::exists(filepath_)) {
    throw RestProfileException(
        "Failed to remove profile; file does not exist.");
  }

  // Read the file into a json object.
  json data = read_file(filepath_);

  // If a profile of the given name exists, remove it.
  auto it = data.find(name_);
  if (it == data.end()) {
    throw RestProfileException(
        "Failed to remove profile; profile does not exist.");
  }
  data.erase(it);

  // Write the json back to the file.
  try {
    write_file(data, filepath_);
  } catch (const std::exception& e) {
    throw RestProfileException(
        "Failed to remove profile; error writing file: " +
        std::string(e.what()));
  }
}

json RestProfile::to_json() {
  json j;
  for (const auto& param : param_values_) {
    j[param.first] = param.second;
  }
  return j;
}

std::string RestProfile::dump() {
  return json{{name_, to_json()}}.dump(2);
}

/* ****************************** */
/*           PRIVATE API          */
/* ****************************** */

void RestProfile::load_from_json_file(const std::string& filename) {
  if (filename.empty() ||
      (filename != filepath_ && filename != old_filepath_)) {
    throw RestProfileException(
        "Cannot load from '" + filename + "'; invalid filename.");
  }

  if (!std::filesystem::exists(filename)) {
    throw RestProfileException(
        "Cannot load from '" + filename + "'; file does not exist.");
  }

  // Load the file into a json object.
  json data = read_file(filename);

  if (filename.c_str() == old_filepath_.c_str()) {
    // Update any written-parameters from the loaded json object without
    // considering name.
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
    if (data.contains("verify_ssl")) {
      verify_ssl_ = data["verify_ssl"];
    }
  } else {
    // Consider the name of the profile to load.
    json profile = data[name_];
    if (!profile.is_null()) {
      for (auto it = profile.begin(); it != profile.end(); ++it) {
        param_values_[it.key()] = profile[it.key()];
      }
    } else {
      throw RestProfileException(
          "Failed to load profile; profile '" + name_ + "' does not exist.");
    }
  }
}

}  // namespace tiledb::sm
