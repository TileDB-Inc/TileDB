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
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;
using namespace tiledb::common::filesystem;

namespace tiledb::sm {

/* ****************************** */
/*       PARAMETER DEFAULTS       */
/* ****************************** */

const std::string RestProfile::DEFAULT_NAME{"default"};
const std::string RestProfile::DEFAULT_PASSWORD{""};
const std::string RestProfile::DEFAULT_PAYER_NAMESPACE{""};
const std::string RestProfile::DEFAULT_TOKEN{""};
const std::string RestProfile::DEFAULT_SERVER_ADDRESS{"https://api.tiledb.com"};
const std::string RestProfile::DEFAULT_USERNAME{""};

const std::map<std::string, std::string> default_values = {
    std::make_pair("rest.password", RestProfile::DEFAULT_PASSWORD),
    std::make_pair(
        "rest.payer_namespace", RestProfile::DEFAULT_PAYER_NAMESPACE),
    std::make_pair("rest.token", RestProfile::DEFAULT_TOKEN),
    std::make_pair("rest.server_address", RestProfile::DEFAULT_SERVER_ADDRESS),
    std::make_pair("rest.username", RestProfile::DEFAULT_USERNAME)};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

RestProfile::RestProfile(std::string name)
    : version_(1)
    , name_(name)
    , homedir_(home_directory().has_value() ? home_directory().value() : "")
    , filepath_(homedir_ + "/.tiledb/profiles.json")
    , old_filepath_(homedir_ + "/.tiledb/cloud.json")
    , param_values_(default_values) {
  // Ensure the user's $HOME is found
  if (homedir_.empty()) {
    throw RestProfileException(
        "Failed to create RestProfile; $HOME is not set.");
  }

  // Fstream cannot create directories. If `homedir_/.tiledb/` DNE, create it.
  if (!std::filesystem::exists(homedir_ + "/.tiledb")) {
    std::filesystem::create_directory(homedir_ + "/.tiledb");
  }

  // If the local file exists, load the profile with the given name.
  if (std::filesystem::exists(filepath_)) {
    load_from_json_file(filepath_);
  } else {
    // If the old version of the file exists, load the profile from there
    if (std::filesystem::exists(old_filepath_)) {
      load_from_json_file(old_filepath_);
    }
  }
}

/* ****************************** */
/*              API               */
/* ****************************** */

void RestProfile::set(const std::string& param, const std::string& value) {
  // Validate incoming parameter name
  if (default_values.count(param) == 0) {
    throw RestProfileException(
        "Failed to set parameter of invalid name \'" + param + "\'");
  }
  param_values_[param] = value;
}

std::string RestProfile::get(const std::string& param) const {
  auto it = param_values_.find(param);
  if (it == param_values_.end()) {
    throw RestProfileException(
        "Failed to retrieve parameter \'" + param + "\'");
  }
  return it->second;
}

/**
 * @note The `version_` will always be listed toward the end of the local file.
 * `nlohmann::json` does not preserve the structure of the original, top-level
 * json object, but rather sorts its elements alphabetically.
 * See issue [#727](https://github.com/nlohmann/json/issues/727) for details.
 */
void RestProfile::save() {
  // Validate that the profile is complete (if username is set, so is password)
  if ((param_values_["rest.username"] != RestProfile::DEFAULT_USERNAME &&
       param_values_["rest.password"] == RestProfile::DEFAULT_PASSWORD) ||
      (param_values_["rest.username"] == RestProfile::DEFAULT_USERNAME &&
       param_values_["rest.password"] != RestProfile::DEFAULT_PASSWORD)) {
    throw RestProfileException(
        "Failed to save RestProfile; invalid username/password pairing.");
  }

  // If the file already exists, load it into a json object.
  json data;
  std::fstream file;
  std::string original_filepath = filepath_;
  if (std::filesystem::exists(filepath_)) {
    // Temporarily append filename with a random label to guarantee atomicity.
    filepath_ = filepath_ + random_label();
    if (std::rename(original_filepath.c_str(), filepath_.c_str()) != 0) {
      throw RestProfileException(
          "Failed to save RestProfile due to an internal error.");
    }

    // Read the file into the json object.
    file.open(filepath_, std::ofstream::in);
    file >> data;
    file.close();

    // If the file is outdated, throw an error. This behavior will evolve.
    if (data["version"] < version_) {
      throw RestProfileException(
          "The version of your local profile.json file is out of date.");
    }

    // If a profile of the given name already exists, remove it.
    if (data.contains(name_)) {
      data.erase(name_);
    }
  } else {
    // Write the version number iff this is the first time opening the file.
    data.push_back(json::object_t::value_type("version", version_));
  }

  // Add this profile to the json object.
  data.push_back(json::object_t::value_type(name_, to_json()));

  // Write to the file, which will be created if it does not yet exist.
  file.open(filepath_, std::ofstream::out);
  file << std::setw(2) << data << std::endl;
  file.close();

  // Remove the random label from the filename, if applicable.
  if (strcmp(filepath_.c_str(), original_filepath.c_str()) != 0) {
    if (std::rename(filepath_.c_str(), original_filepath.c_str()) != 0) {
      throw RestProfileException(
          "Failed to save RestProfile due to an internal error.");
    }
  }
}

void RestProfile::remove() {
  std::string original_filepath = filepath_;
  if (std::filesystem::exists(filepath_)) {
    // Temporarily append filename with a random label to guarantee atomicity.
    filepath_ = filepath_ + random_label();
    if (std::rename(original_filepath.c_str(), filepath_.c_str()) != 0) {
      throw RestProfileException(
          "Failed to remove RestProfile due to an internal error.");
    }

    // Read the file into a json object.
    json data;
    std::fstream file;
    file.open(filepath_, std::ofstream::in);
    file >> data;
    file.close();

    // If a profile of the given name exists, remove it.
    data.erase(data.find(name_));

    // Write the json back to the file.
    file.open(filepath_, std::ofstream::out);
    file << std::setw(2) << data << std::endl;
    file.close();
  }

  // Remove the random label from the filename, if applicable.
  if (strcmp(filepath_.c_str(), original_filepath.c_str()) != 0) {
    if (std::rename(filepath_.c_str(), original_filepath.c_str()) != 0) {
      throw RestProfileException(
          "Failed to remove RestProfile due to an internal error.");
    }
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

}  // namespace tiledb::sm
