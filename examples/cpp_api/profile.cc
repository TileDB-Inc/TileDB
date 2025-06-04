/**
 * @file   profile.cc
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
 * When run, this program will create a simple 2D sparse array, write some data
 * to it, and read a slice of the data back.
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

void create_and_save_profile(
    std::optional<std::string> profile_name = std::nullopt) {
  Profile profile = profile_name ? Profile(profile_name.value()) : Profile();
  profile.set_param(
      "rest.token", profile_name ? "named_custom_token" : "my_custom_token");
  profile.set_param(
      "rest.server_address",
      profile_name ? "https://named.custom.server.address" :
                     "https://my.custom.server.address");
  profile.save();
}

void print_config(std::optional<std::string> profile_name = std::nullopt) {
  // Create a config object. The default profile will be used automatically if
  // it exists.
  Config config;

  if (profile_name.has_value()) {
    config.set_profile(profile_name.value());
  }

  // Print the parameters of the config. They should come from the profile.
  std::cout << "Config parameters coming from "
            << (profile_name.has_value() ? profile_name.value() : "default")
            << " profile" << std::endl;
  std::cout << "rest.token: " << config.get("rest.token") << std::endl;
  std::cout << "rest.server_address: " << config.get("rest.server_address")
            << std::endl
            << std::endl;
}

int main() {
  // IMPORTANT NOTE: both the default and the named profiles  will not be
  // overwritten in case they already exist. If you want to overwrite them, you
  // need to remove them first.
  try {
    // Create, save, and print the config parameters for the default profile
    create_and_save_profile();
    print_config();
    // Remove the default profile ONLY if it was created as part of this example
    Profile::remove();
  } catch (const ProfileException& e) {
    std::cerr << "Error creating default profile: " << e.what() << std::endl;
  }

  try {
    // Create, save, and print the config parameters for a named profile
    const std::string profile_name = "profile_example_123";
    create_and_save_profile(profile_name);
    print_config(profile_name);
    // Remove the named profile
    Profile::remove(profile_name);
  } catch (const ProfileException& e) {
    std::cerr << "Error creating named profile: " << e.what() << std::endl;
  }

  return 0;
}
