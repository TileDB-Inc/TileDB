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
 * When run, this program will create a named profile with some custom rest
 * parameters, save it in the default profiles location
 * ({homedir}/.tiledb/profiles.json), and then create a config object that uses
 * this profile. It will then print the parameters of the config object, which
 * should come from the profile. Finally, it will create an array using the
 * profile, and then remove the profile.
 */

#include <iostream>
#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

using namespace tiledb;

void create_and_save_profile(const std::string& profile_name) {
  Profile profile(profile_name);
  profile.set_param("rest.token", "my_custom_token");
  profile.set_param("rest.server_address", "https://my.custom.server.address");
  profile.save();
}

void print_config(const std::string& profile_name) {
  // Create a config object and set the profile to use.
  Config config;
  config.set_profile(profile_name);

  // Print the parameters of the config. They should come from the profile.
  std::cout << "Config parameters coming from profile " << profile_name << ":"
            << std::endl;
  std::cout << "rest.token: " << config.get("rest.token") << std::endl;
  std::cout << "rest.server_address: " << config.get("rest.server_address")
            << std::endl
            << std::endl;
}

void create_array_with_profile(const std::string& profile_name) {
  // Create a config object and set the profile to use.
  Config config;
  config.set_profile(profile_name);
  // Create a context using the config
  Context ctx(config);

  // Create a schema for an array
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(Domain(ctx).add_dimension(
      Dimension::create<int32_t>(ctx, "d1", {{1, 100}}, 10)));
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_capacity(100);
  schema.add_attributes(Attribute::create<int32_t>(ctx, "a1"));
  schema.add_attributes(Attribute::create<int32_t>(ctx, "a2"));
  schema.check();
  // Create an array using the schema and the credentials from the profile
  const std::string array_uri = "tiledb://my_workspace/my_teamspace/my_array";
  Array::create(array_uri, schema);
}

int main() {
  // IMPORTANT NOTE: in case a profile of the same name already exists it will
  // not be overwritten. If you want to overwrite it, you need to remove it
  // first.
  try {
    // Create, save, and print the config parameters for a named profile
    const std::string profile_name = "profile_example_123";
    create_and_save_profile(profile_name);
    print_config(profile_name);
    // Create an array using the profile
    create_array_with_profile(profile_name);
    // Remove the profile
    Profile::remove(profile_name);
  } catch (const ProfileException& e) {
    std::cerr << "Error creating profile: " << e.what() << std::endl;
  }

  return 0;
}
