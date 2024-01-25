/**
 * @file   config.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This program shows how to set/get the TileDB configuration parameters.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

void set_get_config_ctx_vfs() {
  // Create config objects
  Config config, config_ctx, config_vfs;

  // Set/Get config to/from ctx
  Context ctx(config);
  config_ctx = ctx.config();
  (void)config_ctx;

  // Set config to VFS
  VFS vfs(ctx, config);
  config_vfs = vfs.config();
  (void)config_vfs;
}

void set_get_config() {
  Config config;

  // Set value
  config["vfs.s3.connect_timeout_ms"] = 5000;

  // Append parameter segments with successive []
  config["vfs."]["s3."]["endpoint_override"] = "localhost:8888";

  // Get value
  std::string memory_budget = config["sm.memory_budget"];
  std::cout << "Memory budget: " << memory_budget << "\n\n";
}

void print_default() {
  Config config;
  std::cout << "Default settings:\n";
  for (auto& p : config) {
    std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
  }
}

void iter_config_with_prefix() {
  Config config;

  // Print only the S3 settings
  std::cout << "\nVFS S3 settings:\n";
  for (auto i = config.begin("vfs.s3."); i != config.end(); ++i) {
    auto& p = *i;
    std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
  }
}

void save_load_config() {
  // Save to file
  Config config;
  config["sm.memory_budget"] = 0;
  config.save_to_file("tiledb_config.txt");

  // Load from file
  Config config_load("tiledb_config.txt");
  std::string memory_budget = config_load["sm.memory_budget"];
  std::cout << "\nMemory budget after loading from file: " << memory_budget
            << "\n";
}

int main() {
  set_get_config_ctx_vfs();
  set_get_config();
  print_default();
  iter_config_with_prefix();
  save_load_config();

  return 0;
}
