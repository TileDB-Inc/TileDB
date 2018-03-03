/**
 * @file   tiledb_config.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Shows how to manipulates config parameter objects.
 *
 * Simply run:
 *
 * ./tiledb_config_cc
 *
 */

#include <tiledb/tiledb>

int main() {
  // Create a TileDB config
  tiledb::Config config;

  // Print the default config parameters
  std::cout << "Default settings:\n";
  for (auto& p : config) {
    std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
  }

  // Set values
  config["vfs.s3.connect_timeout_ms"] = 5000;

  // Append parameter segments with successive []
  config["vfs."]["s3."]["endpoint_override"] = "localhost:8888";

  // Get values
  std::string tile_cache_size = config["sm.tile_cache_size"];
  std::cout << "\nTile cache size: " << tile_cache_size << "\n";

  // Print only the S3 settings
  std::cout << "\nVFS S3 settings:\n";
  for (auto i = config.begin("vfs.s3."); i != config.end(); ++i) {
    auto& p = *i;
    std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
  }

  // Assign a config object to a context and VFS
  tiledb::Context ctx(config);
  tiledb::VFS vfs(ctx, config);

  return 0;
}