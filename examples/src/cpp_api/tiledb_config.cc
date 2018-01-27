/**
 * @file   tiledb_config.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * Manipulating a configuration.
 */

#include <tiledb>

int main() {
  tiledb::Config conf;

  // Default conf
  std::cout << "Default settings:\n";
  for (auto &p : conf) {
    std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
  }

  // Change values
  conf["vfs.s3.connect_timeout_ms"] = 5000;

  // Append key fragments with successive []
  conf["vfs."]["s3."]["endpoint_override"] = "localhost:8888";

  // Only S3 settings
  std::cout << "\nVFS S3 settings:\n";
  for (auto i = conf.begin("vfs.s3."); i != conf.end(); ++i ) {
    auto &p = *i;
    std::cout << "\"" << p.first << "\" : \"" << p.second << "\"\n";
  }

  return 0;
}