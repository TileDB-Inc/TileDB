/**
 * @file backend_example.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * Example showing how to identify the backend type of a URI.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

int main() {
  // Create a TileDB context
  Context ctx;

  // Example URIs from different backends
  std::vector<std::string> uris = {
      "s3://my-bucket/my-array",
      "azure://my-container/my-array",
      "gcs://my-bucket/my-array",
      "gs://my-bucket/my-array",
      "https://example.com/my-array"};

  std::cout << "Backend identification example\n";
  std::cout << "==============================\n\n";

  // Check the backend for each URI
  for (const auto& uri : uris) {
    try {
      auto backend = Backend::from_uri(ctx, uri);
      std::cout << "URI:     " << uri << "\n";
      std::cout << "Backend: " << backend << "\n";

      // You can also check the backend type
      if (backend == Backend::Type::S3) {
        std::cout << "  -> This is an S3-compatible backend\n";
      } else if (backend == Backend::Type::Azure) {
        std::cout << "  -> This is an Azure backend\n";
      } else if (backend == Backend::Type::GCS) {
        std::cout << "  -> This is a Google Cloud Storage backend\n";
      }
      std::cout << "\n";
    } catch (const TileDBError& e) {
      std::cerr << "Error identifying backend for " << uri << ": " << e.what()
                << "\n\n";
    }
  }

  return 0;
}
