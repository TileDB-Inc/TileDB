/**
 * @file   info.cc
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
 * CLI tool find information about an object.
 */

#include "tiledb/array.h"
#include "tiledb/map.h"
#include "tiledb/object.h"

template <typename T>
void print_ned(const std::string& uri, const tiledb::ArraySchema& schema);
void print_ned(const std::string& uri, const tiledb::ArraySchema& schema);

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cout << "Usage: tiledb-info \"path/to/array\"\n" << std::endl;
    return 1;
  }

  tiledb::Context ctx;
  auto obj = tiledb::Object::object(ctx, argv[1]);

  // TODO trouble linking
  std::cout << "Object type: " << obj << "\n\n";

  if (obj.type() == tiledb::Object::Type::Array) {
    tiledb::ArraySchema schema(ctx, argv[1]);
    schema.dump(stdout);
    print_ned(argv[1], schema);
  } else if (obj.type() == tiledb::Object::Type::KeyValue) {
    tiledb::MapSchema schema(ctx, argv[1]);
    schema.dump(stdout);
  }

  return 0;
}

template <typename T>
void print_ned(const std::string& uri, const tiledb::ArraySchema& schema) {
  auto ned = tiledb::Array::non_empty_domain<T>(uri, schema);
  std::cout << "\n=== Non-empty domain ===\n";
  for (const auto& attr : ned) {
    auto& name = attr.first;
    auto& domain = attr.second;
    std::cout << "- " << name << ": (" << domain.first << ", " << domain.second
              << ")\n";
  }
}

void print_ned(const std::string& uri, const tiledb::ArraySchema& schema) {
  switch (schema.domain().type()) {
    case TILEDB_INT32:
      print_ned<int32_t>(uri, schema);
      break;
    case TILEDB_INT64:
      print_ned<int64_t>(uri, schema);
      break;
    case TILEDB_FLOAT32:
      print_ned<float>(uri, schema);
      break;
    case TILEDB_FLOAT64:
      print_ned<double>(uri, schema);
      break;
    case TILEDB_CHAR:
      print_ned<char>(uri, schema);
      break;
    case TILEDB_INT8:
      print_ned<int8_t>(uri, schema);
      break;
    case TILEDB_UINT8:
      print_ned<uint8_t>(uri, schema);
      break;
    case TILEDB_INT16:
      print_ned<int16_t>(uri, schema);
      break;
    case TILEDB_UINT16:
      print_ned<uint16_t>(uri, schema);
      break;
    case TILEDB_UINT32:
      print_ned<uint32_t>(uri, schema);
      break;
    case TILEDB_UINT64:
      print_ned<uint64_t>(uri, schema);
      break;
    default:
      std::cout << "No non-empty domain available for non-numeric domains.\n";
  }
}