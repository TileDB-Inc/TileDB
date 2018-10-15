/**
 * @file   encryption.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This is a part of the TileDB encryption tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/encryption.html
 *
 * When run, this program will create an encrypted 2D dense array, write some
 * data to it, and read a slice of the data back.
 *
 */

#include <cstring>
#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

// Name of array.
std::string array_name("encrypted_array");

// The 256-bit encryption key, stored as a string for convenience.
const char encryption_key[32 + 1] = "0123456789abcdeF0123456789abcdeF";

void create_array() {
  // Create a TileDB context.
  Context ctx;

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

  // The array will be dense.
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(Attribute::create<int>(ctx, "a"));

  // Create the (empty) encrypted array with AES-256-GCM.
  Array::create(
      array_name,
      schema,
      TILEDB_AES_256_GCM,
      encryption_key,
      (uint32_t)strlen(encryption_key));
}

void write_array() {
  Context ctx;

  // Prepare some data for the array
  std::vector<int> data = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  // Open the encrypted array for writing and create the query.
  Array array(
      ctx,
      array_name,
      TILEDB_WRITE,
      TILEDB_AES_256_GCM,
      encryption_key,
      (uint32_t)strlen(encryption_key));
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR).set_buffer("a", data);

  // Perform the write and close the array.
  query.submit();
  array.close();
}

void read_array() {
  Context ctx;

  // Open the encrypted array for reading
  Array array(
      ctx,
      array_name,
      TILEDB_READ,
      TILEDB_AES_256_GCM,
      encryption_key,
      (uint32_t)strlen(encryption_key));

  // Slice only rows 1, 2 and cols 2, 3, 4
  const std::vector<int> subarray = {1, 2, 2, 4};

  // Prepare the vector that will hold the result (of size 6 elements)
  std::vector<int> data(6);

  // Prepare the query
  Query query(ctx, array);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a", data);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  for (auto d : data)
    std::cout << d << " ";
  std::cout << "\n";
}

int main() {
  Context ctx;

  if (Object::object(ctx, array_name).type() != Object::Type::Array) {
    create_array();
    write_array();
  }

  read_array();
  return 0;
}