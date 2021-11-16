/**
 * @file   errors.cc
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
 * This example shows how to catch errors in TileDB.
 */

#include <iostream>
#include <tiledb/tiledb>

using namespace tiledb;

int main() {
  // Create TileDB context
  Context ctx;

  // Catch an error
  try {
    create_group(ctx, "my_group");
    create_group(ctx, "my_group");
  } catch (tiledb::TileDBError& e) {
    Error err(ctx);
    std::string msg = err.error_message();
    std::cout << "Last error: " << msg << "\n";
    std::cout << "TileDB exception:\n" << e.what() << "\n";
  }

  // Set a different error handler
  ctx.set_error_handler([](std::string msg) {
    std::cout << "Callback:\n" << msg << "\n";
  });
  create_group(ctx, "my_group");

  return 0;
}
