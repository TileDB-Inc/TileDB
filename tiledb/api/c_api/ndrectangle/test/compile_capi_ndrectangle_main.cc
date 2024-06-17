/**
 * @file tiledb/api/c_api/ndrectangle/test/compile_capi_ndrectangle_main.cc
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
 */

#include "../ndrectangle_api_external_experimental.h"

int main() {
  tiledb_ndrectangle_alloc(nullptr, nullptr, nullptr);
  tiledb_ndrectangle_free(nullptr);
  tiledb_ndrectangle_get_range_from_name(nullptr, nullptr, nullptr, nullptr);
  tiledb_ndrectangle_get_range(nullptr, nullptr, 0, nullptr);
  tiledb_ndrectangle_set_range_for_name(nullptr, nullptr, nullptr, nullptr);
  tiledb_ndrectangle_set_range(nullptr, nullptr, 0, nullptr);

  return 0;
}
