/**
 * @file
 * tiledb/api/c_api/query_aggregate/test/compile_capi_query_aggregate_main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include "../query_aggregate_api_external_experimental.h"

int main() {
  tiledb_query_get_default_channel(nullptr, nullptr, nullptr);
  tiledb_create_unary_aggregate(nullptr, nullptr, nullptr, nullptr, nullptr);
  tiledb_channel_apply_aggregate(nullptr, nullptr, nullptr, nullptr);
  (void)tiledb_channel_operator_sum;
  (void)tiledb_aggregate_count;
  tiledb_channel_operator_sum_get(nullptr, nullptr);
  tiledb_channel_operator_min_get(nullptr, nullptr);
  tiledb_channel_operator_max_get(nullptr, nullptr);
  tiledb_aggregate_count_get(nullptr, nullptr);
  tiledb_aggregate_free(nullptr, nullptr);
  tiledb_query_channel_free(nullptr, nullptr);

  return 0;
}
