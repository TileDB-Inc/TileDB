/**
 * @file   tdbpp_walk_ls.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Walk/list a directory for TileDB Objects.
 */

#include <tiledb>

int main() {
  tdb::Context ctx;

  std::cout << "List children: \n";
  tdb::ObjectIter obj_iter(ctx, "my_group");
  for (const auto &object : obj_iter)
    std::cout << object << '\n';

  std::cout << "\nPreorder traversal: \n";
  obj_iter.set_recursive();  // Default order is preorder
  for (const auto &object : obj_iter)
    std::cout << object << '\n';

  std::cout << "\nPostorder traversal: \n";
  obj_iter.set_recursive(TILEDB_POSTORDER);
  for (const auto &object : obj_iter)
    std::cout << object << '\n';

  std::cout << "\nOnly groups: \n";
  obj_iter.set_iter_policy(true, false, false);
  for (const auto &object : obj_iter)
    std::cout << object << '\n';

  std::cout << "\nOnly arrays and groups: \n";
  obj_iter.set_iter_policy(true, true, false);
  for (const auto &object : obj_iter)
    std::cout << object << '\n';

  return 0;
}
