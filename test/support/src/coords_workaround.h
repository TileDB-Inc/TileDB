/**
 * @file /test/support/src/coords_workaround.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the symbol TILEDB_COORDS, a legacy symbol that used to be
 * exposed through the API.
 */

#ifndef TILEDB_TEST_COORDS_WORKAROUND_H
#define TILEDB_TEST_COORDS_WORKAROUND_H

namespace tiledb::test {
/**
 * This symbol TILEDB_COORDS used to be defined as a preprocessor definition
 * that called a now-removed C API function that returned a constant string.
 * At the time the API function was removed there remained a large number of
 * internal uses of the symbol for testing. This symbol was used for a
 * particular buffer layout for query results ("zipped coordinates") that was
 * long ago deprecated. The tests were never updated.
 *
 * This symbol is defined here to allow tests to continue to compile for the
 * interim until the tests have been rewritten to eliminate its appearance
 * entirely. New code must not use this symbol.
 *
 * This definition duplicates that in `tiledb::sm::constants::coords`. This is
 * intentional, since it simplifies linking by avoiding an object file.
 */
constexpr auto TILEDB_COORDS = "__coords";
}  // End of namespace tiledb::test

#endif
