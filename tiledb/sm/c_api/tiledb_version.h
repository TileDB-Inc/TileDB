/*
 * @file   version.h
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
 */

#define TILEDB_VERSION_MAJOR 2
#define TILEDB_VERSION_MINOR 15
#define TILEDB_VERSION_PATCH 0

#if 0
#if _WIN32
//#define TDB_STRINGIZE(x) #x
#define TDB_STRINGIZE_HELPER(x) #x
#define TDB_STRINGIZE(x) TDB_STRINGIZE_HELPER(x)
#define TDB_WIN32_FILEVERSION \
  TILEDB_VERSION_MAJOR, TILEDB_VERSION_MINOR, TILEDB_VERSION_PATCH, 0
#define TDB_WIN32_FILEVERSION_STR                            \
  TDB_STRINGIZE(TILEDB_VERSION_MAJOR)                        \
  "," TDB_STRINGIZE(TILEDB_VERSION_MINOR) "," TDB_STRINGIZE( \
      TILEDB_VERSION_PATCH)
#endif
#endif
