/*
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
 */

/**
 * NOTE: The values of these enums are serialized to the array schema and/or
 * fragment metadata. Therefore, the values below should never change,
 * otherwise backwards compatibility breaks.
 */

// clang-format off
#ifdef TILEDB_QUERY_TYPE_ENUM
  /** Read query */
  TILEDB_QUERY_TYPE_ENUM(READ) = 0,
  /** Write query */
  TILEDB_QUERY_TYPE_ENUM(WRITE) = 1,
  /** Delete query */
  TILEDB_QUERY_TYPE_ENUM_CONCAT_ID(DEL,ETE) = 2,
  /** Update query */
  TILEDB_QUERY_TYPE_ENUM(UPDATE) = 3,
  /** Exclusive Modification query */
  TILEDB_QUERY_TYPE_ENUM(MODIFY_EXCLUSIVE) = 4,
#endif
