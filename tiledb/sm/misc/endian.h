/**
 * @file endian.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file contains useful (global) functions.
 */

#ifndef TILEDB_MISC_ENDIAN_H
#define TILEDB_MISC_ENDIAN_H

/* ********************************* */
/*          ENDIANNESS FUNCTIONS     */
/* ********************************* */

namespace tiledb::sm::utils::endianness {

/**
 * Returns true if the current CPU architecture has little endian byte
 * ordering, false for big endian.
 */
inline bool is_little_endian() {
  const int n = 1;
  return *(char*)&n == 1;
}

/**
 * Returns true if the current CPU architecture has big endian byte
 * ordering, false for little endian.
 */
inline bool is_big_endian() {
  return !is_little_endian();
}

/**
 * Decodes a little-endian ordered buffer 'data' into a native
 * primitive type, T.
 */
template <class T>
inline T decode_le(const void* const data) {
  const T* const n = reinterpret_cast<const T* const>(data);
  if (is_little_endian()) {
    return *n;
  }

  T le_n;
  char* const p = reinterpret_cast<char*>(&le_n);
  for (size_t i = 0; i < sizeof(T); ++i) {
    p[i] = *n >> ((sizeof(T) - i - 1) * 8);
  }

  return le_n;
}

/**
 * Decodes a big-endian ordered buffer 'data' into a native
 * primitive type, T.
 */
template <class T>
inline T decode_be(const void* const data) {
  const T* const n = reinterpret_cast<const T* const>(data);
  if (is_big_endian()) {
    return *n;
  }

  T be_n;
  char* const p = reinterpret_cast<char*>(&be_n);
  for (size_t i = 0; i < sizeof(T); ++i) {
    p[i] = *n >> (i * 8);
  }

  return be_n;
}

}  // namespace tiledb::sm::utils::endianness

#endif  // TILEDB_MISC_ENDIAN_H
