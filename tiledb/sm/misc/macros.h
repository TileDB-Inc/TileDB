/**
 * @file   macros.h
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
 * This file declares common preprocessor macros.
 */

#ifndef TILEDB_MACROS_H
#define TILEDB_MACROS_H

/** Disables the copy constructor for class 'C'. */
#define DISABLE_COPY(C) C(const C&) = delete

/** Disables the copy-assign operator for class 'C'. */
#define DISABLE_COPY_ASSIGN(C) C& operator=(const C&) = delete

/** Disables the move constructor for class 'C'. */
#define DISABLE_MOVE(C) C(C&&) = delete

/** Disables the move-assign operator for class 'C'. */
#define DISABLE_MOVE_ASSIGN(C) C& operator=(C&&) = delete

/** Disables the copy constructor and assign operator for class 'C'. */
#define DISABLE_COPY_AND_COPY_ASSIGN(C) \
  DISABLE_COPY(C);                      \
  DISABLE_COPY_ASSIGN(C)

/** Disables the move-copy constructor and move-assign operator for class 'C'.
 */
#define DISABLE_MOVE_AND_MOVE_ASSIGN(C) \
  DISABLE_MOVE(C);                      \
  DISABLE_MOVE_ASSIGN(C)

#endif  // TILEDB_MACROS_H