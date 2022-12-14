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
  #if (defined(DELETE))
  // note: 'DELETE' is #define'd somewhere within windows headers as
  // something resolving to '(0x00010000L)', which causes problems with
  // query_type.h which does not qualify the 'id' like tiledb.h does.
  // #undef DELETE
  #error "'DELETE' should not be defined before now in tiledb_enum.h.\nHas it seeped out from include of windows.h somewhere that needs changing?\n(Catch2 includes have been a past culprit.)\nFind error message in tiledb_enum.h for more information."
  // If this is encountered 'too often', further consideration might be given to
  // simply qualifying the currently unqualified definition of TILEDB_QUERY_TYPE_ENUM in
  // query_type.h so 'DELETE' and any other enum items here would not collide with this
  // windows definition known to be in conflict.
  #endif
  TILEDB_QUERY_TYPE_ENUM(DELETE) = 2,
  /** Update query */
  TILEDB_QUERY_TYPE_ENUM(UPDATE) = 3,
  /** Exclusive Modification query */
  TILEDB_QUERY_TYPE_ENUM(MODIFY_EXCLUSIVE) = 4,
#endif
