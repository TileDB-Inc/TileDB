/*
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
#ifdef TILEDB_DATATYPE_ENUM
  /** 32-bit signed integer */
  TILEDB_DATATYPE_ENUM(INT32) = 0,
  /** 64-bit signed integer */
  TILEDB_DATATYPE_ENUM(INT64) = 1,
  /** 32-bit floating point value */
  TILEDB_DATATYPE_ENUM(FLOAT32) = 2,
  /** 64-bit floating point value */
  TILEDB_DATATYPE_ENUM(FLOAT64) = 3,
  /** Character */
  TILEDB_DATATYPE_ENUM(CHAR) = 4,
  /** 8-bit signed integer */
  TILEDB_DATATYPE_ENUM(INT8) = 5,
  /** 8-bit unsigned integer */
  TILEDB_DATATYPE_ENUM(UINT8) = 6,
  /** 16-bit signed integer */
  TILEDB_DATATYPE_ENUM(INT16) = 7,
  /** 16-bit unsigned integer */
  TILEDB_DATATYPE_ENUM(UINT16) = 8,
  /** 32-bit unsigned integer */
  TILEDB_DATATYPE_ENUM(UINT32) = 9,
  /** 64-bit unsigned integer */
  TILEDB_DATATYPE_ENUM(UINT64) = 10,
  /** ASCII string */
  TILEDB_DATATYPE_ENUM(STRING_ASCII) = 11,
  /** UTF-8 string */
  TILEDB_DATATYPE_ENUM(STRING_UTF8) = 12,
  /** UTF-16 string */
  TILEDB_DATATYPE_ENUM(STRING_UTF16) = 13,
  /** UTF-32 string */
  TILEDB_DATATYPE_ENUM(STRING_UTF32) = 14,
  /** UCS2 string */
  TILEDB_DATATYPE_ENUM(STRING_UCS2) = 15,
  /** UCS4 string */
  TILEDB_DATATYPE_ENUM(STRING_UCS4) = 16,
  /** This can be any datatype. Must store (type tag, value) pairs. */
  TILEDB_DATATYPE_ENUM(ANY) = 17,
  /** Datetime with year resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_YEAR) = 18,
  /** Datetime with month resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_MONTH) = 19,
  /** Datetime with week resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_WEEK) = 20,
  /** Datetime with day resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_DAY) = 21,
  /** Datetime with hour resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_HR) = 22,
  /** Datetime with minute resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_MIN) = 23,
  /** Datetime with second resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_SEC) = 24,
  /** Datetime with millisecond resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_MS) = 25,
  /** Datetime with microsecond resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_US) = 26,
  /** Datetime with nanosecond resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_NS) = 27,
  /** Datetime with picosecond resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_PS) = 28,
  /** Datetime with femtosecond resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_FS) = 29,
  /** Datetime with attosecond resolution */
  TILEDB_DATATYPE_ENUM(DATETIME_AS) = 30,
  /** Time with hour resolution */
  TILEDB_DATATYPE_ENUM(TIME_HR) = 31,
  /** Time with minute resolution */
  TILEDB_DATATYPE_ENUM(TIME_MIN) = 32,
  /** Time with second resolution */
  TILEDB_DATATYPE_ENUM(TIME_SEC) = 33,
  /** Time with millisecond resolution */
  TILEDB_DATATYPE_ENUM(TIME_MS) = 34,
  /** Time with microsecond resolution */
  TILEDB_DATATYPE_ENUM(TIME_US) = 35,
  /** Time with nanosecond resolution */
  TILEDB_DATATYPE_ENUM(TIME_NS) = 36,
  /** Time with picosecond resolution */
  TILEDB_DATATYPE_ENUM(TIME_PS) = 37,
  /** Time with femtosecond resolution */
  TILEDB_DATATYPE_ENUM(TIME_FS) = 38,
  /** Time with attosecond resolution */
  TILEDB_DATATYPE_ENUM(TIME_AS) = 39,
  /** std::byte */
  TILEDB_DATATYPE_ENUM(BLOB) = 40,
  /** Boolean */
  TILEDB_DATATYPE_ENUM(BOOL) = 41,
  /** Geometry data in well-known binary (WKB) format, stored as std::byte */
  TILEDB_DATATYPE_ENUM(GEOM_WKB) = 42,
  /** Geometry data in well-known text (WKT) format, stored as std::byte */
  TILEDB_DATATYPE_ENUM(GEOM_WKT) = 43,
#endif
