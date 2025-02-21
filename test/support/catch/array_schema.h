/**
 * @file test/support/catch/array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file provides some common utilities for writing tests using Catch2
 * about array schemata.
 */

#ifndef TILEDB_CATCH_ARRAY_SCHEMA_H
#define TILEDB_CATCH_ARRAY_SCHEMA_H

#include "tiledb/sm/enums/datatype.h"

#define GENERATE_CPPAPI_ALL_DATATYPES()     \
  GENERATE(                                 \
      tiledb::sm::Datatype::FLOAT32,        \
      tiledb::sm::Datatype::FLOAT64,        \
      tiledb::sm::Datatype::INT8,           \
      tiledb::sm::Datatype::UINT8,          \
      tiledb::sm::Datatype::INT16,          \
      tiledb::sm::Datatype::UINT16,         \
      tiledb::sm::Datatype::INT32,          \
      tiledb::sm::Datatype::UINT32,         \
      tiledb::sm::Datatype::INT64,          \
      tiledb::sm::Datatype::UINT64,         \
      tiledb::sm::Datatype::CHAR,           \
      tiledb::sm::Datatype::STRING_ASCII,   \
      tiledb::sm::Datatype::STRING_UTF8,    \
      tiledb::sm::Datatype::STRING_UTF16,   \
      tiledb::sm::Datatype::STRING_UTF32,   \
      tiledb::sm::Datatype::STRING_UCS2,    \
      tiledb::sm::Datatype::STRING_UCS4,    \
      tiledb::sm::Datatype::DATETIME_YEAR,  \
      tiledb::sm::Datatype::DATETIME_MONTH, \
      tiledb::sm::Datatype::DATETIME_WEEK,  \
      tiledb::sm::Datatype::DATETIME_DAY,   \
      tiledb::sm::Datatype::DATETIME_HR,    \
      tiledb::sm::Datatype::DATETIME_MIN,   \
      tiledb::sm::Datatype::DATETIME_SEC,   \
      tiledb::sm::Datatype::DATETIME_MS,    \
      tiledb::sm::Datatype::DATETIME_US,    \
      tiledb::sm::Datatype::DATETIME_NS,    \
      tiledb::sm::Datatype::DATETIME_PS,    \
      tiledb::sm::Datatype::DATETIME_FS,    \
      tiledb::sm::Datatype::DATETIME_AS,    \
      tiledb::sm::Datatype::TIME_HR,        \
      tiledb::sm::Datatype::TIME_MIN,       \
      tiledb::sm::Datatype::TIME_SEC,       \
      tiledb::sm::Datatype::TIME_MS,        \
      tiledb::sm::Datatype::TIME_US,        \
      tiledb::sm::Datatype::TIME_NS,        \
      tiledb::sm::Datatype::TIME_PS,        \
      tiledb::sm::Datatype::TIME_FS,        \
      tiledb::sm::Datatype::TIME_AS,        \
      tiledb::sm::Datatype::BOOL,           \
      tiledb::sm::Datatype::BLOB,           \
      tiledb::sm::Datatype::GEOM_WKT,       \
      tiledb::sm::Datatype::GEOM_WKB,       \
      tiledb::sm::Datatype::ANY)

#endif
