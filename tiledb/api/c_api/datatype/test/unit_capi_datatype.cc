/**
 * @file tiledb/api/c_api/datatype/test/unit_capi_datatype.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB Inc.
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
 * Tests the datatype C API.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/api/c_api/datatype/datatype_api_external.h"

#include <string>

struct TestCase {
  TestCase(tiledb_datatype_t dtype, const char* name, int defined_as)
      : dtype_(dtype)
      , name_(name)
      , defined_as_(defined_as) {
  }

  tiledb_datatype_t dtype_;
  const char* name_;
  int defined_as_;

  void run() {
    const char* c_str = nullptr;
    tiledb_datatype_t from_str;

    REQUIRE(dtype_ == defined_as_);

    REQUIRE(tiledb_datatype_to_str(dtype_, &c_str) == TILEDB_OK);
    REQUIRE(std::string(c_str) == name_);

    REQUIRE(tiledb_datatype_from_str(name_, &from_str) == TILEDB_OK);
    REQUIRE(from_str == dtype_);
  }
};

TEST_CASE(
    "C API: Test datatype enum string conversion", "[capi][enums][datatype]") {
  // clang-format off
  TestCase test = GENERATE(
    TestCase(TILEDB_INT32,          "INT32",          0),
    TestCase(TILEDB_INT64,          "INT64",          1),
    TestCase(TILEDB_FLOAT32,        "FLOAT32",        2),
    TestCase(TILEDB_FLOAT64,        "FLOAT64",        3),
    TestCase(TILEDB_CHAR,           "CHAR",           4),
    TestCase(TILEDB_INT8,           "INT8",           5),
    TestCase(TILEDB_UINT8,          "UINT8",          6),
    TestCase(TILEDB_INT16,          "INT16",          7),
    TestCase(TILEDB_UINT16,         "UINT16",         8),
    TestCase(TILEDB_UINT32,         "UINT32",         9),
    TestCase(TILEDB_UINT64,         "UINT64",         10),
    TestCase(TILEDB_STRING_ASCII,   "STRING_ASCII",   11),
    TestCase(TILEDB_STRING_UTF8,    "STRING_UTF8",    12),
    TestCase(TILEDB_STRING_UTF16,   "STRING_UTF16",   13),
    TestCase(TILEDB_STRING_UTF32,   "STRING_UTF32",   14),
    TestCase(TILEDB_STRING_UCS2,    "STRING_UCS2",    15),
    TestCase(TILEDB_STRING_UCS4,    "STRING_UCS4",    16),
    TestCase(TILEDB_ANY,            "ANY",            17),
    TestCase(TILEDB_DATETIME_YEAR,  "DATETIME_YEAR",  18),
    TestCase(TILEDB_DATETIME_MONTH, "DATETIME_MONTH", 19),
    TestCase(TILEDB_DATETIME_WEEK,  "DATETIME_WEEK",  20),
    TestCase(TILEDB_DATETIME_DAY,   "DATETIME_DAY",   21),
    TestCase(TILEDB_DATETIME_HR,    "DATETIME_HR",    22),
    TestCase(TILEDB_DATETIME_MIN,   "DATETIME_MIN",   23),
    TestCase(TILEDB_DATETIME_SEC,   "DATETIME_SEC",   24),
    TestCase(TILEDB_DATETIME_MS,    "DATETIME_MS",    25),
    TestCase(TILEDB_DATETIME_US,    "DATETIME_US",    26),
    TestCase(TILEDB_DATETIME_NS,    "DATETIME_NS",    27),
    TestCase(TILEDB_DATETIME_PS,    "DATETIME_PS",    28),
    TestCase(TILEDB_DATETIME_FS,    "DATETIME_FS",    29),
    TestCase(TILEDB_DATETIME_AS,    "DATETIME_AS",    30),
    TestCase(TILEDB_TIME_HR,        "TIME_HR",        31),
    TestCase(TILEDB_TIME_MIN,       "TIME_MIN",       32),
    TestCase(TILEDB_TIME_SEC,       "TIME_SEC",       33),
    TestCase(TILEDB_TIME_MS,        "TIME_MS",        34),
    TestCase(TILEDB_TIME_US,        "TIME_US",        35),
    TestCase(TILEDB_TIME_NS,        "TIME_NS",        36),
    TestCase(TILEDB_TIME_PS,        "TIME_PS",        37),
    TestCase(TILEDB_TIME_FS,        "TIME_FS",        38),
    TestCase(TILEDB_TIME_AS,        "TIME_AS",        39),
    TestCase(TILEDB_BLOB,           "BLOB",           40),
    TestCase(TILEDB_BOOL,           "BOOL",           41),
    TestCase(TILEDB_GEOM_WKB,       "GEOM_WKB",       42),
    TestCase(TILEDB_GEOM_WKT,       "GEOM_WKT",       43));

  DYNAMIC_SECTION("[" << test.name_ << "]") {
    test.run();
  }
}
