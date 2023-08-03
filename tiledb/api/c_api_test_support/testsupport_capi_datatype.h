/**
 * @file tiledb/api/c_api_test_support/context/testsupport_capi_datatype.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines test support classes for the datatype section of the C API.
 */

#ifndef TILEDB_TESTSUPPORT_CAPI_DATATYPE_H
#define TILEDB_TESTSUPPORT_CAPI_DATATYPE_H

#include "tiledb/api/c_api/datatype/datatype_api_external.h"

namespace tiledb::api::test_support {

/*
 * TILEDB_INVALID_TYPE is defined as a function rather than a constant to get
 * around compile-time checking that the enumeration value is valid.
 */
tiledb_datatype_t TILEDB_INVALID_TYPE() {
  return static_cast<tiledb_datatype_t>(-1);
}

}  // namespace tiledb::api::test_support
#endif  // TILEDB_TESTSUPPORT_CAPI_DATATYPE_H
