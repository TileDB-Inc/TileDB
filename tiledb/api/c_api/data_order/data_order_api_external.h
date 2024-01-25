/**
 * @file tiledb/api/c_api/data_order/data_order_api_external.h
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
 * This file declares the data order C API for TileDB.
 */

#ifndef TILEDB_CAPI_DATA_ORDER_API_EXTERNAL_H
#define TILEDB_CAPI_DATA_ORDER_API_EXTERNAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/** DataOrder Type*/
typedef enum {
/** Helper macro for defining DataOrder enums. */
#define TILEDB_DATA_ORDER_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/data_order/data_order_api_enum.h"
#undef TILEDB_DATA_ORDER_ENUM
} tiledb_data_order_t;

/**
 * Returns a string representation of the given data order.
 *
 * @param[in] data_order The target data order.
 * @param[out] str A constant string representation of the data order.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_data_order_to_str(
    tiledb_data_order_t data_order, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses the data order from the given string.
 *
 * @param[in] str Target string to parse.
 * @param[out] data_order The data order defined by the input string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_data_order_from_str(
    const char* str, tiledb_data_order_t* data_order) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_DATA_ORDER_API_EXTERNAL_H
