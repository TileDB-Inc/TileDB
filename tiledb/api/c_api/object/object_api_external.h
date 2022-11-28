/**
 * @file tiledb/api/c_api/object/object_api_external.h
 *
 * @section LICENSE
 *
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
 *
 * @section DESCRIPTION
 *
 * This file declares the datatype C API for TileDB.
 */

#ifndef TILEDB_CAPI_OBJECT_API_EXTERNAL_H
#define TILEDB_CAPI_OBJECT_API_EXTERNAL_H

#include "../api_external_common.h"
#include "../context/context_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** TileDB object type. */
typedef enum {
/** Helper macro for defining object type enums. */
#define TILEDB_OBJECT_TYPE_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/object/object_api_enum.h"
#undef TILEDB_OBJECT_TYPE_ENUM
} tiledb_object_t;

/** Walk traversal order. */
typedef enum {
/** Helper macro for defining walk order enums. */
#define TILEDB_WALK_ORDER_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/object/object_api_enum.h"
#undef TILEDB_WALK_ORDER_ENUM
} tiledb_walk_order_t;

/**
 * Returns a string representation of the given object type.
 *
 * @param object_type Object type
 * @param str Set to point to a constant string representation of the object
 * type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_object_type_to_str(
    tiledb_object_t object_type, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a object type from the given string.
 *
 * @param str String representation to parse
 * @param object_type Set to the parsed object type
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_object_type_from_str(
    const char* str, tiledb_object_t* object_type) TILEDB_NOEXCEPT;

/**
 * Returns a string representation of the given walk order.
 *
 * @param walk_order Walk order
 * @param str Set to point to a constant string representation of the walk order
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_walk_order_to_str(
    tiledb_walk_order_t walk_order, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a walk order from the given string.
 *
 * @param str String representation to parse
 * @param walk_order Set to the parsed walk order
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_walk_order_from_str(
    const char* str, tiledb_walk_order_t* walk_order) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_DATATYPE_API_EXTERNAL_H
