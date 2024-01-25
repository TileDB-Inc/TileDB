/**
 * @file tiledb/api/c_api/datatype/datatype_api_external.h
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

#ifndef TILEDB_CAPI_DATATYPE_API_EXTERNAL_H
#define TILEDB_CAPI_DATATYPE_API_EXTERNAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/** TileDB datatype. */
typedef enum {
/** Helper macro for defining datatype enums. */
#define TILEDB_DATATYPE_ENUM(id) TILEDB_##id
#include "tiledb/api/c_api/datatype/datatype_api_enum.h"
#undef TILEDB_DATATYPE_ENUM
#ifdef TILEDB_CHAR
#define TILEDB_CHAR_VAL TILEDB_CHAR
#undef TILEDB_CHAR
#define TILEDB_CHAR TILEDB_DEPRECATED TILEDB_CHAR_VAL
#undef TILEDB_CHAR_VAL
#endif
#ifdef TILEDB_STRING_UCS2
#define TILEDB_STRING_UCS2_VAL TILEDB_STRING_UCS2
#undef TILEDB_STRING_UCS2
#define TILEDB_STRING_UCS2 TILEDB_DEPRECATED TILEDB_STRING_UCS2_VAL
#undef TILEDB_STRING_UCS2_VAL
#endif
#ifdef TILEDB_STRING_UCS4
#define TILEDB_STRING_UCS4_VAL TILEDB_STRING_UCS4
#undef TILEDB_STRING_UCS4
#define TILEDB_STRING_UCS4 TILEDB_DEPRECATED TILEDB_STRING_UCS4_VAL
#undef TILEDB_STRING_UCS4_VAL
#endif
#ifdef TILEDB_ANY
#define TILEDB_ANY_VAL TILEDB_ANY
#undef TILEDB_ANY
#define TILEDB_ANY TILEDB_DEPRECATED TILEDB_ANY_VAL
#undef TILEDB_ANY_VAL
#endif
} tiledb_datatype_t;

/**
 * Returns a string representation of the given datatype.
 *
 * @param datatype Datatype
 * @param str Set to point to a constant string representation of the datatype
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_datatype_to_str(
    tiledb_datatype_t datatype, const char** str) TILEDB_NOEXCEPT;

/**
 * Parses a datatype from the given string.
 *
 * @param str String representation to parse
 * @param datatype Set to the parsed datatype
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_datatype_from_str(
    const char* str, tiledb_datatype_t* datatype) TILEDB_NOEXCEPT;

/**
 * Returns the input datatype size for a given type. Returns zero if the type is
 * not valid.
 */
TILEDB_EXPORT uint64_t tiledb_datatype_size(tiledb_datatype_t type)
    TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_DATATYPE_API_EXTERNAL_H
