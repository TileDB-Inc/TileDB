/**
 * @file tiledb/api/c_api/attribute/attribute_api_external.h
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
 * This file declares the attribute section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_ATTRIBUTE_EXTERNAL_EXPERIMENTAL_H
#define TILEDB_CAPI_ATTRIBUTE_EXTERNAL_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "../string/string_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

// Matches guard in ordinary header
#ifndef TILEDB_ATTRIBUTE_HANDLE_T_DEFINED
#define TILEDB_ATTRIBUTE_HANDLE_T_DEFINED
/** A TileDB attribute */
typedef struct tiledb_attribute_handle_t tiledb_attribute_t;
#endif

/* ********************************* */
/*      ATTRIBUTE ENUMERATIONS       */
/* ********************************* */

/**
 * Set the enumeration name on an attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_set_enumeration_name(ctx, attr, "enumeration_name");
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param enumeration_name The name of the enumeration to use for the attribute.
 * @return `TILEDB_OK` for success, and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_set_enumeration_name(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    const char* enumeration_name) TILEDB_NOEXCEPT;

/**
 * Get the attribute's enumeration name if it has one.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* name;
 * tiledb_attribute_get_enumeration_name(ctx, attr, &name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param name The name of the attribute, nullptr if the attribute does not
 *        have an associated enumeration.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_attribute_get_enumeration_name(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_string_t** name) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ATTRIBUTE_EXTERNAL_EXPERIMENTAL_H
