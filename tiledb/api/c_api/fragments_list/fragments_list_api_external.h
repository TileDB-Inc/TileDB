/**
 * @file tiledb/api/c_api/fragments_list/fragments_list_api_external.h
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
 * This file declares the fragments list section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_FRAGMENTS_LIST_EXTERNAL_H
#define TILEDB_CAPI_FRAGMENTS_LIST_EXTERNAL_H

#include "../api_external_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB fragments list
 */
typedef struct tiledb_fragments_list_handle_t tiledb_fragments_list_t;

/**
 * Returns a view (i.e. data and length) of the uri of the fragment at the
 * given index of a TileDB fragments list object.
 *
 * **Example**
 * @code{.c}
 * tiledb_fragments_list_t* f = NULL;
 * // tiledb_deserialize_array_delete_fragments_list_request(..., &f);
 * uint32_t index = 0;
 * const char* uri;
 * size_t uri_length;
 * tiledb_fragments_list_get_fragment_uri(f, index, &uri, &uri_length);
 * printf("f[%u] = \"%s\"\n", index, uri);
 * tiledb_fragments_list_free(&f);
 * @endcode
 *
 * @param f A TileDB fragments list object
 * @param index The index at which to retrieve a fragment uri
 * @param uri The fragment uri at the given index
 * @param uri_length The length of the fragment uri at the given index
 *
 * @note Lifespan of the uri is maintained by the fragments list.
 */
TILEDB_EXPORT capi_return_t tiledb_fragments_list_get_fragment_uri(
    tiledb_fragments_list_t* f,
    uint32_t index,
    const char** uri,
    size_t* uri_length) TILEDB_NOEXCEPT;

/**
 * Returns the index of the fragment with the given uri in the given TileDB
 * fragments list object. Returns TILEDB_ERR if the fragment is not in the list.
 *
 * **Example**
 * @code{.c}
 * tiledb_fragments_list_t* f = NULL;
 * // tiledb_deserialize_array_delete_fragments_list_request(..., &f);
 * unsigned index;
 * const char* uri = "array/__fragments/1";
 * tiledb_fragments_list_get_fragment_index(f, uri, &index);
 * printf("Fragment %s is at index %u\n", uri, index);
 * tiledb_fragments_list_free(&f);
 * @endcode
 *
 * @param f A TileDB fragments list object
 * @param uri The fragment uri whose index is to be retrieved
 * @param index The index of the given uri
 */
TILEDB_EXPORT capi_return_t tiledb_fragments_list_get_fragment_index(
    tiledb_fragments_list_t* f,
    const char* uri,
    uint32_t* index) TILEDB_NOEXCEPT;

/**
 * Frees the resources associated with a TileDB fragments list object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_fragments_list_t* s = NULL;
 * // tiledb_deserialize_array_delete_fragments_list_request(..., &f);
 * tiledb_fragments_list_free(&f);
 * @endcode
 *
 * @param f A TileDB fragments list object
 */
TILEDB_EXPORT void tiledb_fragments_list_free(tiledb_fragments_list_t** f)
    TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_FRAGMENTS_LIST_EXTERNAL_H
