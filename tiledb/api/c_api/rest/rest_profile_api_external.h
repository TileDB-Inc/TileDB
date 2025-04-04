/**
 * @file tiledb/api/c_api/rest/rest_profile_api_external.h
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
 * This file declares the RestProfile C API for TileDB.
 */

#ifndef TILEDB_CAPI_REST_PROFILE_EXTERNAL_H
#define TILEDB_CAPI_REST_PROFILE_EXTERNAL_H

#include "../api_external_common.h"
#include "tiledb/common/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB RestProfile. */
typedef struct tiledb_rest_profile_handle_t tiledb_rest_profile_t;

/**
 * Allocates a TileDB RestProfile object.
 *
 * Accepts a name parameter, or `nullptr` to use the internal default.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_rest_profile_t* rest_profile;
 * tiledb_rest_profile_alloc("my_rest_profile", &rest_profile);
 *
 * tiledb_rest_profile_t* rest_profile1;
 * tiledb_rest_profile_alloc(nullptr, &rest_profile1);
 * @endcode
 *
 * @param[in] name A rest_profile name, or `nullptr` for default.
 * @param[out] rest_profile The rest_profile object to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_rest_profile_alloc(
    const char* name, tiledb_rest_profile_t** rest_profile) TILEDB_NOEXCEPT;

/**
 * Allocates an in-test TileDB RestProfile object.
 *
 * @note Intended for testing purposes only, to preserve the user's
 * `$HOME` path and their profiles from in-test changes.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_rest_profile_t* rest_profile;
 * tiledb_rest_profile_alloc(
 *   "my_rest_profile",
 *   TemporaryLocalDirectory("unit_capi_rest_profile").path(),
 *   &rest_profile);
 * @endcode
 *
 * @param[in] name The rest_profile name.
 * @param[in] homedir The path to the in-test `$HOME` directory.
 * @param[out] rest_profile The rest_profile object to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_rest_profile_alloc_test(
    const char* name,
    const char* homedir,
    tiledb_rest_profile_t** rest_profile) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB RestProfile object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_rest_profile_t* rest_profile;
 * tiledb_rest_profile_alloc(&rest_profile);
 * tiledb_rest_profile_set_param("rest.username", "my_username");
 * tiledb_rest_profile_save();
 * tiledb_rest_profile_free(&rest_profile);
 * @endcode
 *
 * @param[in] rest_profile The rest_profile object to be freed.
 */
TILEDB_EXPORT void tiledb_rest_profile_free(
    tiledb_rest_profile_t** rest_profile) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_REST_PROFILE_EXTERNAL_H
