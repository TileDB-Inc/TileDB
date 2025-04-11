/**
 * @file tiledb/api/c_api/profile/profile_api_experimental.h
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
 * This file declares the profile C API for TileDB.
 */

#ifndef TILEDB_CAPI_PROFILE_EXPERIMENTAL_H
#define TILEDB_CAPI_PROFILE_EXPERIMENTAL_H

#include "../api_external_common.h"
#include "tiledb/common/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB profile. */
typedef struct tiledb_profile_handle_t tiledb_profile_t;

/**
 * Allocates an in-test TileDB profile object.
 *
 * @note Directly passing `homedir` is intended primarily for testing purposes,
 * to preserve the user's `$HOME` path and their profiles from in-test changes.
 * Users may specifiy this path, or use `nullptr` for the default.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_profile_alloc(
 *   "my_profile",
 *   TemporaryLocalDirectory("unit_capi_profile").path(),
 *   &profile);
 *
 * tiledb_profile_t* profile1;
 * tiledb_profile_alloc(nullptr, nullptr, &profile1);
 * @endcode
 *
 * @param[in] name The profile name, or `nullptr` for default.
 * @param[in] homedir The path to `$HOME` directory, or `nullptr` for default.
 * @param[out] profile The profile object to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_profile_alloc(
    const char* name,
    const char* homedir,
    tiledb_profile_t** profile) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB profile object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_profile_alloc(&profile);
 * tiledb_profile_set_param("rest.username", "my_username");
 * tiledb_profile_save();
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile object to be freed.
 */
TILEDB_EXPORT capi_return_t tiledb_profile_free(tiledb_profile_t** profile)
    TILEDB_NOEXCEPT;

/**
 * Retrieves the name from the given profile.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_profile_alloc(&profile);
 * const char* name;
 * tiledb_profile_get_name(profile, &name);
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile.
 * @param[out] name The name of the profile, to be retrieved.
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_get_name(
    tiledb_profile_t* profile, const char** name) TILEDB_NOEXCEPT;

/**
 * Retrieves the homedir from the given profile.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_profile_alloc(&profile);
 * const char* homedir;
 * tiledb_profile_get_homedir(profile, &homedir);
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile.
 * @param[out] homedir The homedir of the profile, to be retrieved.
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_get_homedir(
    tiledb_profile_t* profile, const char** homedir) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_PROFILE_EXPERIMENTAL_H
