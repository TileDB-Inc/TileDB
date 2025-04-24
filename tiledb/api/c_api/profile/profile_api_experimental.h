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
#include "../error/error_api_external.h"
#include "../string/string_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB profile. */
typedef struct tiledb_profile_handle_t tiledb_profile_t;

/**
 * Allocates a TileDB profile object.
 *
 * @note Users may specify the path, or use `nullptr` for the default.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_error_t* error = NULL;
 * tiledb_profile_alloc(
 *   "my_profile",
 *   TemporaryLocalDirectory("unit_capi_profile").path(),
 *   &profile,
 *   &error);
 *
 * tiledb_profile_t* profile1;
 * tiledb_error_t* error1 = NULL;
 * tiledb_profile_alloc(nullptr, nullptr, &profile1, &error1);
 * @endcode
 *
 * @param[in] name The profile name, or `nullptr` for default.
 * @param[in] homedir The path to `$HOME` directory, or `nullptr` for default.
 * @param[out] profile The profile object to be created.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_profile_alloc(
    const char* name,
    const char* homedir,
    tiledb_profile_t** profile,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Frees a TileDB profile object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_error_t* error = NULL;
 * tiledb_profile_alloc(&profile, &error);
 * tiledb_profile_set_param("rest.username", "my_username");
 * tiledb_profile_save();
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile object to be freed.
 */
TILEDB_EXPORT void tiledb_profile_free(tiledb_profile_t** profile)
    TILEDB_NOEXCEPT;

/**
 * Retrieves the name from the given profile.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_error_t* error = NULL;
 * tiledb_profile_alloc(&profile, &error);
 * tiledb_string_t* name;
 * tiledb_profile_get_name(profile, &name);
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile.
 * @param[out] name The name of the profile, to be retrieved.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_get_name(
    tiledb_profile_t* profile,
    tiledb_string_t** name,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Retrieves the homedir from the given profile.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_error_t* error = NULL;
 * tiledb_profile_alloc(&profile, &error);
 * tiledb_string_t* homedir;
 * tiledb_profile_get_homedir(profile, &homedir);
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile.
 * @param[out] homedir The homedir of the profile, to be retrieved.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_get_homedir(
    tiledb_profile_t* profile,
    tiledb_string_t** homedir,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Sets a parameter in the given profile.
 *
 * @param[in] profile The profile.
 * @param[in] param The parameter name.
 * @param[in] value The parameter value.
 * @param[out] error Error object returned upon error (`NULL` if there is no
 * error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_set_param(
    tiledb_profile_t* profile,
    const char* param,
    const char* value,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Retrieves a parameter value from the given profile.
 *
 * @param[in] profile The profile.
 * @param[in] param The parameter name.
 * @param[out] value The parameter value.
 * @param[out] error Error object returned upon error (`NULL` if there is no
 * error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_get_param(
    tiledb_profile_t* profile,
    const char* param,
    tiledb_string_t** value,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Saves the given profile to the local file.
 *
 * @param[in] profile The profile.
 * @param[out] error Error object returned upon error (`NULL` if there is no
 * error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_save(
    tiledb_profile_t* profile, tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Removes the given profile from the local file.
 *
 * @param[in] profile The profile.
 * @param[out] error Error object returned upon error (`NULL` if there is no
 * error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_remove(
    tiledb_profile_t* profile, tiledb_error_t** error) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_PROFILE_EXPERIMENTAL_H
