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
 * @param[in] dir The directory path on which the profile will be stored, or
 * `nullptr` for home directory.
 * @param[out] profile The profile object to be created.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_profile_alloc(
    const char* name,
    const char* dir,
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
 * Retrieves the name of the given profile.
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
 * Retrieves the directory of the given profile.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_error_t* error = NULL;
 * tiledb_profile_alloc(&profile, &error);
 * tiledb_string_t* profile_dir;
 * tiledb_profile_get_dir(profile, &profile_dir);
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile.
 * @param[out] dir The directory of the profile, to be retrieved.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_get_dir(
    tiledb_profile_t* profile,
    tiledb_string_t** dir,
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
 * @param[out] value A pointer to the value of the parameter to be retrieved
 *    (`NULL` if it does not exist).
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
 * @param[in] overwrite If non-zero, overwrite the existing profile.
 * @param[out] error Error object returned upon error (`NULL` if there is no
 * error).
 * @return TILEDB_EXPORT
 */
TILEDB_EXPORT capi_return_t tiledb_profile_save(
    tiledb_profile_t* profile,
    uint8_t overwrite,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Loads a profile from the local file.
 *
 * TODO: Add a way to set the loaded profile to a config so that we show to the
 * user how to use it after loading it.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_profile_t* profile;
 * tiledb_error_t* error = NULL;
 * tiledb_profile_alloc(&profile, &error);
 * tiledb_profile_load(profile, &error);
 * tiledb_profile_free(&profile);
 * @endcode
 *
 * @param[in] profile The profile.
 * @param[out] error Error object returned upon error (`NULL` if there is
 *     no error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_profile_load(
    tiledb_profile_t* profile, tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Removes a profile from the profiles file in the given directory.
 *
 * @param[in] name The name of the profile to be removed. If `NULL`, the
 * default name is used.
 * @param[in] dir The directory path that contains the profiles file. If `NULL`,
 * the home directory is used.
 * @param[out] error Error object returned upon error (`NULL` if there is no
 *     error).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_profile_remove(
    const char* name, const char* dir, tiledb_error_t** error) TILEDB_NOEXCEPT;

/**
 * Dumps a string representation of the given profile.
 *
 * The output string handle must be freed by the user after use.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* tdb_string;
 * tiledb_profile_dump_str(profile, &tdb_string);
 * // Use the string
 * tiledb_string_free(&tdb_string);
 * @endcode
 *
 * @param[in] profile The profile.
 * @param[out] out The output string handle.
 * @param[out] error Error object returned upon error (`NULL` if there is no
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_profile_dump_str(
    tiledb_profile_t* profile,
    tiledb_string_t** out,
    tiledb_error_t** error) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_PROFILE_EXPERIMENTAL_H
