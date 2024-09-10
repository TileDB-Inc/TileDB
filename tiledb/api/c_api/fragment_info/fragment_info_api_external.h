/**
 * @file tiledb/api/c_api/fragment_info/fragment_info_api_external.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the FragmentInfo C API for TileDB.
 */

#ifndef TILEDB_CAPI_FRAGMENT_INFO_EXTERNAL_H
#define TILEDB_CAPI_FRAGMENT_INFO_EXTERNAL_H

#include "../api_external_common.h"
#include "../array_schema/array_schema_api_external.h"
#include "../context/context_api_external.h"
#include "../string/string_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/** C API carrier for a TileDB fragment info object. */
typedef struct tiledb_fragment_info_handle_t tiledb_fragment_info_t;

/**
 * Creates a fragment info object for a given array, and fetches all
 * the fragment information for that array.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_fragment_info* fragment_info;
 * tiledb_fragment_info_alloc(ctx, "array_uri", &fragment_info);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] array_uri The array URI.
 * @param[out] fragment_info The fragment info object to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_alloc(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_fragment_info_t** fragment_info) TILEDB_NOEXCEPT;

/**
 * Frees a fragment info object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_fragment_info_free(&fragment_info);
 * @endcode
 *
 * @param[in] fragment_info The fragment info object to be freed.
 */
TILEDB_EXPORT void tiledb_fragment_info_free(
    tiledb_fragment_info_t** fragment_info) TILEDB_NOEXCEPT;

/**
 * Set the fragment info config. Useful for passing timestamp ranges and
 * encryption key via the config before loading the fragment info.
 *
 *  * **Example:**
 *
 * @code{.c}
 * tiledb_fragment_info* fragment_info;
 * tiledb_fragment_info_alloc(ctx, "array_uri", &fragment_info);
 *
 * tiledb_config_t* config;
 * tiledb_error_t* error = NULL;
 * tiledb_config_alloc(&config, &error);
 * tiledb_config_set(config, "sm.memory_budget", "1000000", &error);
 *
 * tiledb_fragment_info_load(ctx, fragment_info);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] config The config to be set.
 *
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_set_config(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    tiledb_config_t* config) TILEDB_NOEXCEPT;

/**
 * Retrieves the config from fragment info.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_config_t* config;
 * tiledb_fragment_info_get_config(ctx, vfs, &config);
 * // Make sure to free the retrieved config
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[out] config The config to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_config(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    tiledb_config_t** config) TILEDB_NOEXCEPT;

/**
 * Loads the fragment info.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_fragment_info_load(ctx, fragment_info);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_load(
    tiledb_ctx_t* ctx, tiledb_fragment_info_t* fragment_info) TILEDB_NOEXCEPT;

/**
 * Gets the name of a fragment.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* name;
 * tiledb_fragment_info_get_fragment_name_v2(ctx, fragment_info, 1, &name);
 * // Remember to free the string using tiledb_string_free when done with it.
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] name A pointer to a ::tiledb_string_t* that will hold the
 * fragment's name.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_fragment_name_v2(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_string_t** name) TILEDB_NOEXCEPT;

/**
 * Gets the number of fragments.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t fragment_num;
 * tiledb_fragment_info_get_fragment_num(ctx, fragment_info, &fragment_num);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[out] fragment_num The number of fragments to retrieve.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_fragment_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* fragment_num) TILEDB_NOEXCEPT;

/**
 * Gets a fragment URI.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* uri;
 * tiledb_fragment_info_get_fragment_uri(ctx, fragment_info, 1, &uri);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] uri The fragment URI to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_fragment_uri(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) TILEDB_NOEXCEPT;

/**
 * Gets the fragment size in bytes.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t size;
 * tiledb_fragment_info_get_fragment_size(ctx, fragment_info, 1, &size);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] size The fragment size to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_fragment_size(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* size) TILEDB_NOEXCEPT;

/**
 * Checks if a fragment is dense.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t dense;
 * tiledb_fragment_info_get_dense(ctx, fragment_info, 1, &dense);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] dense `1` if the fragment is dense.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_dense(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* dense) TILEDB_NOEXCEPT;

/**
 * Checks if a fragment is sparse.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t sparse;
 * tiledb_fragment_info_get_sparse(ctx, fragment_info, 1, &sparse);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] sparse `1` if the fragment is sparse.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_sparse(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* sparse) TILEDB_NOEXCEPT;

/**
 * Gets the timestamp range of a fragment.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start, end;
 * tiledb_fragment_info_get_timestamp_range(
 *     ctx, fragment_info, 1, &start, &end);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] start The start of the timestamp range to be retrieved.
 * @param[out] end The end of the timestamp range to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* start,
    uint64_t* end) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from a given fragment for a given
 * dimension index.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t domain[2];
 * tiledb_fragment_info_get_non_empty_domain_from_index(
 *     ctx, fragment_info, 0, 0, domain);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[in] did The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] domain The domain to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_fragment_info_get_non_empty_domain_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* domain) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from a given fragment for a given
 * dimension name.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t domain[2];
 * tiledb_fragment_info_get_non_empty_domain_from_name(
 *     ctx, fragment_info, 0, "d1", domain);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[in] dim_name The dimension name.
 * @param[out] domain The domain to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_non_empty_domain_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* domain) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain range sizes from a fragment for a given
 * dimension index. Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
 *     ctx, fragment_info, 0, &start_size, &end_size);
 * // If non-empty domain range is `[aa, dddd]`,
 * // then `start_size = 2`, and `end_size = 4`.
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment information object.
 * @param[in] fid The fragment index.
 * @param[in] did The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] start_size The size in bytes of the start range.
 * @param[out] end_size The size in bytes of the end range.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain range sizes from a fragment for a given
 * dimension name. Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
 *     ctx, fragment_info, "d", &start_size, &end_size);
 * // If non-empty domain range is `[aa, dddd]`,
 * // then `start_size = 2`, and `end_size = 4`.
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment information object.
 * @param[in] fid The fragment index.
 * @param[in] dim_name The dimension name.
 * @param[out] start_size The size in bytes of the start range.
 * @param[out] end_size The size in bytes of the end range.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from a fragment for a given
 * dimension index. Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 *
 * // Get range sizes first
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
 *     ctx, fragment_info, 0, 0, &start_size, &end_size);
 *
 * // Get domain
 * char start[start_size];
 * char end[end_size];
 * tiledb_fragment_info_get_non_empty_domain_var_from_index(
 *     ctx, fragment_info, 0, 0, start, end);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The fragment index.
 * @param[in] did The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] start The domain range start to set.
 * @param[out] end The domain range end to set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_fragment_info_get_non_empty_domain_var_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Retrieves the non-empty domain from a fragment for a given dimension name.
 * Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 *
 * // Get range sizes first
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
 *     ctx, fragment_info, 0, "d", &start_size, &end_size);
 *
 * // Get domain
 * char start[start_size];
 * char end[end_size];
 * tiledb_fragment_info_get_non_empty_domain_var_from_name(
 *     ctx, fragment_info, 0, "d", start, end);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The fragment index.
 * @param[in] dim_name The dimension name.
 * @param[out] start The domain range start to set.
 * @param[out] end The domain range end to set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_fragment_info_get_non_empty_domain_var_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of MBRs from the fragment.
 * In the case of sparse fragments, this is the number of physical tiles.
 * Dense fragments do not contain MBRs.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t mbr_num;
 * tiledb_fragment_info_get_mbr_num(ctx, fragment_info, 0, &mbr_num);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] mbr_num The number of MBRs to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_mbr_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* mbr_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the MBR from a given fragment for a given dimension index.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t mbr[2];
 * tiledb_fragment_info_get_mbr_from_index(ctx, fragment_info, 0, 0, 0, mbr);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[in] mid The mbr of the fragment of interest.
 * @param[in] did The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] mbr The mbr to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_mbr_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* mbr) TILEDB_NOEXCEPT;

/**
 * Retrieves the MBR from a given fragment for a given dimension name.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t mbr[2];
 * tiledb_fragment_info_get_mbr_from_name(ctx, fragment_info, 0, 0, "d1", mbr);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[in] mid The mbr of the fragment of interest.
 * @param[in] dim_name The dimension name.
 * @param[out] mbr The mbr to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_mbr_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* mbr) TILEDB_NOEXCEPT;

/**
 * Retrieves the MBR sizes from a fragment for a given dimension index.
 * Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_mbr_var_size_from_index(
 *     ctx, fragment_info, 0, 0, 0, &start_size, &end_size);
 * // If non-empty domain range is `[aa, dddd]`,
 * // then `start_size = 2`, and `end_size = 4`.
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment information object.
 * @param[in] fid The fragment index.
   @param[in] mid The mbr of the fragment of interest.
 * @param[in] did The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] start_size The size in bytes of the start range.
 * @param[out] end_size The size in bytes of the end range.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_mbr_var_size_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves the MBR range sizes from a fragment for a given dimension name.
 * Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_mbr_var_size_from_name(
 *     ctx, fragment_info, 0, 0, "d1", &start_size, &end_size);
 * // If non-empty domain range is `[aa, dddd]`,
 * // then `start_size = 2`, and `end_size = 4`.
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment information object.
 * @param[in] fid The fragment index.
 * @param[in] mid The mbr of the fragment of interest.
 * @param[in] dim_name The dimension name.
 * @param[out] start_size The size in bytes of the start range.
 * @param[out] end_size The size in bytes of the end range.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_mbr_var_size_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) TILEDB_NOEXCEPT;

/**
 * Retrieves the MBR from a fragment for a given dimension index.
 * Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 *
 * // Get range sizes first
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_mbr_var_size_from_index(
 *     ctx, fragment_info, 0, 0, 0, &start_size, &end_size);
 *
 * // Get domain
 * char start[start_size];
 * char end[end_size];
 * tiledb_fragment_info_get_mbr_var_from_index(
 *     ctx, fragment_info, 0, 0, 0, start, end);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The fragment index.
 * @param[in] mid The mbr of the fragment of interest.
 * @param[in] did The dimension index, following the order as it was defined
 *      in the domain of the array schema.
 * @param[out] start The domain range start to set.
 * @param[out] end The domain range end to set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_mbr_var_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Retrieves the MBR from a fragment for a given dimension name.
 * Applicable to var-sized dimensions.
 *
 * **Example:**
 *
 * @code{.c}
 *
 * // Get range sizes first
 * uint64_t start_size, end_size;
 * tiledb_fragment_info_get_mbr_var_size_from_name(
 *     ctx, fragment_info, 0, 0, "d1", &start_size, &end_size);
 *
 * // Get domain
 * char start[start_size];
 * char end[end_size];
 * tiledb_fragment_info_get_mbr_var_from_name(
 *     ctx, fragment_info, 0, 0, "d1", start, end);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The fragment index.
 * @param[in] mid The mbr of the fragment of interest.
 * @param[in] dim_name The dimension name.
 * @param[out] start The domain range start to set.
 * @param[out] end The domain range end to set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_mbr_var_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* start,
    void* end) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of cells written to the fragment by the user.
 *
 * In the case of sparse fragments, this is the number of non-empty
 * cells in the fragment.
 *
 * In the case of dense fragments, TileDB may add fill
 * values to populate partially populated tiles. Those fill values
 * are counted in the returned number of cells. In other words,
 * the cell number is derived from the number of *integral* tiles
 * written in the file.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t cell_num;
 * tiledb_fragment_info_get_cell_num(ctx, fragment_info, 0, &cell_num);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] cell_num The number of cells to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_cell_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* cell_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the format version of a fragment.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t version;
 * tiledb_fragment_info_get_version(ctx, fragment_info, 0, &version);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] version The format version to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_version(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t* version) TILEDB_NOEXCEPT;

/**
 * Checks if a fragment has consolidated metadata.
 *
 * **Example:**
 *
 * @code{.c}
 * int32_t has;
 * tiledb_fragment_info_has_consolidated_metadata(ctx, fragment_info, 0, &has);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] has True if the fragment has consolidated metadata.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_has_consolidated_metadata(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* has) TILEDB_NOEXCEPT;

/**
 * Gets the number of fragments with unconsolidated metadata.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t unconsolidated;
 * tiledb_fragment_info_get_unconsolidated_metadata_num(
 *      ctx, fragment_info, &unconsolidated);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[out] unconsolidated The number of fragments with unconsolidated
 * metadata.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t
tiledb_fragment_info_get_unconsolidated_metadata_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* unconsolidated) TILEDB_NOEXCEPT;

/**
 * Gets the number of fragments to vacuum.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t to_vacuum_num;
 * tiledb_fragment_info_get_to_vacuum_num(ctx, fragment_info, &to_vacuum_num);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[out] to_vacuum_num The number of fragments to vacuum.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_to_vacuum_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* to_vacuum_num) TILEDB_NOEXCEPT;

/**
 * Gets the URI of the fragment to vacuum with the given index.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* uri;
 * tiledb_fragment_info_get_to_vacuum_uri(ctx, fragment_info, 1, &uri);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment to vacuum of interest.
 * @param[out] uri The fragment URI to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_to_vacuum_uri(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) TILEDB_NOEXCEPT;

/**
 * Retrieves the array schema name a fragment.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_t* array_schema;
 * tiledb_fragment_info_get_array_schema(ctx, fragment_info, 0, &array_schema);
 * @endcode
 *
 * @param[in] ctx The TileDB context
 * @param[in] fragment_info The fragment info object.
 * @param[in] fid The index of the fragment of interest.
 * @param[out] array_schema The array schema to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_array_schema(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_array_schema_t** array_schema) TILEDB_NOEXCEPT;

/**
 * Get the fragment info schema name.
 *
 * **Example:**
 *
 * @code{.c}
 * char* name;
 * tiledb_fragment_info_schema_name(ctx, fragment_info, &schema_name);
 * @endcode
 *
 * @param[in]  ctx The TileDB context.
 * @param[in]  fragment_info The fragment info object.
 * @param[in]  fid The fragment info index.
 * @param[out] schema_name The schema name.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_get_array_schema_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** schema_name) TILEDB_NOEXCEPT;

/**
 * Dumps the fragment info in ASCII format in the selected output.
 *
 * **Example:**
 *
 * The following prints the fragment info dump in standard output.
 *
 * @code{.c}
 * tiledb_fragment_info_dump(ctx, fragment_info, stdout);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[out] out The output.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_dump(
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    FILE* out) TILEDB_NOEXCEPT;

/**
 * Dumps the fragment info in ASCII format in the selected string output.
 *
 * The output string handle must be freed by the user after use.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* tdb_string;
 * tiledb_fragment_info_dump_str(ctx, fragment_info, &tdb_string);
 * // Use the string
 * tiledb_string_free(&tdb_string);
 * @endcode
 *
 * @param[in] ctx The TileDB context.
 * @param[in] fragment_info The fragment info object.
 * @param[out] out The output string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_fragment_info_dump_str(
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    tiledb_string_t** out) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_FRAGMENT_INFO_EXTERNAL_H
