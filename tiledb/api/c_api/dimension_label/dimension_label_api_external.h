/**
 * @file tiledb/api/c_api/dimension_label/dimension_label_api_external.h
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
 * This file declares the C API for a dimension label in TileDB
 */

#include "tiledb/api/c_api/api_external_common.h"
#include "tiledb/api/c_api/context/context_api_external.h"
#include "tiledb/api/c_api/data_order/data_order_api_external.h"
#include "tiledb/api/c_api/datatype/datatype_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * C API carrier for a TileDB dimensin label
 */
typedef struct tiledb_dimension_label_handle_t tiledb_dimension_label_t;

/**
 * Destroys a TileDB dimension label, freeing associated memory.
 *
 * This function must be called on every dimension label returned from the API.
 *
 * ** Example:**
 *
 * @code{.c}
 * tiledb_dimension_label_t* dim_label;
 * tiledb_array_schema_get_dimension_label_from_name(
 *       ctx, array_schema, "label1", &dimension_label);
 * tiledb_dimension_label_free(&dimension_label);
 * @endcode
 *
 * @param[in,out] dim_label The dimension label to be destroyed.
 */
TILEDB_EXPORT void tiledb_dimension_label_free(
    tiledb_dimension_label_t** dim_label) TILEDB_NOEXCEPT;

/**
 * Returns the index of the dimension the dimension label provides labels for.
 *
 * @param[in] ctx TileDB context.
 * @param[in] dim_label The target dimension label.
 * @param[out] dim_index The index of the dimension in the array schema.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dimension_label_get_dimension_index(
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    uint32_t* dim_index) TILEDB_NOEXCEPT;

/**
 * Returns the name of the dimension label.
 *
 * @param[in] ctx TileDB context.
 * @param[in] dim_label The target dimension label.
 * @param[out] label_attr_name The name of the attribute the label data is
 *     stored under.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dimension_label_get_label_attr_name(
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    const char** label_attr_name) TILEDB_NOEXCEPT;

/**
 * Returns the number of values per cell for the labels on the dimension label.
 * For variable-sized labels the result is TILEDB_VAR_NUM.
 *
 * @param[in] ctx TileDB context.
 * @param[in] dim_label The target dimension label.
 * @param[out] label_cell_val_num The number of values per label cell.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dimension_label_get_label_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    uint32_t* label_cell_val_num) TILEDB_NOEXCEPT;

/**
 * Returns the order of the labels on the dimension label.
 *
 * @param[in] ctx TileDB context.
 * @param[in] dim_label The target dimension label.
 * @param[out]
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dimension_label_get_label_order(
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    tiledb_data_order_t* label_order) TILEDB_NOEXCEPT;

/**
 * Returns the type of the labels on the dimension label.
 *
 * @param[in] ctx TileDB context.
 * @param[in] dim_label The target dimension label.
 * @param[out] label_type The type of the labels.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dimension_label_get_label_type(
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    tiledb_datatype_t* label_type) TILEDB_NOEXCEPT;

/**
 * Returns the name of the dimension label.
 *
 * @param[in] ctx TileDB context.
 * @param[in] dim_label The target dimension label.
 * @param[out] name The name of the dimension label.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dimension_label_get_name(
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    const char** name) TILEDB_NOEXCEPT;

/**
 * Returns the URI of the dimension label array.
 *
 * @param[in] ctx TileDB context.
 * @param[in] dim_label The target dimension label.
 * @param[out] uri The uri of the dimension label.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT capi_return_t tiledb_dimension_label_get_uri(
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    const char** uri) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif
