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

#ifndef TILEDB_CAPI_ATTRIBUTE_EXTERNAL_H
#define TILEDB_CAPI_ATTRIBUTE_EXTERNAL_H

#include "../api_external_common.h"
#include "../datatype/datatype_api_external.h"
#include "../filter_list/filter_list_api_external.h"
#include "../string/string_api_external.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Guard allows compilation prior to C11 when both this and the experimental
 * header are included. This is repetition rather than a separate header.
 */
#ifndef TILEDB_ATTRIBUTE_HANDLE_T_DEFINED
#define TILEDB_ATTRIBUTE_HANDLE_T_DEFINED
/** A TileDB attribute */
typedef struct tiledb_attribute_handle_t tiledb_attribute_t;
#endif

/**
 * Creates a TileDB attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param name The attribute name. Providing an empty string for the name
 * creates an anonymous attribute.
 * @param type The attribute type.
 * @param attr The TileDB attribute to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 *
 * @note The default number of values per cell is 1.
 */
TILEDB_EXPORT int32_t tiledb_attribute_alloc(
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    tiledb_attribute_t** attr) TILEDB_NOEXCEPT;

/**
 * Destroys a TileDB attribute, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * tiledb_attribute_free(&attr);
 * @endcode
 *
 * @param attr The attribute to be destroyed.
 */
TILEDB_EXPORT void tiledb_attribute_free(tiledb_attribute_t** attr)
    TILEDB_NOEXCEPT;

/**
 * Sets the nullability of an attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_set_nullable(ctx, attr, 1);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param nullable Non-zero if the attribute is nullable.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_set_nullable(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    uint8_t nullable) TILEDB_NOEXCEPT;

/**
 * Sets the filter list for an attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_filter_list_alloc(ctx, &filter_list);
 * tiledb_filter_list_add_filter(ctx, filter_list, filter);
 * tiledb_attribute_set_filter_list(ctx, attr, filter_list);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param filter_list The filter_list to be set.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_set_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_filter_list_t* filter_list) TILEDB_NOEXCEPT;

/**
 * Sets the number of values per cell for an attribute. If this is not
 * used, the default is `1`.
 *
 * **Examples:**
 *
 * For a fixed-sized attribute:
 *
 * @code{.c}
 * tiledb_attribute_set_cell_val_num(ctx, attr, 3);
 * @endcode
 *
 * For a variable-sized attribute:
 *
 * @code{.c}
 * tiledb_attribute_set_cell_val_num(ctx, attr, TILEDB_VAR_NUM);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param cell_val_num The number of values per cell.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_set_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    uint32_t cell_val_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the attribute name.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* attr_name;
 * tiledb_attribute_get_name(ctx, attr, &attr_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param name The name to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_name(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    const char** name) TILEDB_NOEXCEPT;

/**
 * Retrieves the attribute type.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_datatype_t attr_type;
 * tiledb_attribute_get_type(ctx, attr, &attr_type);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param type The type to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_datatype_t* type) TILEDB_NOEXCEPT;

/**
 * Retrieves the nullability of an attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * uint8_t nullable;
 * tiledb_attribute_get_nullable(ctx, attr, &nullable);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param nullable Output argument, non-zero for nullable and zero
 *    for non-nullable.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_nullable(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    uint8_t* nullable) TILEDB_NOEXCEPT;

/**
 * Retrieves the filter list for an attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_filter_list_t* filter_list;
 * tiledb_attribute_get_filter_list(ctx, attr, &filter_list);
 * tiledb_filter_list_free(&filter_list);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param filter_list The filter list to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_filter_list_t** filter_list) TILEDB_NOEXCEPT;

/**
 * Retrieves the number of values per cell for the attribute. For variable-sized
 * attributes result is TILEDB_VAR_NUM.
 *
 * **Example:**
 *
 * @code{.c}
 * uint32_t num;
 * tiledb_attribute_get_cell_val_num(ctx, attr, &num);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param cell_val_num The number of values per cell to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_cell_val_num(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    uint32_t* cell_val_num) TILEDB_NOEXCEPT;

/**
 * Retrieves the cell size for this attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * uint64_t cell_size;
 * tiledb_attribute_get_cell_size(ctx, attr, &cell_size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param cell_size The cell size to be retrieved.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_cell_size(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    uint64_t* cell_size) TILEDB_NOEXCEPT;

/**
 * Dumps the contents of an Attribute in ASCII form to the selected string
 * output.
 *
 * The output string handle must be freed by the user after use.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_string_t* tdb_string;
 * tiledb_attribute_dump_str(ctx, attr, &tdb_string);
 * // Use the string
 * tiledb_string_free(&tdb_string);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The attribute.
 * @param out The output string.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_dump_str(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_string_t** out) TILEDB_NOEXCEPT;

/**
 * Sets the default fill value for the input attribute. This value will
 * be used for the input attribute whenever querying (1) an empty cell in
 * a dense array, or (2) a non-empty cell (in either dense or sparse array)
 * when values on the input attribute are missing (e.g., if the user writes
 * a subset of the attributes in a write operation).
 *
 * Applicable to var-sized attributes.
 *
 * **Example:**
 *
 * @code{.c}
 * // Assumming a int32 attribute
 * int32_t value = 0;
 * uint64_t size = sizeof(value);
 * tiledb_attribute_set_fill_value(ctx, attr, &value, size);
 *
 * // Assumming a var char attribute
 * const char* value = "foo";
 * uint64_t size = strlen(value);
 * tiledb_attribute_set_fill_value(ctx, attr, value, size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param value The fill value to set.
 * @param size The fill value size in bytes.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note A call to `tiledb_attribute_cell_val_num` sets the fill value
 *     of the attribute to its default. Therefore, make sure you invoke
 *     `tiledb_attribute_set_fill_value` after deciding on the number
 *     of values this attribute will hold in each cell.
 *
 * @note For fixed-sized attributes, the input `size` should be equal
 *     to the cell size.
 */
TILEDB_EXPORT int32_t tiledb_attribute_set_fill_value(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    const void* value,
    uint64_t size) TILEDB_NOEXCEPT;

/**
 * Gets the default fill value for the input attribute. This value will
 * be used for the input attribute whenever querying (1) an empty cell in
 * a dense array, or (2) a non-empty cell (in either dense or sparse array)
 * when values on the input attribute are missing (e.g., if the user writes
 * a subset of the attributes in a write operation).
 *
 * Applicable to both fixed-sized and var-sized attributes.
 *
 * **Example:**
 *
 * @code{.c}
 * // Assuming a int32 attribute
 * const int32_t* value;
 * uint64_t size;
 * tiledb_attribute_get_fill_value(ctx, attr, &value, &size);
 *
 * // Assuming a var char attribute
 * const char* value;
 * uint64_t size;
 * tiledb_attribute_get_fill_value(ctx, attr, &value, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param value A pointer to the fill value to get.
 * @param size The size of the fill value to get.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_fill_value(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    const void** value,
    uint64_t* size) TILEDB_NOEXCEPT;

/**
 * Sets the default fill value for the input, nullable attribute. This value
 * will be used for the input attribute whenever querying (1) an empty cell in
 * a dense array, or (2) a non-empty cell (in either dense or sparse array)
 * when values on the input attribute are missing (e.g., if the user writes
 * a subset of the attributes in a write operation).
 *
 * Applicable to var-sized attributes.
 *
 * **Example:**
 *
 * @code{.c}
 * // Assumming a int32 attribute
 * int32_t value = 0;
 * uint64_t size = sizeof(value);
 * uint8_t valid = 0;
 * tiledb_attribute_set_fill_value_nullable(ctx, attr, &value, size, valid);
 *
 * // Assumming a var char attribute
 * const char* value = "foo";
 * uint64_t size = strlen(value);
 * uint8_t valid = 1;
 * tiledb_attribute_set_fill_value_nullable(ctx, attr, value, size, valid);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param value The fill value to set.
 * @param size The fill value size in bytes.
 * @param validity The validity fill value, zero for a null value and
 *     non-zero for a valid attribute.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note A call to `tiledb_attribute_cell_val_num` sets the fill value
 *     of the attribute to its default. Therefore, make sure you invoke
 *     `tiledb_attribute_set_fill_value_nullable` after deciding on the
 *     number of values this attribute will hold in each cell.
 *
 * @note For fixed-sized attributes, the input `size` should be equal
 *     to the cell size.
 */
TILEDB_EXPORT int32_t tiledb_attribute_set_fill_value_nullable(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    const void* value,
    uint64_t size,
    uint8_t validity) TILEDB_NOEXCEPT;

/**
 * Gets the default fill value for the input, nullable attribute. This value
 * will be used for the input attribute whenever querying (1) an empty cell in
 * a dense array, or (2) a non-empty cell (in either dense or sparse array)
 * when values on the input attribute are missing (e.g., if the user writes
 * a subset of the attributes in a write operation).
 *
 * Applicable to both fixed-sized and var-sized attributes.
 *
 * **Example:**
 *
 * @code{.c}
 * // Assuming a int32 attribute
 * const int32_t* value;
 * uint64_t size;
 * uint8_t valid;
 * tiledb_attribute_get_fill_value_nullable(ctx, attr, &value, &size, &valid);
 *
 * // Assuming a var char attribute
 * const char* value;
 * uint64_t size;
 * uint8_t valid;
 * tiledb_attribute_get_fill_value_nullable(ctx, attr, &value, &size, &valid);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param attr The target attribute.
 * @param value A pointer to the fill value to get.
 * @param size The size of the fill value to get.
 * @param valid The fill value validity to get.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_attribute_get_fill_value_nullable(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    const void** value,
    uint64_t* size,
    uint8_t* valid) TILEDB_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_ATTRIBUTE_EXTERNAL_H
