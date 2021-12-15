/**
 * @file   tiledb_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file declares experimental C API for TileDB.
 * Experimental APIs to do not fall under the API compatibility guarantees and
 * might change between TileDB versions
 */

#ifndef TILEDB_EXPERIMENTAL_H
#define TILEDB_EXPERIMENTAL_H

#include "tiledb.h"

/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB array schema. */
typedef struct tiledb_array_schema_evolution_t tiledb_array_schema_evolution_t;

/* ********************************* */
/*      ARRAY SCHEMA EVOLUTION       */
/* ********************************* */

/**
 * Creates a TileDB schema evolution object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_t* array_schema_evolution;
 * tiledb_array_schema_evolution_alloc(ctx, &array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The TileDB schema evolution to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Destroys an array schema evolution, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_free(&array_schema_evolution);
 * @endcode
 *
 * @param array_schema_evolution The array schema evolution to be destroyed.
 */
TILEDB_EXPORT void tiledb_array_schema_evolution_free(
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Adds an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * tiledb_array_schema_evolution_add_attribute(ctx, array_schema_evolution,
 * attr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attr The attribute to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attribute);

/**
 * Drops an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* attribute_name="a1";
 * tiledb_array_schema_evolution_drop_attribute(ctx, array_schema_evolution,
 * attribute_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attribute_name The name of the attribute to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_drop_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/**
 * Evolve array schema of an array.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_evolve(ctx, array_uri,array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param array_schema_evolution The schema evolution.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_evolve(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution);

/**
 * Upgrades an array to the latest format version.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_upgrade_version(ctx, array_uri);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param config Configuration parameters for the upgrade
 *     (`nullptr` means default, which will use the config from `ctx`).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_upgrade_version(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config);

/* ********************************* */
/*        SUBARRAY PARTITIONER       */
/* ********************************* */

/** A subarray partitioner object. */
typedef struct tiledb_subarray_partitioner_t tiledb_subarray_partitioner_t;

/**
 * Allocates a TileDB subarray partitioner object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_partitioner_t* subarray_partitioner;
 * tiledb_subarray_alloc(ctx, array, &subarray_partitioner);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array An open array object.
 * @param subarray The subarray_partitioner object to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_alloc(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    tiledb_subarray_partitioner_t** subarray_partitioner,
    uint64_t memory_budget,
    uint64_t memory_budget_var,
    uint64_t memory_budget_validity);

/**
 * Frees a TileDB subarray partitioner object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_t* subarray;
 * tiledb_subarray_partitioner_t *subarray_partitioner;
 * tiledb_subarray_partitioner_alloc(ctx, subarray, &subarray_partitioner);
 * tiledb_subarray_partitioner_free(&subarray_partitioner);
 * @endcode
 *
 * @param subarray_partitioner The subarray partitioner object to be freed.
 */
TILEDB_EXPORT void tiledb_subarray_partitioner_free(
    tiledb_subarray_partitioner_t** subarray_partitioner);

/**
 * Set the layout of the subarray associated with a subarray partitioner object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_t* subarray;
 * tiledb_subarray_partitioner_t *subarray_partitioner;
 * tiledb_subarray_partitioner_set_layout(ctx, layout, subarray_partitioner);
 * @endcode
 *
 * @param layout The layout to set into the subarray partioner object's
 * subarray.
 *    - `TILEDB_COL_MAJOR`:
 *      This means column-major order with respect to the subarray.
 *    - `TILEDB_ROW_MAJOR`:
 *      This means row-major order with respect to the subarray.
 *    - `TILEDB_GLOBAL_ORDER`:
 *      This means that cells are stored or retrieved in the array global
 *      cell order.
 *    - `TILEDB_UNORDERED`:
 *      This is applicable only to reads and writes for sparse arrays, or for
 *      sparse writes to dense arrays. For writes, it specifies that the cells
 *      are unordered and, hence, TileDB must sort the cells in the global cell
 *      order prior to writing. For reads, TileDB will return the cells without
 *      any particular order, which will often lead to better performance.
 * * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_set_layout(
    tiledb_ctx_t* ctx,
    tiledb_layout_t layout,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Compute the complete series of partitions/subarrays leaving them stored
 * internally in the object.
 *
 * These partitions can then be retrieved via
 * tiledb_subarray_partitioner_get_partition().
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_partitioner_t *subarray_partitioner;
 * tiledb_subarray_partitioner_compute_partitions(ctx, subarray_partitioner);
 * @endcode
 *
 * @param The partitioner for which to compute the complete series of (sub)
 * partitions
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_compute_partitions(
    tiledb_ctx_t* ctx, tiledb_subarray_partitioner_t* partitioner);

/**
 * Gets number of computed partitions available to be retrieved via
 * tiledb_subarray_partitioner_get_partition().
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_partitioner_t *subarray_partitioner;
 * tiledb_subarray_partitioner_compute(ctx, subarray_partitioner);
 * @endcode
 *
 * @param layout The layout to set into the subarray partioner object's
 * subarray.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_get_partitions_num(
    tiledb_ctx_t* ctx,
    uint64_t* num,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Retrieve the part-id(th) partition subarray from the partitioner's computed
 * subarray partitions.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_partitioner_t *subarray_partitioner;
 * uint64_t partition_id; //# of partition to retrieve, must be less than #
 * computed
 * partitions tiledb_subarray_t *retrieved_subarray;
 * tiledb_subarray_partitioner_get_partition(ctx, subarray_partitioner,
 * partition_id, retrieved_subarray);
 * @endcode
 *
 * @param part_id The index of a partition to retrieve from a prior
 * tiledb_subarray_partitioner_compute(), must be less than the number
 * of computed partitions.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 *
 * @note Any returned subarray object was allocated within this call and is to
 * be freed by the caller.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_get_partition(
    tiledb_ctx_t* ctx,
    tiledb_subarray_partitioner_t* partitioner,
    uint64_t partition_id,
    tiledb_subarray_t* subarray);

/**
 * Set memory partitioning budget (in bytes) for fixed size attribute/dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_ctx_t *ctx;
 * char* attrname;
 * uint64_t budget;
 * tiledb_subarray_partitioner_set_result_budget(ctx, attrname, budget,
 * partitioner);
 * @endcode
 *
 * @param budget - byte count specifying memory budget for the specified
 * attribute.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_set_result_budget(
    tiledb_ctx_t* ctx,
    const char* attrname,
    uint64_t budget,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Gets result size budget (in bytes) for the input fixed-sized
 * attribute/dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget;
 * tiledb_subarray_partitioner_get_result_budget_fixed(ctx, attrname, &budget,
 * partitioner);
 * @endcode
 *
 * @param budget - byte count memory budget for the specified attribute.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_get_result_budget_fixed(
    tiledb_ctx_t* ctx,
    const char* attrname,
    uint64_t* budget,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Gets result size budget (in bytes) for the input var-sized
 * attribute/dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget_off;
 * uint64_t budget_val;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_get_result_budget_var(ctx, attrname, &budget_off,
 * &budget_val, partitioner);
 * @endcode
 *
 * @param budget_off - Receive the budget in bytes for offsets of var attributes
 * @param budget_val - Receive the budget in bytes for var sized attribute values
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_get_result_budget_var(
    tiledb_ctx_t* ctx,
    const char* name,
    uint64_t* budget_off,
    uint64_t* budget_val,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Sets result size budget (in bytes) for the input fixed-sized
 * nullable attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget;
 * uint64_t budget_validity;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_set_result_budget_nullable(ctx, attrname, budget,
 * budget_validity, partitioner);
 * @endcode
 *
 * @param budget The budget in bytes for the (fixed size) nullable attribute's
 * values
 * @param budget_validity The budget in bytes for validity vectors for the attr
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t
tiledb_subarray_partitioner_set_result_budget_nullable_fixed(
    tiledb_ctx_t* ctx,
    const char* name,
    uint64_t budget,
    uint64_t budget_validity,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Sets result size budget (in bytes) for the input var-sized
 * nullable attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget_off;
 * uint64_t budget_val;
 * uint64_t budget_validity;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_set_result_budget_nullable(ctx, attrname,
 * budget_off, budget_val, budget_validity, partitioner);
 * @endcode
 *
 * @param budget_off - The budget in bytes for offsets of var attributes
 * @param budget_val - The budget in bytes for var sized attribute values
 * @param budget_validity The budget in bytes for validity vectors.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t
tiledb_subarray_partitioner_set_result_budget_nullable_var(
    tiledb_ctx_t* ctx,
    const char* name,
    uint64_t budget_off,
    uint64_t budget_val,
    uint64_t budget_validity,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Gets result size budget (in bytes) for the input fixed-sized
 * nullable attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget;
 * uint64_t budget_validity;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_get_result_budget_nullable(ctx, attrname,
 * &budget,
 * &budget_validity, partitioner);
 * @endcode
 *
 * @param budget The budget in bytes for the (fixed size) nullable attribute's
 * values
 * @param budget_validity The budget in bytes for validity vectors for the attr
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t
tiledb_subarray_partitioner_get_result_budget_nullable_fixed(
    tiledb_ctx_t* ctx,
    const char* name,
    uint64_t* budget,
    uint64_t* budget_validity,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Gets result size budget (in bytes) for the input var-sized
 * nullable attribute.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget_off;
 * uint64_t budget_val;
 * uint64_t budget_validity;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_get_result_budget_nullable(ctx, attrname,
 * &budget_off, &budget_val, &budget_validity, partitioner);
 * @endcode
 *
 * @param budget_off - Receives the budget in bytes for offsets of var attributes
 * @param budget_val - Receives the budget in bytes for var sized attribute values
 * @param budget_validity Receives the budget in bytes for validity vectors.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t
tiledb_subarray_partitioner_get_result_budget_nullable_var(
    tiledb_ctx_t* ctx,
    const char* name,
    uint64_t* budget_off,
    uint64_t* budget_val,
    uint64_t* budget_validity,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Get partitioning budget for fixed sized attribute.
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_get_result_budget_fixed(ctx, attrname, &budget,
 * partitioner);
 * @endcode
 *
 * @param budget - Receives the byte count memory budget for the specified attribute.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_get_result_budget_fixed(
    tiledb_ctx_t* ctx,
    const char* attrname,
    uint64_t* budget,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Sets result size budget (in bytes) for the input var-sized
 * attribute/dimension.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * char attrname[] = "name_of_attr";
 * uint64_t budget_off;
 * uint64_t budget_val;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_set_result_budget_var(ctx, attrname, budget_off,
 * budget_val, partitioner);
 * @endcode
 *
 * @param budget_off The budget in bytes for offsets
 * @param budget_val The budget for validity vectors
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_set_result_budget_var_attr(
    tiledb_ctx_t* ctx,
    const char* attrname,
    uint64_t budget_off,
    uint64_t budget_val,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Set partitioning memory budget values (in bytes).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_ctx_t *ctx;
 * uint64_t budget;
 * uint64_t budget_var;
 * uint64_t budget_validity;
 * tiledb_subarray_partitioner_set_memory_budget
 *     (ctx, budget, budget_var, budget_validity, partitioner);
 * @endcode
 *
 * @param budget The budget for the fixed-sized attributes and the
 *     offsets of the var-sized attributes.
 * @param budget_var The budget for the var-sized attributes.
 * @param budget_validity The budget for validity vectors.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_set_memory_budget(
    tiledb_ctx_t* ctx,
    uint64_t budget,
    uint64_t budget_var,
    uint64_t budget_validity,
    tiledb_subarray_partitioner_t* partitioner);

/**
 * Get partitioning memory budget values (in bytes).
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_ctx_t *ctx;
 * uint64_t budget;
 * uint64_t budget_var;
 * uint64_t budget_validity;
 * tiledb_subarray_partitioner_t *partitioner;
 * tiledb_subarray_partitioner_get_memory_budget
 *     (ctx, &budget, &budget_var, &budget_validity, partitioner);
 * @endcode
 *
 * @param budget Receives the budget for the fixed-sized attributes and the
 *     offsets of the var-sized attributes.
 * @param budget_var Receives the budget for the var-sized attributes.
 * @param budget_validity Receives the budget for validity vectors.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_subarray_partitioner_get_memory_budget(
    tiledb_ctx_t* ctx,
    uint64_t* budget,
    uint64_t* budget_var,
    uint64_t* budget_validity,
    tiledb_subarray_partitioner_t* partitioner);

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_EXPERIMENTAL_H
