/**
 * @file   tiledb.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file declares the C APIs for TileDB. These APIs are exposed in library
 * **libtiledb.so**. 
 */

#ifndef __TILEDB_H__
#define __TILEDB_H__

#include <stdint.h>
#include <stddef.h>

/** The TileDB version */
#define TILEDB_VERSION "TileDB v0.1"

#ifdef __cplusplus
extern "C" {
#endif

#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#  define TILEDB_EXPORT __attribute__((visibility("default")))
#else
#  define TILEDB_EXPORT
#endif

/* ********************************* */
/*              CONTEXT              */
/* ********************************* */

/** 
 * Constitutes the TileDB context, which maintains some state for all used
 * TileDB modules. 
 */
typedef struct TileDB_CTX TileDB_CTX; 

/** 
 * Finalizes the TileDB context. 
 * @param tiledb_ctx The TileDB context to be finalized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_CTX, tiledb_ctx_init
 */
TILEDB_EXPORT int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx);

/** 
 * Initializes the TileDB context.  
 * @param tiledb_ctx The TileDB context initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_CTX, tiledb_ctx_finalize
 */
TILEDB_EXPORT int tiledb_ctx_init(TileDB_CTX*& tiledb_ctx);

/* ********************************* */
/*                I/O                */
/* ********************************* */

/** 
 * Closes an array, cleaning its metadata from main memory.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array to be closed
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_open
 */
TILEDB_EXPORT int tiledb_array_close(
    TileDB_CTX* tiledb_ctx,
    int ad);

/** 
 * Prepares an array for reading or writing. It returns an **array descriptor**,
 * which is used in subsequent array operations. 
 * @param tiledb_ctx The TileDB state.
 * @param workspace The path to the workspace directory where the array is 
 * stored. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array is 
 * stored. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array to be opened.
 * @param mode The mode in which is the array is opened. Currently, the 
 * following  modes are supported:
 * - **r**: Read mode
 * - **w**: Write mode (if the array exists, its data are cleared)
 * - **a**: Append mode (used when updating the array)
 * - **c**: Consolidate mode (used for array consolidation)
 * @return An array descriptor (>=0) or <b>-1</b> for error. 
 * @see TileDB_CTX, tiledb_array_close
 */
TILEDB_EXPORT int tiledb_array_open(
    TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* mode);

/** 
 * Writes a binary cell to an array. The format of the cell is discussed in
 * tiledb_array_load().
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array to receive the cell.
 * @param cell The binary cell to be written.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_CTX, tiledb_cell_write_sorted
 */
TILEDB_EXPORT int tiledb_cell_write(
    TileDB_CTX* tiledb_ctx,
    int ad, 
    const void* cell);

/** 
 * Writes a binary cell to an array. The format of the cell is discussed in
 * tiledb_array_load(). The difference to tildb_cell_write() is that the 
 * cells are assumed to be written in the same order as the global cell order
 * of the array. Therefore, this is a simple **append** command,
 * whereas tiledb_cell_write() triggers **sorting** at some point.
 *
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array to receive the cell.
 * @param cell The binary cell to be written.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_CTX, tiledb_cell_write
 */
TILEDB_EXPORT int tiledb_cell_write_sorted(
    TileDB_CTX* tiledb_ctx,
    int ad, 
    const void* cell);

/** 
 * This is very similar to tiledb_subarray(). The difference is that the result
 * cells are written in the provided buffer, serialized one after the other.
 * See tiledb_array_load() for more information on the binary cell format.
 * The function fails if the provided buffer size is not sufficient to hold
 * all the cells, and the buffer size is set to -1.
 *
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array where the subarray is applied.
 * @param range The range of the subarray. It must contain real values.
 * @param range_size The nunber of elements of the range vector. It must be
 * equal to 2*dim_num, where *dim_num* is the number of the dimensions of the
 * array.
 * @param dim_names An array holding the names of the dimensions whose 
 * coordinates will appear in the result cells. If it is NULL, then *all* the 
 * coordinates will appear in the result cells. If it contains special 
 * name <b>"__hide"</b>, then *no* coordinates will appear.
 * @param dim_names_num The number of elements in *dim_names*. 
 * @param attribute_names An array holding the names of the attribute whose
 * values will be included in the result cells. If it is NULL, then *all* the 
 * attributes of the input array will appear in the result cells. If there is 
 * only one special attribute name "__hide", then *no* attribute value will be 
 * included in the result cells.
 * @param attribute_names_num The number of elements in attribute_names.
 * @param buffer The buffer where the result cells are written.
 * @param buffer_size The size of the input buffer. If the function succeeds,
 * it is set to the actual size occupied by the result cells. If the function
 * fails because the size of the result cells exceeds the buffer size, it is set
 * to -1. 
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_CTX, tiledb_subarray, tiledb_array_load
 */
TILEDB_EXPORT int tiledb_subarray_buf( 
    const TileDB_CTX* tiledb_ctx, 
    int ad, 
    const double* range,
    int range_size,
    const char** dim_names,
    int dim_names_num,
    const char** attribute_names,
    int attribute_names_num,
    void* buffer,
    size_t* buffer_size);

/* ********************************* */
/*           CELL ITERATORS          */
/* ********************************* */

// DEFINITIONS

/** A constant cell iterator. */
typedef struct TileDB_ConstCellIterator TileDB_ConstCellIterator;

/** A constant reverse cell iterator. */
typedef struct TileDB_ConstReverseCellIterator TileDB_ConstReverseCellIterator;

/** 
 * A constant cell iterator that simulates a dense array. 
 * Specifically, this iterator will return "zero" cells even for the empty
 * cells (that are not explicitly stored in a sparse array). In a zero cell,
 * all attributes have special NULL values.
 */
typedef struct TileDB_ConstDenseCellIterator TileDB_ConstDenseCellIterator;

/** 
 * A constant reverse cell iterator that simulates a dense array. 
 * Specifically, this iterator will return "zero" cells even for the empty
 * cells (that are not explicitly stored in a sparse array). In a zero cell,
 * all attributes have special NULL values.
 */
typedef struct TileDB_ConstReverseDenseCellIterator 
    TileDB_ConstReverseDenseCellIterator;

// FUNCTIONS

/**
 * Finalizes a constant cell iterator, clearing its state.
 * @param cell_it The constant cell iterator.
 * @return **0** for success and <b>-1</b> for error.
 * @see tiledb_const_cell_iterator_init, tiledb_const_cell_iterator_next
 */
TILEDB_EXPORT int tiledb_const_cell_iterator_finalize(
    TileDB_ConstCellIterator* cell_it);

/**
 * Finalizes a constant dense cell iterator, clearing its state.
 * @param cell_it The constant dense cell iterator.
 * @return **0** for success and <b>-1</b> for error.
 * @see tiledb_const_dense_cell_iterator_init,
 * tiledb_const_dense_cell_iterator_next
 */
TILEDB_EXPORT int tiledb_const_dense_cell_iterator_finalize(
    TileDB_ConstDenseCellIterator* cell_it);

/**
 * Finalizes a constant reverse cell iterator, clearing its state.
 * @param cell_it The constant reverse cell iterator.
 * @return **0** for success and <b>-1</b> for error.
 * @see tiledb_const_revrse_cell_iterator_init, 
 * tiledb_const_reverse_cell_iterator_next
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_finalize(
    TileDB_ConstReverseCellIterator* cell_it);

/**
 * Finalizes a constant reverse dense cell iterator, clearing its state.
 * @param cell_it The constant reverse dense cell iterator.
 * @return **0** for success and <b>-1</b> for error.
 * @see tiledb_const_reverse_dense_cell_iterator_init, 
 * tiledb_const_reverse_dense_cell_iterator_next
 */
TILEDB_EXPORT int tiledb_const_reverse_dense_cell_iterator_finalize(
    TileDB_ConstReverseDenseCellIterator* cell_it);

/** 
 * Initializes a constant cell iterator
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized.
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially implements projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument. If it is NULL, then **all** the attributes will be included.
 * @param attribute_names_num The length of *attribute_names*. If it is zero, 
 * then **all** attributes are used.
 * @param cell_it The cell iterator that is initialized
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_cell_iterator_in_range_init,
 * tiledb_const_cell_iterator_next,
 * tiledb_const_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstCellIterator*& cell_it);

/** 
 * Initializes a constant cell iterator, but constrains it inside a particular
 * subspace oriented by the input range. 
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized.
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially implements projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of *attribute_names*. If it zero, then
 * **all** attributes are used.
 * @param range The subspace in which the iterator is constrained. The number
 * of elements should be 2*dim_num, where dim_num is the number of dimensions.
 * This is derived from the array schema inside the function. The underlying
 * type must be the same as that of the coordinates.
 * @param cell_it The constant cell iterator that is initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_cell_iterator_init, 
 * tiledb_const_cell_iterator_next, tiledb_const_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstCellIterator*& cell_it);

/** 
 * Initializes a constant reverse cell iterator
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized.
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of *attribute_names*. If it zero, then
 * **all** attributes are used.
 * @param cell_it The constant reverse cell iterator that is initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_reverse_cell_iterator_in_range_init,
 * tiledb_const_reverse_cell_iterator_next,
 * tiledb_const_reverse_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstReverseCellIterator*& cell_it);

/** 
 * Initializes a constant reverse cell iterator, but constrains it inside a 
 * particular subspace oriented by the input range.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of *attribute_names*. If it zero, then
 * **all** attributes are used.
 * @param range The subspace in which the iterator is constrained. The number
 * of elements should be 2*dim_num, where dim_num is the number of dimensions.
 * This is derived from the array schema inside the function. The underlying 
 * type must be the same as that of the coordinates.
 * @param cell_it The constant reverse cell iterator that is initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tilebd_const_reverse_cell_iterator_init,
 * tiledb_const_reverse_cell_iterator_next,
 * tiledb_const_reverse_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstReverseCellIterator*& cell_it);

/** 
 * Initializes a constant dense cell iterator.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of *attribute_names*. If it zero, then
 * **all** attributes are used.
 * @param cell_it The constant dense cell iterator that is initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_dense_cell_iterator_in_range_init,
 * tiledb_const_dense_cell_iterator_next,
 * tiledb_const_dense_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_dense_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstDenseCellIterator*& cell_it);

/** 
 * Initializes a constant dense cell iterator, but constrains it inside a 
 * particular subspace oriented by the input range.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of *attribute_names*. If it zero, then
 * **all** attributes are used.
 * @param range The subspace in which the iterator is constrained. The number
 * of elements should be 2*dim_num, where dim_num is the number of dimensions.
 * This is derived from the array schema inside the function. The underlying 
 * type must be the same as that of the coordinates.
 * @param cell_it The constant dense cell iterator that is initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_dense_cell_iterator_init,
 * tiledb_const_dense_cell_iterator_next,
 * tiledb_const_dense_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_dense_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstDenseCellIterator*& cell_it);

/** 
 * Initializes a constant reverse dense cell iterator.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of *attribute_names*. If it zero, then
 * **all** attributes are used.
 * @param cell_it The constant reverse dense cell iterator that is initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_reverse_dense_cell_iterator_in_range_init,
 * tiledb_const_reverse_dense_cell_iterator_next,
 * tiledb_const_reverse_dense_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_dense_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstReverseDenseCellIterator*& cell_it);

/** 
 * Initializes a constant reverse dense cell iterator, but constrains it inside 
 * a particular subspace oriented by the input range.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of *attribute_names*. If it zero, then
 * **all** attributes are used.
 * @param range The subspace in which the iterator is constrained. The number
 * of elements should be 2*dim_num, where dim_num is the number of dimenaions. 
 * This is derived from the array schema inside the function. The underlying 
 * type must be the same as that of the coordinates.
 * @param cell_it The constant reverse dense cell iterator that is initialized.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_reverse_dense_cell_iterator_init,
 * tiledb_const_reverse_dense_cell_iterator_next,
 * tiledb_const_reverse__dense_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_dense_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstReverseDenseCellIterator*& cell_it);

/** 
 * Retrieves the next cell. If the iterator has reached its end, then the 
 * the returned cell is NULL. The format of a binary cell is the same as
 * that described in tiledb_array_load(). If the iterator was initialized
 * with a subset of attributes, then only those attributes are included
 * in the binary cell. Finally, if the iterator was initialized in a
 * range, then the iterator will always retrieve the next cell along the
 * global order *in that specified range*.
 * @param cell_it The constant cell iterator.
 * @param cell The returned cell. It is NULL if the iterator has reached its
 * end. 
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_cell_iterator_init,
 * tiledb_const_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_cell_iterator_next(
    TileDB_ConstCellIterator* cell_it,
    const void*& cell);

/** 
 * Retrieves the next cell. If the iterator has reached its end, then the 
 * the returned cell is NULL. The format of a binary cell is the same as
 * that described in tiledb_load_bin(). If the iterator was initialized
 * with a subset of attributes, then only those attributes are included
 * in the binary cell. Finally, if the iterator was initialized in a
 * range, then the iterator will always retrieve the next cell along the
 * global order *in that specified range*.
 * @param cell_it The constant reverse cell iterator.
 * @param cell The returned cell. It is NULL if the iterator has reached its
 * end. 
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_reverse_cell_iterator_init,
 * tiledb_const_reverse_cell_iterator_in_range_init,
 * tiledb_const_reverse_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_next(
    TileDB_ConstReverseCellIterator* cell_it,
    const void*& cell);

/** 
 * Retrieves the next cell. If the iterator has reached its end, then the 
 * the returned cell is NULL. The format of a binary cell is the same as
 * that described in tiledb_load_bin(). If the iterator was initialized
 * with a subset of attributes, then only those attributes are included
 * in the binary cell. Finally, if the iterator was initialized in a
 * range, then the iterator will always retrieve the next cell along the
 * global order *in that specified range*.
 * @param cell_it The constant dense cell iterator.
 * @param cell The returned cell. It is NULL if the iterator has reached its
 * end. 
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_dense_cell_iterator_init,
 * tiledb_const_dense_cell_iterator_in_range_init,
 * tiledb_const_dense_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_dense_cell_iterator_next(
    TileDB_ConstDenseCellIterator* cell_it,
    const void*& cell);

/** 
 * Retrieves the next cell. If the iterator has reached its end, then the 
 * the returned cell is NULL. The format of a binary cell is the same as
 * that described in tiledb_load_bin(). If the iterator was initialized
 * with a subset of attributes, then only those attributes are included
 * in the binary cell. Finally, if the iterator was initialized in a
 * range, then the iterator will always retrieve the next cell along the
 * global order *in that specified range*.
 * @param cell_it The constant reverse dense cell iterator.
 * @param cell The returned cell. It is NULL if the iterator has reached its
 * end. 
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_const_reverse_dense_iterator_init,
 * tiledb_const_reverse_dense_iterator_in_range_init,
 * tiledb_const_reverse_dense_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_dense_cell_iterator_next(
    TileDB_ConstReverseDenseCellIterator* cell_it,
    const void*& cell);

/* ********************************* */
/*              QUERIES              */
/* ********************************* */

/** 
 * Clears all data from an array. However, the array remains defined, i.e.,
 * one can still invoke tiledb_array_load() for this array, without having
 * to redefine the array schema.
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace folder where the array is 
 * stored. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array 
 * is stored. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array to be cleared.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_delete
 */
TILEDB_EXPORT int tiledb_array_clear(    
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_name);

/** 
 * Consolidates the fragments of an array, using the technique described in
 * detail in tiledb_array_define(). 
 *
 * --- **Directory name conventions for fragments** --- \n 
 *
 * When a fragment is created, either via tiledb_array_load() or
 * tiledb_array_update(), it is given a temporary name of the form
 * **__pid_timestamp**, where "pid" is id of the process that created it
 * and "timestamp" is the time of creation in milliseconds. During 
 * consolidation, the first thing that happens is changing these temporary names
 * to the form **0_y+1**, **0_y+2**, etc., where "y" here is the *largest*
 * number appearing in older fragments of the form "0_y". For instance, if 
 * before consolidation there were existing fragments with names "0_1" and "0_2"
 * (and no other fragment name starting with "0_"), then the temporary names 
 * would be changed to "0_3", "0_4", etc.
 *
 * One important invariant of the consolidation is that, after it occurs,
 * the fragment names represent nodes in the merge tree explained in 
 * tiledb_array_define(). Specifically, they are in the form
 * **x_y** where "x" indicates the level of the corresponding node and "y"
 * is the node id at that level. By the definition of the merge tree and the
 * consolidation algorithm, the names appear in a non-increasing order of
 * "x" and a strictly increasing order of "y". For instance, if the 
 * consolidation step is 3 (meaning that the merge tree is 3-ary) and there have
 * been loaded 8 batches (creating 8 fragments), after consolidation, the 
 * following fragments will appear: "1_1", "1_2", "0_1", "0_2".
 * This means that there are 2 fragments at level 1 in the merge tree (each
 * encompassing three earlier fragments/batches), and 2 fragments at level
 * 0 (each encompassing just one fragment/batch).
 *
 * Continuing the above example, suppose that there are another 5 batches 
 * loaded, without activating the consolidation flag in tiledb_array_update().
 * When consolidation is triggered by executing this program, first the new
 * fragments are renamed to "0_3", ..., "0_7". Now we have fragments
 * "1_1", "1_2", "0_1", ..., "0_7". What consolidation
 * does is the following. It understands that it has to merge "0_1", 
 * "0_2", "0_3" into "1_3", and then "1_1", "1_2", 
 * "1_3" into "2_1". Instead of doing this recursively, it directly 
 * merges "1_1", "1_2", "0_1", "0_2", "0_3", into
 * "2_1". Then it merges "0_4", "0_5" and "0_6" into
 * "1_1". Finally, there is only one fragment remaining at level 0, 
 * namely "0_7". The program renames this into "0_1", in order to
 * maintain the invariant mentioned above, and then terminates.
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace folder where the array is 
 * defined. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array 
 * is defined. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array to be consolidated.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_define, tiledb_array_load, 
 * tiledb_array_update
 */
TILEDB_EXPORT int tiledb_array_consolidate( 
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_name);

/** 
 * Defines an array, specifying its schema. Every array must be defined prior to
 * its use. On error, it returns <b>-1</b>. Moreover, if it is compiled in 
 * <b>VERBOSE</b> mode, it prints comprehensive error messages on *stderr*. 
 * On success, it returns **0**.
 * 
 * Each array is comprised of a set of **dimensions**, and a set of 
 * **attributes**. Each array **cell** is essentially a tuple, consisting of a
 * set of dimension values, collectively called as *coordinates*, and a set of
 * *attribute values*. The coordinates and attribute values may be of different
 * **types**. All coordinates though must have the same type. The 
 * coordinates draw their values from their corresponding **dimension domains**.
 * A cell may have *multiple* (fixed or variable) values on each
 * attribute. TileDB stores internally only the *non-empty* (i.e., non-null,
 * non-zero) cells. 
 *
 * An array is internally represented as a set of **fragments** (i.e., snapshots
 * of the array after a set of updates), each consisting of a set of **tiles**. 
 * Each tile is essentially a *hyper-rectangle* in the *logical* (i.e., 
 * dimension) space that groups a set of non-empty cells. The tiles of an
 * array may be *regular* or *irregular*. Regular tiles have *fixed*
 * **tile extents** across each dimension, i.e., they have the same shape in 
 * the logical space, but may have a different non-empty cell **capacity**.
 * Irregular tiles have a *fixed* non-empty cell capacity, but may have 
 * different *shape* in the logical space.
 *
 * Each array stores its cells internally in a *sorted* **tile order** and 
 * **cell order**. For the case of irregular tiles, the tile order is implied
 * by the cell order. We refer to the sorted order of the cells as the 
 * **global cell order**.
 *
 * The user may define a **compression** type *per attribute*. The
 * compression is performed on a tile basis (implying also per fragment, per 
 * attribute).
 *
 * Finally, TileDB updates arrays in **batches**, i.e., it modifies 
 * *sets of cells* instead of single cells. When a new
 * set of cells is inserted into an array, TileDB initially creates a new
 * array **fragment** encompassing only the updates. Periodically, as new
 * updates arrive, TileDB **consolidates** multiple fragments into a single one.
 * The consolidation frequency is defined by a **consolidation step** parameter.
 * If this parameter is 1, every new fragment is always consolidated with the
 * existing one. If it is larger than 1, then consolidation occurs in a 
 * *hierarchical* manner. For instance, if **s** is the consolidation step,
 * each new fragment essentially represents a leaf of a complete **s**-ary 
 * **merge tree** that is constructed bottom-up. When **s** fragment-leaves are
 * created, they are consolidated into a single one, which becomes their parent
 * node in the tree, and the leaves are disregarded for the rest of the system
 * lifetime. In general, whenever **s** fragment-nodes are created at the
 * same level, they are consolidated into a new fragment that becomes
 * their parent, and these nodes are disregarded thereafter.
 *
 * All the data of an array are included in a directory in the underlying file
 * system, which has the same name as the array. Each array has a file called 
 * *array_schema* under its directory, which stores its schema information.
 * Multiple arrays can be organized into **groups**, where a group is a 
 * directory containing array directories and/or other group directories. Each 
 * group directory contains an empty file called <b>.tiledb_group</b>, which is 
 * used for sanity checks. A **workspace** can contain groups and/or arrays,
 * whereas it can also be a group itself. Groups enable the hierarchical 
 * organization of arrays, which follows naturally the directory organization in
 * file systems. The workspace practically constitutes the root of this 
 * organization. Each workspace directory contains an empty file called 
 * <b>.tiledb_workspace</b>, which is used for sanity checks.
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace folder where TileDB arrays can be
 * stored. Note that all non-existent directories in the workspace path will be
 * created. Moreover, the caller must have read and write permissions on the 
 * workspace directory. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array data 
 * will be stored. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored is directory *W1/G1/A*. The groups allow the 
 * hierarchical organization of TileDB arrays into sub-directories within the 
 * workspace directory. If the group "", the workspace is the 
 * default group. Note that all non-existent directories in the group path 
 * will be created.
 * @param array_schema_csv The array schema, serialized in a CSV line string
 * as follows (no spaces before and after the commas, this is a *single*
 * CSV line):
 * - - -
 * *array_name* , *attribute_num* , *attribute_name<SUB>1</SUB>* , ... ,
 * *attribute_name<SUB>attribute_num</SUB>* , 
 * *dim_num* , *dim_name<SUB>1</SUB>* , ... , *dim_name<SUB>dim_num</SUB>* , \n
 * *dim_domain_low<SUB>1</SUB>*, *dim_domain_high<SUB>1</SUB>*, ... , 
 * *dim_domain_low<SUB>dim_num</SUB>*, *dim_domain_high<SUB>dim_num</SUB>*, \n 
 * *type<SUB>attribute<SUB>1</SUB></SUB>* , ... ,
 * *type<SUB>attribute<SUB>attribute_num</SUB></SUB>* , 
 * *type<SUB>coordinates</SUB>* , 
 * *tile_extent<SUB>1</SUB>*, ... , *tile_extent<SUB>dim_num</SUB>* , \n
 * *cell_order* , *tile_order* , *capacity* , *consolidation_step*, \n
 * *compression<SUB>attribute<SUB>1</SUB></SUB>*, ..., 
 * *compression<SUB>attribute<SUB>attribute_num</SUB></SUB>*, 
 * *compression<SUB>coordinates</SUB>*
 * - - - 
 * The details of each array schema item are as follows: \n
 * - **array name** - *array_name* : \n 
 *     The name of the array whose schema is being defined. It can contain 
 *     only alphanumerics and characters '_', '.', and '-' (POSIX format). 
 * - **attribute names** - *attribute_num* , *attribute_name<SUB>1</SUB>* ,
 *   ... , *attribute_name<SUB>attribute_num</SUB>* : \n 
 *     The names of the attributes of the array. Each name can contain 
 *     only alphanumerics and characters '_', '.', and '-' (POSIX format). 
 *     The number of given names must comply with the *attribute_num* value.
 * - **dimension names** - *dim_num* , *dim_name<SUB>1</SUB>* ,
 *   ... , *dim_name<SUB>dim_num</SUB>* : \n 
 *     The names of the dimensions of the array. Each name can contain 
 *     only alphanumerics and characters '_', '.', and '-' (POSIX format). 
 *     The number of given names must comply with the *dim_num* value.
 *     A dimension cannot have the same name as an attribute.
 * - **dimension domains** - 
 *     *dim_domain_low<SUB>1</SUB>*, *dim_domain_high<SUB>1</SUB>*, ... , 
 *     *dim_domain_low<SUB>dim_num</SUB>*, 
 *     *dim_domain_high<SUB>dim_num</SUB>* \n
 *     The domains of the dimensions. There should be a [low,high] pair for
 *     every dimension, whose order must follow that of the dimension names
 *     in the list *dim_name<SUB>1</SUB>* , ... , *dim_name<SUB>dim_num</SUB>*.
 * - **types** - *type<SUB>attribute<SUB>1</SUB></SUB>* , ... ,
 *   *type<SUB>attribute<SUB>attribute_num</SUB></SUB>* , 
 *   *type<SUB>coordinates</SUB>* \n
 *     The types of the attributes and (collectively) of the coordinates. 
 *     Specifically, if there are *attribute_num* attributes, there should be 
 *     provided *attribute_num+1* types. The types of the attributes must be 
 *     given first, following the order of the attribute names in the 
 *     *attribute_name<SUB>1</SUB>* , ... , 
 *     *attribute_name<SUB>attribute_num</SUB>* list. The type of the 
 *     coordinates must be appear last. The supported attribute types are 
 *     **char**, **int32**, **int64**, **float32**, and **float64**.
 *     The supported coordinate types are **int32**, **int64**, **float32**, and
 *     **float64**. 
 * 
 *     Optionally, one may specify the number of values to be stored for a 
 *     particular attribute in each cell. This is done by appending ':' 
 *     followed by the desired number of values after the type (e.g., 
 *     **int32:3**). If no such value is provided, the default is 1. If one 
 *     needs to specify that an attribute may take a variable (unknown a priori)
 *     number of values, ':var' must be appended after the type (e.g., 
 *     **int32:var**). Note that the dimension type cannot have multiple values;
 *     a single set of coordinates uniquely identifies each cell.
 * - **tile extents** - *tile_extent<SUB>1</SUB>*, ... , 
 *   *tile_extent<SUB>dim_num</SUB>* \n
 *     It specifies the extent of a tile across each dimension. If this option 
 *     is included, then the array will have *regular* tiles; if it is omitted,
 *     the array will have *irregular* tiles. If there are *dim_num* dimensions,
 *     there should be provided *dim_num* tile extents, following the same order
 *     as that of the dimension names in the *dim_name<SUB>1</SUB>* ,
 *     ... , *dim_name<SUB>dim_num</SUB>* list. Each tile extent must
 *     be a non-negative real number that does not exceed the corresponding 
 *     domain size.
 * - **cell order** - *cell_order* \n
 *     The order in which the cells will be stored internally. The supported 
 *     orders are **row-major**, **column-major** and **hilbert** (i.e., 
 *     following the Hilbert space-filling curve). If no cell order is provided,
 *     the *default* is **row-major**.  
 * - **tile order** - *tile_order*  \n
 *     The order in which the tiles will be stored internally. The supported
 *     orders are **row-major**, **column-major** and **hilbert** (i.e., 
 *     following the Hilbert space-filling curve). If no cell order is provided,
 *     the default is **row-major**. This is applicable only to regular tiles.
 * - **capacity** - *capacity* \n
 *     This specifies the fixed number of non-empty cells stored in each tile. 
 *     It is applicable only to irregular tiles and, hence, cannot be used 
 *     together with tile extents and tile order. If it is not provided, a 
 *     default value is used. 
 * - **consolidation step** - *consolidation_step* \n
 *     It specifies the frequency of fragment consolidation, described above. 
 *     If it is not provided, the *default* is **1**.
 * - <b>compression</b> - *compression<SUB>attribute<SUB>1</SUB></SUB>*, ..., 
 *   *compression<SUB>attribute<SUB>attribute_num</SUB></SUB>*, 
 *   *compression<SUB>coordinates</SUB>* \n
 *     It specifies the compression type of each attribute and coorinates.
 *     Two compression types are currently supported: **NONE** and **GZIP**.
 *     If it is not provided the default is **NONE** for every attribute and 
 *     coordinates. If it is given, there should be an one-to-one
 *     correspondence between the attributes and the compression types,
 *     plus an extra compression type for the coordinates in the end.
 * .  
 * <br>
 * **NOTE:** To omit an optional array schema item (e.g., tile extents, 
 * capacity, etc), you *must* put character '*' in the corresponding field of 
 * the CSV string.  
 * .
 * <br> <br> 
 * **Examples**
 * - my_array , 3 , attr1 , attr2 , attr3 , 2 , dim1 , dim2 , 0 , 100 , 0 , 
 * 200 , int32:3 , float64 , char:var , int64 , * , hilbert , * , 1000 , 5, * \n
 *     \n
 *     This defines array *my_array*, which has *3* attributes and *2* 
 *     dimensions. Dimension *dim1* has domain [0,100], *dim2* has domain 
 *     [0,200]. The coordinates are of type **int64**. Attribute *attr1* is of 
 *     type **int32**, and each cell always stores *3* values on this attribute.
 *     Attribute *attr2* is of type **float64**, and each cell always stores *1*
 *     value on this attribute. Attribute *attr3* is of type **char**, and each
 *     cell stores a *variable* number of values on this attribute (i.e., it 
 *     stores arbitrary character strings). The array has *irregular* tiles.
 *     The cell order is **hilbert**. Each tile accommodates exactly *1000* 
 *     (non-empty) cells. Finally, the consolidation step is set to *5*.
 *     No compression will be used for any attribute. 
 * - - - 
 * - my_array , 3 , attr1 , attr2 , attr3 , 2 , dim1 , dim2 , 0 , 100 , 0 , 
 * 200 , int32:3 , float64 , char:var , int64 , 10, 20 , hilbert , 
 * row-major , * , 5 , GZIP, GZIP, NONE, NONE  \n
 *     \n
 *     This is similar to the previous example, but now the array has *regular*
 *     tiles. In detail, it defines array *my_array*, which has *3* attributes 
 *     and *2* dimensions. Dimension *dim1* has domain [0,100], *dim2* has 
 *     domain [0,200]. The coordinates are of type **int64**. Attribute *attr1*
 *     is of type **int32**, and each cell always stores *3* values on this 
 *     attribute. Attribute *attr2* is of type **float64**, and each cell always
 *     stores *1* value on this attribute. Attribute *attr3* is of type 
 *     **char**, and each cell stores a *variable* number of values on this 
 *     attribute (i.e., it stores arbitrary character strings). The array has
 *     *regular* tiles. Each tile has (logical) size *10x20*. The tile order is
 *     **row-major**, whereas the cell order is **hilbert**. The 
 *     consolidation step is set to *5*. GZIP compression will be used for
 *     *attr1* and *attr2*, and no compression for *attr3* and coordinates.
 *
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_schema_show
 */
TILEDB_EXPORT int tiledb_array_define(
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_schema_csv);

/** 
 * Deletes all data from an array. Contrary to tiledb_array_clear(), the array
 * does **not** remain defined, i.e., one must redefine its schema via
 * tiledb_array_define() prior to loading data to it via tiledb_array_load(),
 * or tiledb_array_update().
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace folder where the array is 
 * stored. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array 
 * is stored. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array to be deleted.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_clear
 */
TILEDB_EXPORT int tiledb_array_delete(    
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_name);

/** 
 * Exports the data of an array into a CSV or binary file. The documentation on
 * the exported CSV or binary format can be found in program 
 * tiledb_array_load().  
 *
 * The user can specify a subset of dimensions and attributes to export, as well
 * as to constrain the exported cell to a subarray. In addition, the user may 
 * specify whether to export the cells in the normal cell order they are stored
 * or in **reverse** order. The user may also select to export the array in
 * **dense** format, in which case even the empty cells are going to be 
 * exported, having special "empty" values in their attributes (e.g., 0 for 
 * numerics, and NULL_CHAR, '*', for strings). Finally, the user may specify a 
 * custom delimiter, as well as the number of decimal digits to be printed for 
 * real numbers, for the case of exporting to CSV files.
 *
 * On error, it returns <b>-1</b>. Moreover, if it is compiled in **VERBOSE**
 * mode, it prints comprehensive error messages on *stderr*. On success, it 
 * returns **0**.
 *
 * **Examples**
 *
 * Supposing that tiledb_ctx stores a pointer to a TileDB_Context object,
 * and assuming that the array has dimensions *dim1*,*dim2* and attributes
 * *attr1*,*attr2*,*attr3* :
 * - <b>tiledb_array_export(
 *         tiledb_ctx, "my_workspace", "my_group", "my_array",
 *         "my_array.csv", "csv", NULL, 0, NULL, 0, NULL, 0, '\\t', 3) </b> \n
 *    Exports into *my_array.csv* all the cells of array *my_array*, stored
 *    in folder *my_workspace/my_group/my_array*, in their native order, 
 *    including *all* the coordinates and attributes. They are exported in
 *    sparse format, and the CSV delimiter is the *tab*. The precision of
 *    real attribute values is limited to 3 digits.
 * - <b>tiledb_array_export(
 *         tiledb_ctx, "my_workspace", "my_group", "my_array",
 *         "my_array.bin.gz", "dense.bin.gz", {"dim1"}, 1,
 *         {"attr1","attr2"}, 2, NULL, 0, ',', 6) </b> \n
 *    Exports into GZIP compressed binary file *my_array.csv.gz* all the cells
 *    of *my_array*, stored in folder *my_workspace/my_group/my_array*, in the 
 *    native order, including only the coordinates on the *dim1* dimension and 
 *    the values on the *attr1*, *attr2* attributes. This time the array is 
 *    exported in dense form, i.e., even the empty cell coordinates will appear 
 *    in the file, having "empty" attribute values. The delimiter is ',' and
 *    the precision is 6 digits.
 * - <b> tiledb_array_export(
 *         tiledb_ctx, "my_workspace", "my_group", "my_array",
 *         "my_array.bin.gz", "reverse.dense.bin.gz", {"dim1"}, 1,
 *         {"attr1","attr2"}, 2, NULL, 0, 0, 0) </b> \n
 *   Same as above, but the cells are exported in the reverse order. Moreover,
 *   the delimiter and the precision values (0 in the example) are ignored.
 * - <b> tiledb_array_export(
 *         tiledb_ctx, "my_workspace", "my_group", "my_array",
 *         "my_array.bin.gz", "dense.bin.gz", {"__hide"}, 1,
 *         {"attr1","attr2"}, 2, {0,10,10,20}, 4, 0 , 0) </b> \n
 *   Same as the second example, but no coordinate is exported, and only the 
 *   cells with dim1 in [0,10] and dim2 in [10,20] are exported. Moreover,
 *   the delimiter and the precision values (0 in the example) are ignored.
 * - <b> tiledb_array_export(
 *         tiledb_ctx, "my_workspace", "my_group", "my_array",
 *         "my_array.bin.gz", "dense.bin.gz", {"dim1"}, 1,
 *         {"__hide"}, 1, NULL, 0, 0, 0) </b> \n
 *   Same as the second example, but no attribute value is exported. Moreover,
 *   the delimiter and the precision values (0 in the example) are ignored.
 * - <b> tiledb_array_export(
 *         tiledb_ctx, "my_workspace", "my_group", "my_array",
 *         "my_array.bin.gz", "dense.bin.gz", {"dim1"}, 1,
 *         {"attr1,attr2,attr1"}, 1, NULL, 0, 0, 0) </b> \n
 *   Same as the second example, but now the *attr1* values are shown twice
 *   (once before those of *attr2* and once after). Moreover,
 *   the delimiter and the precision values (0 in the example) are ignored.
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace directory where the array is 
 * stored. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array is 
 * stored. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array whose data will be exported.
 * @param filename The name of the exported file. 
 * @param format It can be one of the following: 
 * - **csv** (CSV format)
 * - **csv.gz** (GZIP-compressed CSV format)
 * - **dense.csv** (dense CSV format)
 * - **dense.csv.gz** (GZIP-compressed dense CSV format)
 * - **reverse.csv** (CSV format in reverse order)
 * - **reverse.csv.gz** (GZIP-compressed CSV format in reverse order)
 * - **reverse.dense.csv** (dense CSV format in reverse order)
 * - **reverse.dense.csv.gz** (dense GZIP-compressed CSV format in reverse 
 *   order)
 * - **bin** (binary format)
 * - **bin.gz** (GZIP-compressed binary format)
 * - **dense.bin** (dense binary format)
 * - **dense.bin.gz** (GZIP-compressed dense binary format)
 * - **reverse.bin** (binary format in reverse order)
 * - **reverse.bin.gz** (GZIP-compressed binary format in reverse order)
 * - **reverse.dense.bin** (dense binary format in reverse order)
 * - **reverse.dense.bin.gz** (dense GZIP-compressed binary format in reverse 
 *   order).
 * @param dim_names An array holding the dimension names to be exported. If it
 * is NULL, then all the coordinates will be exported. If it contains special 
 * name <b>"__hide"</b>, then no coordinates will be exported.
 * @param dim_names_num The number of elements in dim_names. 
 * @param attribute_names An array holding the attribute names to be exported.
 * If it is NULL, then all the attribute values will be exported. If it 
 * contains special name <b>"__hide"</b>, then no attribute values will be
 * exported.
 * @param attribute_names_num The number of elements in attribute_names. 
 * @param range A range given as a sequence of [low,high] bounds across each 
 * dimension. Each range bound must be a real number. The range constrains the
 * exported cells into a subarray. If the entire array must be exported, NULL
 * should be given.
 * @param range_size The number of elements in the *range* array.
 * @param delimiter This is meaningful only for CSV format. It stands for the 
 * delimiter which separates the values in a CSV line in the CSV file.The 
 * delimiter is ignored in the case of exporting to binary files. 
 * @param precision This applies only to exporting to CSV files (it is ignored
 * in the case of binary files). It indicates the number of decimal digits to
 * be printed for the real values.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_load, tiledb_dataset_generate
 */
TILEDB_EXPORT int tiledb_array_export( 
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* filename,
    const char* format,
    const char** dim_names,
    int dim_names_num,
    const char** attribute_names,
    int attribute_names_num,
    double* range,
    int range_size,
    char delimiter,
    int precision);

/** 
 * Generates sythetic data for a particular array schema in a **CSV** or
 * **binary** form. The output dataset essentially consists of a collection of
 * cells, whose format complies with the schema of the input array.
 * This means that the given array must already be defined. The coordinate
 * values are drawn uniformly at random from their corresponding dimension 
 * domains defined in the array schema. The attribute values depend on their
 * type. The real attribute values (i.e., **float32** and **float64**) are drawn
 * uniformly at random from **[0.0,1.0]**. The **int32** and **int64** data
 * values are drawn uniformly at random from **[0,max()]**, where **max()** is
 * the maximum value of the corresponding type supported by the system that
 * calls the function. Finally, the **char** values are drawn uniformly at
 * random from ASCII domain **[45,126]**, i.e., all the characters between and
 * including **'-'** and **'~'**.
 *
 * The user can must specify explicitly the data format. The CSV and 
 * binary formats are explained in detail in tiledb_array_load(). Note that the
 * generated data do not contain deletions.
 *
 * In addition, specifically or the case of CSV file, the use may specify a 
 * custom delimiter. Finally, the user may specify a random seed, so that he/she
 * can generate the same "random" dataset multiple times (e.g., for experiment
 * reproducibility purposes). 
 *
 * On error, it returns <b>-1</b>. Moreover, if it is compiled in **VERBOSE** 
 * mode, it prints comprehensive error messages on *stderr*. On success, it 
 * returns **0**.
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace directory where the array is 
 * defined. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array is 
 * defined. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array for which the data will be generated.
 * @param filename The name of the produced file. 
 * @param format It can be one of the following: 
 * - **csv** (CSV format)
 * - **csv.gz** (GZIP-compressed CSV format)
 * - **bin** (binary format)
 * - **bin.gz** (GZIP-compressed binary format)
 * @param seed The seed that will be used internally for the random generator.
 * @param cell_num The number of cells to be generated. It must be a positive
 * number
 * @param delimiter This is meaningful only for CSV format. It stands for the 
 * delimiter which separates the values in a CSV line in the CSV file. The 
 * delimiter is ignored in the case of generating binary data. 
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_load_csv
 */
TILEDB_EXPORT int tiledb_dataset_generate(    
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* filename,
    const char* format,
    unsigned int seed,
    int64_t cell_num,
    char delimiter);

/**
 * Checks if an array is defined within the database.
 * @param tiledb_ctx The TileDB context.
 * @param workspace The path to the workspace directory where the array is
 * stored. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array is 
 * stored. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data are stored in directory *W1/G1/A*. If the group is "", the 
 * the workspace is the default group.
 * @param array_name The name of the array to check.
 * @return **1** if the array is defined and **0** otherwise.
 */
TILEDB_EXPORT int tiledb_array_is_defined(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name);

/** 
 * Loads a collection of CSV or binary files into an array. The user specifies 
 * the path to a single file name, or to a directory containing a collection of 
 * files (all of which will be loaded into the array). Moreover, the user may 
 * indicate whether the cells in each file are sorted along the global cell 
 * order defined in the array schema or not. This has a dramatic effect on 
 * performance; if the cells are already sorted, then the costly sorting 
 * operation upon loading is avoided and, hence, the load performance is 
 * substantially improved. Note that, for the case of multiple sorted files, 
 * loading is essentially a *merge* operation (the cells are sorted in each 
 * file, but not necessarily across files). The user may also specify if the 
 * files are compressed or uncompressed. Finally, the user may specify an 
 * arbitrary delimiter in the case of CSV files. 
 * 
 * On error, it returns <b>-1</b>. Moreover, if it is compiled in **VERBOSE** 
 * mode, it prints comprehensive error messages on *stderr*. On success, it 
 * returns **0**.
 *
 * --- **CSV file format** --- \n 
 * 
 * The CSV file is essentially a collection of (CSV) lines, where each line
 * represents an array cell. The general format of each line is of the form
 * (no spaces before and after each comma):
 *
 * - - - 
 * *c<SUB>1</SUB>* , ... , *c<SUB>dim_num</SUB>* , *a<SUB>1</SUB>* , 
 * ... , *a<SUB>attribute_num</SUB>*
 * - - -
 *
 * where *c<SUB>1</SUB>* , ... , *c<SUB>dim_num</SUB>* are the *dim_num*
 * coordinates and *a<SUB>1</SUB>* , ... , *a<SUB>attribute_num</SUB>* are the 
 * *attribute_num* attributes.
 * 
 * If an attribute takes multiple values, but their number is *predefined* in 
 * the array schema (e.g., **int32:3**), then these values are simply included
 * next to each other and separated by comma. However, if the number of values 
 * is *variable* (e.g, **int32:var**), then the number of values must precede 
 * the actual attribute values (e.g., "3,0.1,0.2,0.3" for an attribute *attr1* 
 * whose type was defined as **float32:var** means that this cell stores three 
 * values on attribute *attr1*, namely 0.1, 0.2, and 0.3 (more examples are 
 * provided below).
 *
 * There is one exception of the above for the case of *strings* (i.e., variable
 * lengthed attributes defined as **char:var**). These are simply given
 * in one CSV field (e.g., "abc"), since their number can be easily deduced
 * by the size of the string (the same is not true for numerics). If multiple
 * strings are to be included in a **var:char** attribute, the user must
 * simply include an arbitray separator. For instance, one may store
 * strings "abc" and "defg" as "abc*defg". It falls upon the "conslumer"
 * of the data to recognize how to split the strings (TileDB simply stores
 * a variable number of characters). Finally, note that,
 * if an attribute is defined, say, as **char:3** (i.e., the number of
 * characters for this attribute per cell is known upon definition), then the
 * line must simply include a,b,c instead of "abc" (i.e., it is treated as
 * in the case of the other types).
 *
 * A **null** attribute value is represented by character '*'. 
 *
 * A **deletion** of a cell in TileDB is represented by a CSV line that 
 * contains the coordinates of the cell to be deleted, and stores character
 * '$' in *all* the attribute fields.
 * 
 * **Example CSV lines** \n 
 *
 * Suppose that the array contains **2 dimensions** and **3 attributes**, whose
 * types are defined as **int32:2,float64:var,char:var,int64** (recall that the 
 * last type corresponds always to all coordinates collectively).
 * - 1,3,10,11,2,0.1,0.2,paok \n
 *    (1,3) are the coordinates of the cell (of type int64). (10,11) is the 
 *    value on the first attribute (of type int32). (0.1,0.2) is the value of 
 *    the second attribute (of type float64). Finally, "paok" is the value of 
 *    the third attribute (of type char), and my favorite soccer team in Greece
 *    :P.
 * - 1,3,10,11,*,paok \n
 *    Same as above, but now the second attribute value is null.
 * - 1,3,$,$,$ \n
 *    %Cell (1,3) is being deleted.
 *
 * --- **Binary file format** --- \n 
 *
 * Each binary file is essentially a collection of cells in binary form,
 * concatenated one after the other in the file. The general format of each 
 * binary cell is the following (all values in binary format and of the 
 * corresponding type defined in the array schema, and '|' denotes binary 
 * concatenation):
 *
 * - - - 
 * *c<SUB>1</SUB>* | ... | *c<SUB>dim_num</SUB>* | *a<SUB>1</SUB>* | 
 * ... | *a<SUB>attribute_num</SUB>*
 * - - -
 *
 * where *c<SUB>1</SUB>* , ... , *c<SUB>dim_num</SUB>* are the *dim_num*
 * coordinates and *a<SUB>1</SUB>* , ... , *a<SUB>attribute_num</SUB>* are the 
 * *attribute_num* attributes.
 * 
 * If an attribute takes multiple values, but their number is *predefined* in 
 * the array schema (e.g., **int32:3**), then these values are simply 
 * concatenated next to each other. However, if the number of values is 
 * *variable* (e.g, **int32:var**), then the number of values must precede 
 * the actual attribute values, and it should be of type **int32**. For example,
 * **3 | 0.1 | 0.2 | 0.3** for an attribute *attr1* whose type was defined as
 * **float32:var** means that this cell stores **3** values on attribute 
 * *attr1*, namely **0.1,0.2,0.3** (more examples are provided below).
 * Moreover, if even a single attribute is variable-sized, the size of the
 * **entire** binary cell must be included immediately after the coordinates and
 * before the attributes, and it must be of type **int32**
 * (i.e., **unsigned int32**). Note that the cell size is essentially the size
 * of coordinates, plus the size of attributes, plus the size of a **int32**
 * that holds the size value (examples below).
 *
 * A **null** attribute value is represented by the **maximum** value in
 * the domain of the corresponding type. For attributes of type **char**, null
 * is represented by character '*'.
 *
 * A **deletion** of a cell in TileDB is represented by a cell that 
 * contains the coordinates of the cell to be deleted, and stores the 
 * **maximum-1** value of the corresponding type in *all* the attribute 
 * fields. For attributes of type **char**, a deletion is represented by
 * the binary representation of character '$'. 
 * 
 * **Example binary cells** \n 
 *
 * Suppose that the array contains **2 dimensions** and **3 attributes**, whose
 * types are defined as **int32:3,float64,char,int64** (recall that the 
 * last type corresponds always to all coordinates collectively). Observe that 
 * this schema essentially defines a **fixed size** for *all* cells in the 
 * array.
 * - 1 | 3 | 10 | 11 | 12 | 0.1 | p \n
 *    (1,3) are the coordinates of the cell (of type **int64**). 
 *    (10,11,12) is the value on the *first* attribute (of type **int32**). 
 *    **0.1**  is the value of the *second* attribute (of type **float64**). 
 *    Finally, **p** is the value of the *third* attribute (of type **char**).
 *
 * Now suppose that the array contains **2 dimensions** and **3 attributes** as
 * in the previous example, but their types are now defined as
 * **int32:3,float64:var,char:var,int64**. This means that the cells of the 
 * array may be of **variable size**. Also let an **int32** value consume 
 * *4 bytes*, a **int32** *4 bytes*, a **float64** *8 bytes*, 
 * a **char** *1 byte*, and a **int64** *8 bytes*.
 * - 1 | 3 | 60 | 10 | 11 | 12 | 2 | 0.1 | 0.2 | 4 | paok \n
 *    (1,3) are the coordinates of the cell (of type **int64**). **60**
 *    is the size in bytes of the entire cell (including even this size value 
 *    itself). (10,11,12) is the value on the *first* attribute (of type 
 *    **int32**). **2** is the number of values for the *second* attribute (of 
 *    type **int32**). (0.1,0.2) is the values of the *second* attribute (of 
 *    type **float64**). **4** is the number of characters in the string of the 
 *    *third* attribute (of type **int32**). Finally, **paok** is the string of 
 *    the *third* attribute (of type **char**).
 *
 * --- **Physical Representation of array data** --- \n
 * 
 * As explained in tiledb_array_define(), every array is associated with
 * a directory with the same name as the array, in a particular group directory
 * hierarchy inside a particular workspace directory. The array directory 
 * contains one folder for each fragment. Upon loading data, the array
 * is cleared, and a new fragment is created as a folder in the array directory.
 *
 * The new fragment (and its directory) is initially named after the process
 * that created it and a timestamp, i.e., it is in the form 
 * <b>"__pid_timestamp"</b>. This name will change later if the fragment takes
 * part in a **consolidation** process, explained in tiledb_consolidate().
 * Note that during the loading operation, the fragment has name 
 * <b>".__pid_timestamp"</b>, so that it is hidden from potential reads that 
 * occur simultaneously with the fragment creation. When the fragment is created
 * successfully, its name changes to <b>"__pid_timestamp"</b>.
 *
 * Each fragment consists of **data** files and **book-keeping** files.
 * There is one data file for each attribute (named after the attribute) and
 * one file for the coordinates (called <b>"__coords.tdt"</b>). All data files
 * have suffix <b>".tdt"</b>. The book-keeping files have suffix 
 * <b>".bkp.gz"</b> and they are always GZIP'ed. There is one file called 
 * <b>"mbrs.bkp.gz"</b> that keeps the tile 
 * **minimum bounding rectangles (MBRs)**, which are useful for searching. File
 * <b>"offsets.bkp.gz"</b> maintains the offsets of the tiles in the data files.
 * Finally, <b>"bounding_coordinates.bkp.gz"</b> keeps the first and last 
 * coordinate of each tile, which are useful in various operations.
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace directory where the array is 
 * defined. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array is 
 * defined. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array into which the file(s) are
 * loaded. Note that the array must already be defined.
 * @param path The path to a CSV/binary file or to a directory of CSV/binary 
 * files. If it is a file, then this single file will be loaded. If it is a 
 * directory, **all** the files in the directory will be loaded.   
 * @param format It can be one of the following: 
 * - **csv** (CSV format)
 * - **sorted.csv** (sorted CSV format)
 * - **csv.gz** (GZIP-compressed CSV format)
 * - **sorted.csv.gz** (sorted GZIP-compressed CSV format)
 * - **bin** (binary format)
 * - **sorted.bin** (sorted binary format)
 * - **bin.gz** (GZIP-compressed binary format)
 * - **sorted.bin.gz** (sorted GZIP-compressed binary format)
 * @param delimiter This is meaningful only for CSV format. It stands for the 
 * delimiter which separates the values in a CSV line in the CSV file.  
 * @return **0** for success and <b>-1</b> for error.
 * @note All the files in a directory path must be in the *same* format.
 * @see TileDB_Context, tiledb_array_define, tiledb_array_export,
 * tiledb_dataset_generate
 */
TILEDB_EXPORT int tiledb_array_load(    
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* path,
    const char* format,
    char delimiter);

/**
 * Copies the array schema CSV string description into *array_schema_c_str*.
 * The maximum buffer length is given by *array_schema_c_str_length*.
 * If the schema string is longer than *array_schema_c_str_length*, then the 
 * schema CSV string is not copied to *array_schema_c_str*, and only the
 * *array_schema_c_str_length* pointer is updated with the size needed to
 * store the CSV string.
 * 
 * For a detailed decrption of the CSV schema format, see tiledb_array_define().
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace folder where the array is 
 * defined. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array 
 * is defined. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array whose schema is retrieved.
 * @param array_schema_c_str A pointer to a **pre-allocated** buffer to copy the
 * resulting schema CSV string.
 * @param array_schema_c_str_length A pointer to the pre-allocated maximum
 * buffer length to hold the schema CSV string.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_schema_show, tiledb_array_is_defined,
 * tiledb_array_define
 */
TILEDB_EXPORT int tiledb_array_schema_get(
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_name,
    char* array_schema_c_str, 
    size_t* array_schema_c_str_length);

/** 
 * Prints the schema of an array on the standard output. The array must be
 * defined. 
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace folder where the array is 
 * defined. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array 
 * is defined. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array whose schema is printed.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_define
 */
TILEDB_EXPORT int tiledb_array_schema_show(    
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* group,
    const char* array_name);

/** 
 * Creates a new array with the same schema as the input array (or including a 
 * subset of the attributes in a potentially different order), 
 * conataining only the cells that lie in the input range. The range must be a 
 * hyper-rectangle that is completely contained in the dimension space. It is
 * also given as a sequence of [low,high] pairs across each dimension. 
 * On error, it returns <b>-1</b>. Moreover, if it is compiled in **VERBOSE**
 * mode, it prints comprehensive error messages on *stderr*. On success, it 
 * returns **0**.
 *
 * **Examples**
 *
 * Supposing that tiledb_ctx stores a pointer to a TileDB_Context object,
 * and assuming that the array has dimensions *dim1*,*dim2* with domains
 * [0,100] and [0,200], respectively, and attributes *attr1*,*attr2*,*attr3* :
 * - <b> tiledb_subarray(
 *         tiledb_ctx, "my_workspace", "my_workspace_sub", 
 *         "my_group", "my_group_sub", "my_array", "my_array_sub", 
 *         {10,20,100,150}, 4, NULL, 0, 0) </b> \n
 *     Creates a new array *my_array_sub* with exactly the same schema as
 *     *my_array*, but only containing the cells with *dim1* in 
 *     **[10,20]** and *dim2* in **[100,150]**. The input array is
 *     stored in *my_workspace/my_group/my_array*, whereas the output
 *     array is stored in *my_workspace_sub/my_group_sub/my_array_sub*.
 * - <b> tiledb_subarray(
 *         tiledb_ctx, "my_workspace", "my_workspace_sub", 
 *         "my_group", "my_group_sub", "my_array", "my_array_sub", 
 *         {10,20,100,150}, 4, {"attr1","attr2"}, 2, 0) </b> \n
 *     Same as the first example, but now \fImy_array_sub\fR contains only two
 *     attributes, namely *attr1* and *attr2*.
 * - <b> tiledb_subarray(
 *         tiledb_ctx, "my_workspace", "", 
 *         "my_group", "", "my_array", "my_array_sub", 
 *         {10,20,100,150}, 4, {"attr2","attr1"}, 2, 0) </b> \n
 *     Same as the second example, but *attr1* and *attr2* appear
 *     in a different order in *my_array_sub*. Also the output array
 *     is stored in the same workspace and group as the input array,
 *     namely *my_workspace/my_group/my_array*.
 * - <b> tiledb_subarray(
 *         tiledb_ctx, "my_workspace", "my_workspace_sub", 
 *         "my_group", "my_group_sub", "my_array", "my_array_sub", 
 *         {10,20,100,150}, 4, {"attr1","attr2"}, 2, 1) </b> \n
 *     Same as the second example, but now the result array will be stored in
 *     dense format, i.e., it will store even empty cells.
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace folder where the input array is 
 * stored. If *workspace* is "", then the current working directory is set
 * as the workspace by default. 
 * @param workspace_sub The path to the workspace where the subarray result
 * will be stored. If *workspace_sub* is "", then the input array workspace
 * is set as the result workspace by default.
 * @param group This is a directory inside the workspace where the input array 
 * is stored. Note that any group path provided is converted 
 * into an absolute path **relative to its corresponding workspace directory**,
 * i.e., regarding all home ("~/"), current ("./") and root ("/") as its
 * workspace directory. If *group* is "", then the input array workspace is
 * set as the group by default.
 * group_sub This is a directory inside the workspace where the subarray result 
 * is stored. Note that any group path provided is converted 
 * into an absolute path **relative to its corresponding workspace directory**,
 * i.e., regarding all home ("~/"), current ("./") and root ("/") as its
 * workspace directory. If *group_sub* is "", then the output array workspace
 * is set as the group by default.
 * @param array_name The name of the array the subarray will be applied on.
 * @param array_name_sub The name of the output array.
 * @param range The range of the subarray. It must contain real values.
 * @param range_size The nunber of elements of the range vector. It must be
 * equal to 2*dim_num, where *dim_num* is the number of the dimensions of the
 * array.
 * @param attribute_names An array holding the attribute names to be included
 * in the schema of the result array. If it is NULL, then all the attributes
 * of the input array will appear in the output array.
 * @param attribute_names_num The number of elements in attribute_names.
 * @return **0** for success and <b>-1</b> for error.
 * @see TileDB_Context, tiledb_array_define
 */
TILEDB_EXPORT int tiledb_subarray(    
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace,
    const char* workspace_sub,
    const char* group,
    const char* group_sub,
    const char* array_name,
    const char* array_name_sub,
    const double* range,
    int range_size,
    const char** attribute_names,
    int attribute_names_num);

/**
 * This is very similar to tiledb_array_load(). The difference is that
 * the loaded data create a new **fragment** in the array (i.e., they do not
 * delete the previous data), which may be *consolidated* with some or all of 
 * the existing ones. The update mechanism is described in detail in
 * tiledb_array_define().
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param workspace The path to the workspace directory where the array is 
 * defined. If it is "", the current working directory is 
 * set as the workspace by default.
 * @param group This is a directory inside the workspace where the array is 
 * defined. Note that any group path provided is converted into an 
 * absolute path **relative to the workspace directory**, i.e., 
 * regarding all home ("~/"), current ("./") and root ("/") as the workspace
 * directory. For instance, if *workspace* is *W1* and *group* is *~/G1*, and
 * supposing that the array name is *A*, then the 
 * **canonicalized absolute path** of the group is *W1/G1*, and the array
 * data will be stored in directory *W1/G1/A*. If the group is "", 
 * the workspace is the default group.
 * @param array_name The name of the array into which the file(s) are
 * loaded. Note that the array must already be defined.
 * @param path The path to a CSV/binary file or to a directory of CSV/binary 
 * files. If it is a file, then this single file will be loaded. If it is a 
 * directory, **all** the files in the directory will be loaded.   
 * @param format It can be one of the following: 
 * - **csv** (CSV format)
 * - **sorted.csv** (sorted CSV format)
 * - **csv.gz** (GZIP-compressed CSV format)
 * - **sorted.csv.gz** (sorted GZIP-compressed CSV format)
 * - **bin** (binary format)
 * - **sorted.bin** (sorted binary format)
 * - **bin.gz** (GZIP-compressed binary format)
 * - **sorted.bin.gz** (sorted GZIP-compressed binary format) 
 * @param delimiter This is meaningful only for CSV format. It stands for the 
 * delimiter which separates the values in a CSV line in the CSV file. The 
 * delimiter is ignored in the case of loading binary data.
 * @param consolidate If *1*, then after the creation of the new fragment,
 * consolidation will take place as explained in tiledb_array_define(), whereas
 * if *0* the fragments will remain intact. The reason why one may choose to 
 * consolidate on demand is *parallelism*. For instance, if one wishes to 
 * parallelize loading a huge CSV file, he/she may split the file into, say, 10 
 * CSV files, and spawn 10 different processes that invoke 
 * tiledb_array_update(), which all can work in parallel. He/She can then 
 * consolidate on demand by spawning a separate process that invokes 
 * tiledb_array_consolidate().
 * @return **0** for success and <b>-1</b> for error.
 * @note All the files in a directory path must be in the *same* format.
 * @see TileDB_Context, tiledb_array_define, tiledb_array_load, 
 * tiledb_array_export, tiledb_array_load, tiledb_dataset_generate
 */
TILEDB_EXPORT int tiledb_array_update( 
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* path,
    const char* format,
    char delimiter,
    int consolidate);

/* ********************************* */
/*               MISC                */
/* ********************************* */

/**
 * Returns the current TileDB version.
 * @return A string stating the TileDB version in the form "TileDB vX.Y", for
 * variable "X" and "Y".
 */
TILEDB_EXPORT const char* tiledb_version(); 

#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif
