/**
 * @file   tiledb_cell_iterators.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file declares the C APIs for the use of cell iterators.
 */

#ifndef __TILEDB_CELL_ITERATORS_H__
#define __TILEDB_CELL_ITERATORS_H__

#include "tiledb_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

#if (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#  define TILEDB_EXPORT __attribute__((visibility("default")))
#else
#  define TILEDB_EXPORT
#endif

/** A constant cell iterator. */
typedef struct TileDB_ConstCellIterator TileDB_ConstCellIterator;

/** A constant reverse cell iterator. */
typedef struct TileDB_ConstReverseCellIterator TileDB_ConstReverseCellIterator;

/**
 * Finalizes a constant cell iterator, clearing its state.
 * @param cell_it The cell iterator.
 * @return TBD
 * @see tiledb_const_cell_iterator_init, tiledb_const_cell_iterator_next
 */
TILEDB_EXPORT int tiledb_const_cell_iterator_finalize(
    TileDB_ConstCellIterator*& cell_it);

/**
 * Finalizes a constant reverse cell iterator, clearing its state.
 * @param cell_it The cell iterator.
 * @return TBD
 * @see tiledb_const_revrse_cell_iterator_init, 
 * tiledb_const_reverse_cell_iterator_next
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_finalize(
    TileDB_ConstReverseCellIterator*& cell_it);

/** 
 * Initializes a constant cell iterator
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of attribute_names. If it zero, then
 * **all** attributes are used.
 * @param cell_it The cell iterator that is initialized
 * @return An error code, which may be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_const_cell_iterator_next,
 * tiledb_const_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstCellIterator*& cell_it);

/** 
 * Initializes a constant cell iterator, but contrains it inside a particular
 * subspace oriented by the input range.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of attribute_names. If it zero, then
 * **all** attributes are used.
 * @param range The subspace in which the iterator is constrained. The number
 * of elements should be 2*dim_num, and it is derived from the array schema.
 * The type must be the same as the coordinates.
 * @param cell_it The cell iterator that is initialized
 * @return An error code, which may be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_const_cell_iterator_next,
 * tiledb_const_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_cell_iterator_init_in_range(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstCellIterator*& cell_it);

/** 
 * Initializes a constant reverse cell iterator
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of attribute_names. If it zero, then
 * **all** attributes are used.
 * @param cell_it The cell iterator that is initialized
 * @return An error code, which may be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_const_reverse_cell_iterator_next,
 * tiledb_const_reverse_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstReverseCellIterator*& cell_it);

/** 
 * Initializes a constant reverse cell iterator, but contrains it inside a 
 * particular subspace oriented by the input range.
 * @param tiledb_ctx The TileDB state.
 * @param ad The descriptor of the array for which the iterator is initialized
 * @param attribute_names The names of the attributes included in the returned
 * cells. This essentially realizes projections on attributes. If one does not
 * need attributes at all (i.e., the iterator should iterate only over the
 * coordinates) then he/she should include "__hide" as the only name in 
 * this argument.
 * @param attribute_names_num The length of attribute_names. If it zero, then
 * **all** attributes are used.
 * @param range The subspace in which the iterator is constrained. The number
 * of elements should be 2*dim_num, and it is derived from the array schema.
 * The type must be the same as the coordinates.
 * @param cell_it The cell iterator that is initialized
 * @return An error code, which may be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_const_cell_iterator_next,
 * tiledb_const_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_init_in_range(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstReverseCellIterator*& cell_it);

/** 
 * Retrieves the next cell. If the iterator has reached its end, then the 
 * the returned cell is NULL. The format of a binary cell is the same as
 * that described in tiledb_load_bin(). If the iterator was initialized
 * with a subset of attributes, then only those attributes are included
 * in the binary cell.
 * @param cell_it The cell iterator
 * @param cell The returned cell. It is NULL if the iterator has reached its
 * end. 
 * @return An error code, which may be one of the following:
 * TBD
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
 * in the binary cell.
 * @param cell_it The cell iterator
 * @param cell The returned cell. It is NULL if the iterator has reached its
 * end. 
 * @return An error code, which may be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_const_cell_iterator_init,
 * tiledb_const_cell_iterator_finalize
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_next(
    TileDB_ConstReverseCellIterator* cell_it,
    const void*& cell);

#undef TILEDB_EXPORT

#ifdef __cplusplus
}
#endif

#endif
