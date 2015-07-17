/**
 * @file   tiledb.h
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
 * This file declares the C APIs for TileDB.
 */

#ifndef __TILEDB_H__
#define __TILEDB_H__

#include "tiledb_error.h"
#include <stdint.h>
#include <stddef.h>

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

/**  Constitutes the TileDB state, wrapping the TileDB modules. */
typedef struct TileDB_CTX TileDB_CTX; 

/** 
 * Finalizes the TileDB context. On error, it prints a message on stderr and
 * returns an error code (shown below).
 * @param tiledb_context The TileDB state.
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 * @see TileDB_CTX, tiledb_ctx_init
 */
TILEDB_EXPORT int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx);

/** 
 * Initializes the TileDB context. On error, it prints a message on stderr and
 * returns an error code (shown below). 
 * @param workspace The path to the workspace folder, i.e., the directory where
 * TileDB will store the array data. The workspace must exist, and the caller
 * must have read and write permissions to it.
 * @param tiledb_context The TileDB state.
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENSMCREAT**\n 
 *     Failed to create storage manager
 *   - **TILEDB_ENLDCREAT**\n 
 *     Failed to create loader
 *   - **TILEDB_ENQPCREAT**\n 
 *     Failed to create query processor
 * @see TileDB_CTX, tiledb_ctx_finalize
 */
TILEDB_EXPORT int tiledb_ctx_init(
    const char* workspace, 
    TileDB_CTX*& tiledb_ctx);

/**
 * Returns the workspace path of the TileDB_Context.
 * @param tiledb_context The TileDB state.
 * @return A C string pointer to the TileDB_Context's workspace.
 * @see TileDB_Context
 */
TILEDB_EXPORT const char* tiledb_ctx_workspace(TileDB_CTX* tiledb_ctx);

/* ********************************* */
/*                I/O                */
/* ********************************* */

/** 
 * Closes an array, cleaning its metadata from main memory.
 * @param tiledb_context The TileDB state.
 * @param ad The descriptor of the array to be closed
 * @return An array descriptor (>=0) or an error code (<0). The error code may
 * be one of the following:
 * TBD
 * @see TileDB_Context, tiledb_array_open
 */
TILEDB_EXPORT int tiledb_array_close(
    TileDB_CTX* tiledb_ctx,
    int ad);

/** 
 * Prepares an array for reading or writing, reading its matadata in main 
 * memory. It returns an **array descriptor**, which is used in subsequent array
 * operations. 
 * @param tiledb_context The TileDB state.
 * @param array_name The name of the array to be opened
 * @param mode The mode in which is the array is opened. Currently, the 
 * following  modes are supported:
 * - **r**: Read mode
 * - **w**: Write mode (if the array exists, its data are cleared)
 * - **a**: Append mode (used when updating the array)
 * @return An array descriptor (>=0) or an error code (<0). The error code may
 * be one of the following:
 * TBD
 * @see TileDB_CTX, tiledb_array_close
 */
TILEDB_EXPORT int tiledb_array_open(
    TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* mode);

/** 
 * Writes a binary cell to an array. The format of the cell is discussed in
 * tiledb_load_bin().
 * @param tiledb_context The TileDB state.
 * @param ad The descriptor of the array to receive the cell
 * @param cell The binary cell to be written 
 * @return An error code that may be one of the following:
 * TBD
 * @see TileDB_CTX, tiledb_write_cell_sorted
 */
TILEDB_EXPORT int tiledb_write_cell(
    TileDB_CTX* tiledb_ctx,
    int ad, 
    const void* cell);

/** 
 * Writes a binary cell to an array. The format of the cell is discussed in
 * tiledb_load_bin(). The difference to tildb_write_cell() is that the 
 * cells are assumed to be written in the same order as the cell order
 * defined in the array. Therefore, this is a simple **append** command,
 * whereas tiledb_write_cell() at some point triggers **sorting**.
 * @param tiledb_context The TileDB state.
 * @param ad The descriptor of the array to receive the cell
 * @param cell The binary cell to be written 
 * @return An error code that may be one of the following:
 * TBD
 * @see TileDB_CTX, tiledb_write_cell
 */
TILEDB_EXPORT int tiledb_write_cell_sorted(
    TileDB_CTX* tiledb_ctx,
    int ad, 
    const void* cell);

/* ********************************* */
/*           CELL ITERATORS          */
/* ********************************* */

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
    TileDB_ConstCellIterator* cell_it);

/**
 * Finalizes a constant reverse cell iterator, clearing its state.
 * @param cell_it The cell iterator.
 * @return TBD
 * @see tiledb_const_revrse_cell_iterator_init, 
 * tiledb_const_reverse_cell_iterator_next
 */
TILEDB_EXPORT int tiledb_const_reverse_cell_iterator_finalize(
    TileDB_ConstReverseCellIterator* cell_it);

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

/* ********************************* */
/*              QUERIES              */
/* ********************************* */

/** 
 * Clears all data from an array. However, the array remains defined, i.e.,
 * one can still invoke tiledb_load_csv() for this array, without having
 * to redefine the array schema.
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array whose data will be deleted.
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 * @see TileDB_Context, tiledb_delete_array, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_clear_array(    
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name);

/** 
 * Defines an array, specifying its schema. Every array must be defined prior to
 * its use. On error, it prints appopriate messages on stderr and returns 
 * an error code (outlined below).
 * Each array is comprised of a set of **dimensions**, and a set of 
 * **attributes**. Each array **cell** is essentially a tuple, consisting of a
 * set of dimension values, collectively called as *coordinates*, and a set of
 * attribute values. The coordinates and attribute values may be of different
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
 * the, logical space, but may have a different non-empty cell **capacity**.
 * Irregular tiles have a *fixed* non-empty cell capacity, but may have 
 * different *shape* in the logical space.
 *
 * Each array stores its cells internally in a *sorted* **tile order** and 
 * **cell order**. For the case of irregular tiles, the tile order is implied
 * by the cell order. 
 *
 * Finally, TileDB updates arrays in *batches*, i.e., it modifies 
 * *sets of cells* instead of single cells. When a new
 * set of cells is inserted into an array, TileDB initially creates a new
 * array **fragment** encompassing only the updates. Periodically, as new
 * updates arrive, TileDB **consolidates** multiple fragments into a single one.
 * The consolidation frequency is defined by a **consolidation step** parameter.
 * If this parameter is 1, every new fragment is always consolidated with the
 * existing one. If it is larger than 1, then consolidation occurs in a 
 * *hierarchical* manner. For instance, if **s** is the consolidation step,
 * each new fragment essentially represents a leaf of a complete **s**-ary 
 * tree that is constructed bottom-up. When **s** fragment-leaves are created,
 * they are consolidated into a single one, which becomes their parent node
 * in the tree, and the leaves are disregarded for the rest of they system
 * lifetime. In general, whenever **s** fragment-nodes are created at the
 * same level, they are consolidated into a new fragment that becomes
 * their parent, and these nodes are disregarded thereafter.
 *
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
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
 * *cell_order* , *tile_order* , *capacity* , *consolidation_step*
 * - - - 
 * The details of each array schema item are as follows: \n
 * - **array name** - *array_name* : \n 
 *     The name of the array whose schema is being defined. It can contain 
 *     only alphanumerics and character '_'. 
 * - **attribute names** - *attribute_num* , *attribute_name<SUB>1</SUB>* ,
 *   ... , *attribute_name<SUB>attribute_num</SUB>* : \n 
 *     The names of the attributes of the array. Each name can contain only
 *     alphanumerics and character '_'. The number of given names must comply
 *     with the *attribute_num* value.
 * - **dimension names** - *dim_num* , *dim_name<SUB>1</SUB>* ,
 *   ... , *dim_name<SUB>dim_num</SUB>* : \n 
 *     The names of the dimensions of the array. Each name can contain only
 *     alphanumerics and character '_'. The number of given names must comply
 *     with the *dim_num* value.
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
 *     **char**, **int**, **int64**, **float**, and **double**.
 *     The supported coordinate types are **int**, **int64**, **float**, and 
 *     **double**. 
 * 
 *     Optionally, one may specify the number of values to be stored for a 
 *     particular attribute in each cell. This is done by appending ':' 
 *     followed by the desired number of values after the type (e.g., 
 *     **int:3**). If no such value is provided, the default is 1. If one needs
 *     to specify that an attribute may take a variable (unknown a priori) 
 *     number of values, ':var' must be appended after the type (e.g., 
 *     **int:var**). Note that the dimension type cannot have multiple values; a
 *     single set of coordinates uniquely identifies each cell.
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
 *     If it is not provided, the default is 1.
 * .  
 * <br>
 * **NOTE:** To omit an optional array schema item (e.g., tile extents, 
 * capacity, etc), you *must* put character '*' in the corresponding field of 
 * the CSV string.  
 * .
 * <br> <br> 
 * **Examples**
 * - my_array , 3 , attr1 , attr2 , attr3 , 2 , dim1 , dim2 , 0 , 100 , 0 , 
 * 200 , int:3 , double , char:var , int64 , * , hilbert , * , 1000 , 5 \n
 *     This defines array *my_array*, which has *3* attributes and *2* 
 *     dimensions. Dimension *dim1* has domain [0,100], *dim2* has domain 
 *     [0,200]. The coordinates are of type **int64**. Attribute *attr1* is of 
 *     type **int**, and each cell always stores *3* values on this attribute. 
 *     Attribute *attr2* is of type **double**, and each cell always stores *1* 
 *     value on this attribute. Attribute *attr3* is of type **char**, and each
 *     cell stores a *variable* number of values on this attribute (i.e., it 
 *     stores arbitrary character strings). The array has *irregular* tiles.
 *     The cell order is **hilbert**. Each tile accommodates exactly *1000* 
 *     (non-empty) cells. Finally, the consolidation step is set to *5*.
 * - my_array , 3 , attr1 , attr2 , attr3 , 2 , dim1 , dim2 , 0 , 100 , 0 , 
 * 200 , int:3 , double , char:var , int64 , 10, 20 , hilbert , 
 * row-major , * , 5 \n
 *     This is similar to the previous example, but now the array has *regular*
 *     tiles. In detail, it defines array *my_array*, which has *3* attributes 
 *     and *2* dimensions. Dimension *dim1* has domain [0,100], *dim2* has 
 *     domain [0,200]. The coordinates are of type **int64**. Attribute *attr1*
 *     is of type **int**, and each cell always stores *3* values on this 
 *     attribute. Attribute *attr2* is of type **double**, and each cell always 
 *     stores *1* value on this attribute. Attribute *attr3* is of type 
 *     **char**, and each cell stores a *variable* number of values on this 
 *     attribute (i.e., it stores arbitrary character strings). The array has
 *     *regular* tiles. Each tile has (logical) size *10x20*. The tile order is
 *     **row-major**, whereas the cell order is **hilbert**. Finally, the 
 *     consolidation step is set to *5*.
 *
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_PRARRSCHEMA**\n
 *     Failed to parse array schema
 * @see TileDB_Context, tiledb_show_array_schema, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_define_array(
    const TileDB_CTX* tiledb_ctx, 
    const char* array_schema_csv);

/**
 * Checks if the array with a given name is defined within the database.
 * @param tiledb_context The TileDB state
 * @param array_name The name of the array to query the database
 * @return A boolean return code, which can be one of the following:
 *   - **1**\n
 *     Array is defined
 *   - **0**\n
 *     Array is not defined
 */
TILEDB_EXPORT int tiledb_array_defined(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name);

/**
 * Copies the array schema CSV string description into schema_str.
 * The maximum buffer length is given by schema_length.  If the schema
 * string is longer than schema_length then the schema_string is not copied
 * and the schema_length pointer is updated.
 * 
 * For a detailed decrption of the CSV schema format, see tiledb_array_defined.
 *
 * @param tiledb_ctx The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array to query the schema.
 * @param schema_str A pointer to a pre-allocated buffer to copy the resulting
 *                   CSV schema string.
 * @param schema_length A pointer to the pre-allocated buffer maximum
 *                   buffer length.
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 * @see TileDB_Context, tiledb_show_array_schema, tiledb_array_defined 
 */
TILEDB_EXPORT int tiledb_array_schema(
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    char* schema_str, 
    size_t* schema_length);

/** 
 * Deletes all data from an array. Contrary to tiledb_clear_array(), the array
 * does **not** remain defined, i.e., one must redefine its schema (via
 * tiledb_define_array()) prior to loading data to it (e.g., via 
 * tiledb_load_csv()).
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array that will be deleted.
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 * @see TileDB_Context, tiledb_delete_array, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_delete_array(    
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name);

/** 
 * Exports the (binary) data of an array into a CSV file. The documentation on
 * the exported CSV format can be found in function tiledb_load_csv(). 
 * On error, it prints a message on stderr and returns an error code
 * (shown below).
 *
 * The user may also specify a subset of attributes and dimensions from the
 * array to export. Also, he/she may specify any order of attributes and 
 * dimensions, as well as multiplicities. 
 *
 * In case the user wishes to completely hide the dimensions or the attributes,
 * he/she should include "__hide" as the only name in the dimension 
 * or attribute name list.
 *
 * Finally, the user may specify whether the array cells should be exported
 * in the stored order or in *reverse*.
 *
 * **Examples**
 *
 * Supposing that tiledb_context stores a pointer to a TileDB_Context object,
 * and assuming that the array has dimensions *dim1*,*dim2* and attributes
 * *attr1*,*attr2*,*attr3* :
 * - tiledb_export_csv(tiledb_context, "A", "A.csv", {}, 0, {}, 0, 0) \n
 *     Exports into "A.csv" all the cells of "A" in the order they are stored,
 *     including all the coordinates and attributes.
 * - tiledb_export(tiledb_context, "A", "A.csv", {"dim1"}, 1, {"attr1","attr2"},
 * 2, 0) \n
 *     Exports into "A.csv" all the cells of "A" in the order they are stored,
 *     including only the coordinates on the *dim1* dimension and the values
 *     on the *attr1*,*attr2* attributes.
 * - tiledb_export_csv(tiledb_context, "A", "A.csv", {"dim1"}, 1, 
 * {"attr1","attr2"}, 2, 1) \n
 *     Same as above, but the cells are exported in the reverse order.
 * - tiledb_export_csv(tiledb_context, "A", "A.csv", {"__hide"}, 1, 
 * {attr1,attr2}, 2, 0) \n
 *     Same as the first example, but no coordinate is exported.
 * - tiledb_export_csv(tiledb_context, "A", "A.csv", {"dim1"}, 1, {"__hide"}, 2,
 * 0) \n
 *     Same as the first example, but no attribute value is exported.
 * - tiledb_export_csv(tiledb_context, "A", "A.csv", {"dim1"}, 1, 
 * {"attr1","attr2","attr1"}, 2, 0) \n
 *     Same as the first example, but now the *attr1* values are shown twice
 *     (once before those of *attr2* and once after).
 *
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array whose data will be exported.
 * @param filename The name of the CSV file that will hold the output.
 * @param dim_names An array holding the dimension names to be exported. If it
 * is empty, then all the coordinates will be exported. If it contains special 
 * name "__hide", then no coordinates will be exported.
 * @param dim_names_num The number of elements in dim_names 
 * @param attribute_names An array holding the attribute names to be exported.
 * If it is empty, then all the attribute values will be exported. If it 
 * contains special name "__hide", then no attribute values will be
 * exported.
 * @param attribute_names_num The number of elements in attribute_names 
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 *   - **TILEDB_EOARR**\n
 *     Failed to open array
 *   - **TILEDB_ECARR**\n
 *     Failed to close array
 * @see TileDB_Context, tiledb_load_csv, 
 * tiledb_generate_data, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_export_csv(    
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    const char* filename,
    const char** dim_names,
    int dim_names_num,
    const char** attribute_names,
    int attribute_names_num,
    int reverse);

/** 
 * Generates a synthetic dataset in either **CSV** or **binary** form, which
 * is acceptable by a TileDB load command, namely tiledb_load_csv() or
 * tiledb_load_bin(), respectively. The dataset essentially consists of
 * a collection of cells, whose format complies with the schema of an
 * array given as input to the function. This means that the input
 * array must already be defined. The number of cells to be generated,
 * as well as the seed to the random generator, are given as inputs
 * to the function as well.
 *
 * **Data distribution**
 *
 *   - *Cooridinates* \n
 *        The coordinate values are drawn uniformly at random from their 
 *        corresponding dimension domains defined in the array schema.
 *   - *Attribute values* \n
 *        The attribute values depend on their type. The real data values (i.e.,
 *        **float** and **double**) are drawn uniformly at random from 
 *        [0.0,1.0]. The **int** and **int64_t** data values are drawn uniformly
 *        at random from [0,max()], where max() is the maximum value of the
 *        corresponding type supported by the system that calls the function. 
 *        Finally, the **char** values are drawn uniformly at random from ASCII
 *        domain [45,126], i.e., all the characters between and including '-' 
 *        and '~'.
 *
 * **CSV file format** \n
 * This is described in detail in tiledb_load_csv(). The difference is
 * that the generated data do not contain null values and deletions.
 *
 * **Binary file format** \n
 * This is described in detail in tiledb_load_bin(). The difference is
 * that the generated data do not contain null values and deletions.
 *
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array for which the data will be generated.
 * @param filename The name of the produced file.
 * @param filetype The type of the generated file. It can be 
 * either **csv** (CSV file) or **bin** (binary file).
 * @param seed The seed that will be used internally for the random generator.
 * @param cell_num The number of cells to be generated.
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 *   - **TILEDB_EIARG**\n
 *     Invalid argument
 * @see TileDB_Context, tiledb_load_csv, tiledb_load_bin, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_generate_data(    
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    const char* filename,
    const char* filetype,
    unsigned int seed,
    int64_t cell_num);

/** 
 * Loads a collection of binary files into an array. The user specifies
 * the path to a single binary file name, or to a directory containing a 
 * collection of binary files (all of which will be loaded into
 * the array). Moreover, the user may indicate whether the cells in each file
 * are sorted along the cell order defined in the array schema or not. This
 * has a dramatic effect on performance; if the cells are already sorted, then
 * the costly sorting operation upon loading is avoided and, hence, the load
 * performance is substantially improved. Note that, for the case of multiple
 * sorted files, loading is essentially a *merge* operation (the cells
 * are sorted in each file, but not necessarily across files). On error, it 
 * prints a message on stderr and returns an error code (see below).
 *
 * **Binary file format** \n 
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
 * the array schema (e.g., **int:3**), then these values are simply concatenated
 * next to each other. However, if the number of values is *variable*
 * (e.g, **int:var**), then the number of values must precede 
 * the actual attribute values, and it should be of type **int**. For example,
 * **3 | 0.1 | 0.2 | 0.3** for an attribute *attr1* whose type was defined as
 * **float:var** means that this cell stores **3** values on attribute 
 * *attr1*, namely **0.1,0.2,0.3** (more examples are provided below).
 * Moreover, if even a single attribute is variable-sized, the size of the
 * **entire** binary cell must be included immediately after the coordinates and
 * before the attributes, and it must be of type **size_t**
 * (i.e., **unsigned int**). Note that the cell size is essentially the size
 * of coordinates, plus the size of attributes, plus the size of a **size_t**
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
 * character '$'. 
 * 
 * **Example binary cells** \n 
 * Suppose that the array contains **2 dimensions** and **3 attributes**, whose
 * types are defined as **int:3,double,char,int64** (recall that the 
 * last type corresponds always to all coordinates collectively). Observe that 
 * this schema essentially defines a **fixed size** for *all* cells in the 
 * array.
 * - 1 | 3 | 10 | 11 | 12 | 0.1 | p \n
 *    (1,3) are the coordinates of the cell (of type **int64**). 
 *    (10,11,12) is the value on the *first* attribute (of type **int**). 
 *    **0.1**  is the value of the *second* attribute (of type **double**). 
 *    Finally, **p** is the value of the *third* attribute (of type **char**).
 *
 * Now suppose that the array contains **2 dimensions** and **3 attributes** as
 * in the previous example, but their types are now defined as
 * **int:3,double:var,char:var,int64**. This means that the cells of the array
 * may be of **variable size**. Also let an **int** value consume *4 bytes*,
 * a **size_t** *4 bytes*, a **double** *8 bytes*, 
 * a **char** *1 byte*, and a **int64** *8 bytes*.
 * - 1 | 3 | 60 | 10 | 11 | 12 | 2 | 0.1 | 0.2 | 4 | paok \n
 *    (1,3) are the coordinates of the cell (of type **int64**). **60**
 *    is the size in bytes of the entire cell (including even this size value 
 *    itself). (10,11,12) is the value on the *first* attribute (of type 
 *    **int**). **2** is the number of values for the *second* attribute (of 
 *    type **int**). (0.1,0.2) is the values of the *second* attribute (of type 
 *    **double**). **4** is the number of characters in the string of the 
 *    *third* attribute (of type **int**). Finally, **paok** is the string of 
 *    the *third* attribute (of type **char**), and my favorite soccer team in 
 *    Greece :P.
 *
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array into which the binary file(s) is 
 * loaded. Note that the array must already be defined.
 * @param path The path to a binary file or to a directory of binary files. 
 * If it is a file, then this single file will be loaded. If it is a directory,
 * **all** the files in the directory will be loaded.
 * @param sorted Indicates whether the cells in the binary file(s) are sorted 
 * along the cell order defined in the array schema. This choice will have a 
 * great effect on performance (sorted cells are loaded substantially faster). 
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 *   - **TILEDB_EOARR**\n
 *     Failed to open array
 *   - **TILEDB_ECARR**\n
 *     Failed to close array
 * @see TileDB_Context, tiledb_define_array, tiledb_export_csv, tiledb_load_csv,
 * tiledb_generate_data, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_load_bin( 
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    const char* path,
    int sorted);

/** 
 * Loads a collection of CSV files into an array. The user specifies
 * the path to a single CSV file name, or to a directory containing a 
 * collection of CSV files (all of which will be loaded into
 * the array). Moreover, the user may indicate whether the cells in each file
 * are sorted along the cell order defined in the array schema or not. This
 * has a dramatic effect on performance; if the cells are already sorted, then
 * the costly sorting operation upon loading is avoided and, hence, the load
 * performance is substantially improved. Note that, for the case of multiple
 * sorted files, loading is essentially a *merge* operation (the cells
 * are sorted in each file, but not necessarily across files). On error, it 
 * prints a message on stderr and returns an error code (see below).
 format.
 *
 * **CSV file format** \n 
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
 * the array schema (e.g., **int:3**), then these values are simply included
 * next to each other and separated by comma. However, if the number of values 
 * is *variable* (e.g, **int:var**), then the number of values must precede the
 * actual attribute values (e.g., "3,0.1,0.2,0.3" for an attribute *attr1* whose
 * type was defined as **float:var** means that this cell stores three values
 * on attribute *attr1*, namely 0.1, 0.2, and 0.3 (more examples are provided
 * below).
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
 * Suppose that the array contains **2 dimensions** and **3 attributes**, whose
 * types are defined as **int:2,double:var,char:var,int64** (recall that the 
 * last type corresponds always to all coordinates collectively).
 * - 1,3,10,11,2,0.1,0.2,paok \n
 *    (1,3) are the coordinates of the cell (of type int64). (10,11) is the 
 *    value on the first attribute (of type int). (0.1,0.2) is the value of the
 *    second attribute (of type double). Finally, "paok" is the value of the
 *    third attribute (of type char), and my favorite soccer team in Greece :P.
 * - 1,3,10,11,*,paok \n
 *    Same as above, but now the second attribute value is null.
 * - 1,3,$,$,$ \n
 *    %Cell (1,3) is being deleted.
 *

 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array into which the CSV file(s) are
 * loaded. Note that the array must already be defined.
 * @param path The path to a CSV file or to a directory of CSV files. 
 * If it is a file, then this single file will be loaded. If it is a directory,
 * **all** the files in the directory will be loaded.
 * @param sorted Indicates whether the cells in the CSV file(s) are sorted 
 * along the cell order defined in the array schema. This choice will have a 
 * great effect on performance (sorted cells are loaded substantially faster). 
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 *   - **TILEDB_EOARR**\n
 *     Failed to open array
 *   - **TILEDB_ECARR**\n
 *     Failed to close array
 * @see TileDB_Context, tiledb_define_array, tiledb_export_csv, tiledb_load_bin,
 * tiledb_generate_data, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_load_csv(    
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    const char* path,
    int sorted);

/** 
 * Prints the schema of an array on the standard output. The array must be
 * defined. On error, it prints a message on stderr and returns an error code
 * (shown below).
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array whose schema is printed.
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 * @see TileDB_Context, tiledb_define_array, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_show_array_schema(    
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name);

/** 
 * Creates a new array with the same schema as the input array (or
 * including a subset of the attributes in a potentially different
 * order), conataining only the cells that lie in the input range. 
 * The range must be a hyper-rectangle that is completely contained
 * in the dimension space. It is also given as a sequence of [low,high]
 * pairs across each dimension. On error, it prints a message on stderr and
 * returns an error code (shown below).
 *
 * **Examples**
 *
 * Supposing that tiledb_context stores a pointer to a TileDB_Context object,
 * and assuming that the array has dimensions *dim1*,*dim2* with domains
 * [0,100] and [0,200], respectively, and attributes *attr1*,*attr2*,*attr3* :
 * - tiledb_subarray(tiledb_context, "A", "A_sub", {10,20,100,150}, 4, {}, 0) \n
 *     Creates a new array "A_sub" with exactly the same schema as "A", but only
 *     containing the cells within rectangle [10,20] (*dim1*) , [100,150] 
 *     (*dim2).
 * - tiledb_subarray(tiledb_context, "A", "A_sub", {10,20,100,150}, 4, 
 *     {"attr1","attr2"}, 2) \n
 *     Same as the first example, but now "A_sub" contains only two attributes
 *     (*attr1*,*attr2*).
 * - tiledb_subarray(tiledb_context, "A", "A_sub", {10,20,100,150}, 4, 
 *     {"attr2","attr1"}, 2) \n
 *     Same as the second example, but *attr1* and *attr2* appear
 *     in a different order in "A_sub".
 *
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array the subarray will be applied on.
 * @param result_name The name of the output array.
 * @param range The range of the subarray. It must contain real values.
 * @param range_size The nunber of elements of the range vector. It must be
 * equal to 2*dim_num, where *dim_num* is the number of the dimensions of the
 * array.
 * @param attribute_names A vector holding the attribute names to be included
 * in the schema of the result array. If it is empty, then all the attributes
 * of the input array will appear in the output array.
 * @param attribute_names_num The number of elements in attribute_names 
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 *   - **TILEDB_EOARR**\n
 *     Failed to open array
 *   - **TILEDB_ECARR**\n
 *     Failed to close array
 * @see TileDB_Context, tiledb_define_array, tiledb_error.h
 */
TILEDB_EXPORT int tiledb_subarray(    
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    const char* result_name,
    const double* range,
    int range_size,
    const char** attribute_names,
    int attribute_names_num);

/**
 * This is very similar to tiledb_load_bin(). The difference is that the loaded
 * data create a new fragment in the array, which may be consolidated with
 * some or all of the existing ones. The update mechanism is described in 
 * detail in tiledb_define_array().
 *
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array into which the binary file(s) are 
 * loaded. Note that the array must already be defined.
 * @param path The path to a binary file or to a directory of binary files. 
 * If it is a file, then this single file will be loaded. If it is a directory,
 * **all** the files in the directory will be loaded.
 * @param sorted Indicates whether the cells in the binary file(s) are sorted 
 * along the cell order defined in the array schema. This choice will have a 
 * great effect on performance (sorted cells are loaded substantially faster). 
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 *   - **TILEDB_EOARR**\n
 *     Failed to open array
 *   - **TILEDB_ECARR**\n
 *     Failed to close array
 * @see TileDB_Context, tiledb_define_array, tiledb_export_csv, tiledb_load_bin,
 * tiledb_update_csv, tiledb_generate_data, tiledb_error.h
*/
TILEDB_EXPORT int tiledb_update_bin( 
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    const char* path,
    int sorted);

/**
 * This is very similar to tiledb_load_csv(). The difference is that the loaded
 * data create a new fragment in the array, which may be consolidated with
 * some or all of the existing ones. The update mechanism is described in 
 * detail in tiledb_define_array().
 *
 * @param tiledb_context The TileDB state consisting of the TileDB modules. 
 * @param array_name The name of the array into which the CSV file(s) are 
 * loaded. Note that the array must already be defined.
 * @param path The path to a binary file or to a directory of CSV files. 
 * If it is a file, then this single file will be loaded. If it is a directory,
 * **all** the files in the directory will be loaded.
 * @param sorted Indicates whether the cells in the CSV file(s) are sorted 
 * along the cell order defined in the array schema. This choice will have a 
 * great effect on performance (sorted cells are loaded substantially faster). 
 * @return An error code, which can be one of the following:
 *   - **TILEDB_OK**\n 
 *     Success
 *   - **TILEDB_ENDEFARR**\n
 *     Undefined array
 *   - **TILEDB_EFILE**\n
 *     File operation failed
 *   - **TILEDB_EOARR**\n
 *     Failed to open array
 *   - **TILEDB_ECARR**\n
 *     Failed to close array
 * @see TileDB_Context, tiledb_define_array, tiledb_export_csv, tiledb_load_csv,
 * tiledb_update_bin, tiledb_generate_data, tiledb_error.h
*/
TILEDB_EXPORT int tiledb_update_csv( 
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name,
    const char* path,
    int sorted);

#undef TILEDB_EXPORT
#ifdef __cplusplus
}
#endif

#endif
