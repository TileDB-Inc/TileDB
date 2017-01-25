/**
 * @file   fragment.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
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
 * This file defines class Fragment. 
 */

#ifndef __FRAGMENT_H__
#define __FRAGMENT_H__

#include "array.h"
#include "array_schema.h"
#include "book_keeping.h"
#include "read_state.h"
#include "write_state.h"
#include <vector>




/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_FG_OK        0
#define TILEDB_FG_ERR      -1
/**@}*/

/** Default error message. */
#define TILEDB_FG_ERRMSG std::string("[TileDB::Fragment] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
extern std::string tiledb_fg_errmsg;




class Array;
class BookKeeping;
class ReadState;
class WriteState;

/** Manages a TileDB fragment object. */
class Fragment {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  
  /** 
   * Constructor. 
   *
   * @param array The array the fragment belongs to.
   */
  Fragment(const Array* array);

  /** Destructor. */
  ~Fragment();




  /* ********************************* */
  /*              ACCESSORS            */
  /* ********************************* */
  
  /** Returns the array the fragment belongs to. */
  const Array* array() const;

  /** Returns the number of cell per (full) tile. */
  int64_t cell_num_per_tile() const;

  /** Returns true if the fragment is dense, and false if it is sparse. */
  bool dense() const;

  /** Returns the fragment name. */
  const std::string& fragment_name() const;

  /** Returns the mode of the fragment. */
  int mode() const;

  /** Returns true if the array is in read mode. */
  bool read_mode() const;

  /** Returns the read state of the fragment. */
  ReadState* read_state() const;

  /** 
   * Returns the tile size for a given attribute (TILEDB_VAR_SIZE in case
   * of a variable-sized attribute.
   */
  size_t tile_size(int attribute_id) const;

  /** Returns true if the array is in write mode. */
  bool write_mode() const;




  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /**
   * Finalizes the fragment, properly freeing up memory space.
   *
   * @return TILEDB_FG_OK on success and TILEDB_FG_ERR on error. 
   */
  int finalize();

  /**
   * Initializes a fragment in write mode.
   *
   * @param fragment_name The name that will be given to the fragment.
   * @param mode The fragment mode. It can be one of the following: 
   *    - TILEDB_WRITE 
   *    - TILEDB_WRITE_UNSORTED 
   * @param subarray The subarray the fragment is constrained on.
   * @return TILEDB_FG_OK on success and TILEDB_FG_ERR on error. 
   */
  int init(
      const std::string& fragment_name, 
      int mode,
      const void* subarray);

  /**
   * Initializes a fragment in read mode.
   *
   * @param fragment_name The name that will be given to the fragment.
   * @param book_keeping The book-keeping of the fragment.
   * @return TILEDB_FG_OK on success and TILEDB_FG_ERR on error. 
   */
  int init(
      const std::string& fragment_name, 
      BookKeeping* book_keeping);

  /** Resets the read state (typically to start a new read). */
  void reset_read_state();

  /**
   * Syncs all attribute files in the fragment.
   * 
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int sync();

  /**
   * Syncs the currently written files associated with the input attribute
   * in the input array. 
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int sync_attribute(const std::string& attribute);

  /**
   * Performs a write operation in the fragment. The cell values are provided
   * in a set of buffers (one per attribute specified upon initialization).
   * Note that there must be a one-to-one correspondance between the cell
   * values across the attribute buffers.
   *
   * The fragment must be initialized in one of the following write modes,
   * each of which having a different behaviour:
   *    - TILEDB_ARRAY_WRITE: \n
   *      In this mode, the cell values are provided in the buffers respecting
   *      the cell order on the disk. It is practically an **append** operation,
   *      where the provided cell values are simply written at the end of
   *      their corresponding attribute files. This mode leads to the best
   *      performance. The user may invoke this function an arbitrary number
   *      of times, and all the writes will occur in the same fragment. 
   *      Moreover, the buffers need not be synchronized, i.e., some buffers
   *      may have more cells than others when the function is invoked.
   *    - TILEDB_ARRAY_WRITE_UNSORTED: \n
   *      This mode is applicable to sparse arrays, or when writing sparse
   *      updates to a dense array. One of the buffers holds the coordinates.
   *      The cells in this mode are given in an arbitrary, unsorted order
   *      (i.e., without respecting how the cells must be stored on the disk
   *      according to the array schema definition). Each invocation of this
   *      function internally sorts the cells and writes them to the disk on the
   *      proper order. In addition, each invocation creates a **new** fragment.
   *      Finally, the buffers in each invocation must be synced, i.e., they
   *      must have the same number of cell values across all attributes.
   * 
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in Array::init()
   *     or Array::reset_attributes(). The case of variable-sized attributes is
   *     special. Instead of providing a single buffer for such an attribute,
   *     **two** must be provided: the second holds the variable-sized cell
   *     values, whereas the first holds the start offsets of each cell in the
   *     second buffer.
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @return TILEDB_FG_OK for success and TILEDB_FG_ERR for error.
   */
  int write(const void** buffers, const size_t* buffer_sizes);



   
 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The array the fragment belongs to. */
  const Array* array_;
  /** The fragment book-keeping. */
  BookKeeping* book_keeping_;
  /** Indicates whether the fragment is dense or sparse. */
  bool dense_;
  /** The fragment name. */
  std::string fragment_name_;
  /**
   * The fragment mode. It must be one of the following:
   *    - TILEDB_WRITE 
   *    - TILEDB_WRITE_UNSORTED 
   *    - TILEDB_READ 
   */
  int mode_;
  /** The fragment read state. */
  ReadState* read_state_;
  /** The fragment write state. */
  WriteState* write_state_;




  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** 
   * Changes the temporary fragment name into a stable one.
   *
   * @return TILEDB_FG_OK for success, and TILEDB_FG_ERR for error.
   */
  int rename_fragment();
};

#endif
