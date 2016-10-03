/**
 * @file   aio_request.h
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
 * This file declares the AIO_Request struct. 
 */

#ifndef __AIO_REQUEST_H__
#define __AIO_REQUEST_H__

#include <stdio.h>

/** Describes an AIO (read or write) request. */
struct AIO_Request {
  /**
   * An array of buffers, one for each attribute. These must be
   * provided in the same order as the attributes specified in
   * array initialization or when resetting the attributes. The case of
   * variable-sized attributes is special. Instead of providing a single
   * buffer for such an attribute, **two** must be provided: the second
   * will hold the variable-sized cell values, whereas the first holds the
   * start offsets of each cell in the second buffer.
   */
  void** buffers_;
  /**
   * The sizes (in bytes) allocated by the user for the input
   * buffers (there is a one-to-one correspondence). The function will attempt
   * to write as many results as can fit in the buffers, and potentially
   * alter the buffer size to indicate the size of the *useful* data written
   * in the buffer. 
   */
  size_t* buffer_sizes_;
  /** Function to be called upon completion of the request. */
  void *(*completion_handle_) (void*);
  /** Data to be passed to the completion handle. */
  void* completion_data_;
  /** A unique request id. */
  size_t id_;
  /** 
   * It can be one of the following:
   *    - TILEDB_ARRAY_READ 
   *    - TILEDB_ARRAY_READ_SORTED_COL
   *    - TILEDB_ARRAY_READ_SORTED_ROW
   *    - TILEDB_ARRAY_WRITE
   *    - TILEDB_ARRAY_WRITE_UNSORTED
   */
  int mode_;
  /** 
   * Applicable only to read requests.
   * Indicates whether a buffer has overflowed during a read request.
   * If it is NULL, it will be ignored. Otherwise, it must be an array
   * with as many elements as the number of buffers above. 
   */
  bool* overflow_;
  /**
   * The status of the AIO request. It can be one of the following:
   *    - TILEDB_AIO_COMPLETED
   *      The request is completed.
   *    - TILEDB_AIO_INPROGRESS
   *      The request is still in progress.
   *    - TILEDB_AIO_OVERFLOW
   *      At least one of the input buffers overflowed (applicable only to AIO 
   *      read requests)
   *    - TILEDB_AIO_ERR 
   *      The request caused an error (and thus was canceled). 
   */
  int* status_;
  /** 
   * The subarray in which the array read/write will be
   * constrained on. It should be a sequence of [low, high] pairs (one 
   * pair per dimension), whose type should be the same as that of the
   * coordinates. If it is NULL, then the subarray is set to the entire
   * array domain. For the case of writes, this is meaningful only for
   * dense arrays, and specifically dense writes.
   */
  const void* subarray_;
};

#endif

