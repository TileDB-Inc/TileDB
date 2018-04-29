/**
 * @file   query.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class Query.
 */

#ifndef TILEDB_QUERY_H
#define TILEDB_QUERY_H

#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/dense_cell_range_iter.h"
#include "tiledb/sm/query/reader.h"
#include "tiledb/sm/query/writer.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <functional>
#include <vector>

namespace tiledb {
namespace sm {

/** Processes a (read/write) query. */
class Query {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  explicit Query(QueryType type);

  /** Destructor. */
  ~Query();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /**
   * Computes a vector of `subarrays` into which `subarray` must be partitioned,
   * such that each subarray in `subarrays` can be safely answered by the
   * query without a memory overflow.
   *
   * @param subarray The input subarray.
   * @param subarrays The vector of subarray partitions to be retrieved.
   * @return Status
   */
  Status compute_subarrays(void* subarray, std::vector<void*>* subarrays) const;

  /**
   * Finalizes the query, properly finalizing and deleting the involved
   * fragments.
   */
  Status finalize();

  /** Returns the number of fragments involved in the (read) query. */
  unsigned fragment_num() const;

  /** Returns a vector with the fragment URIs. */
  std::vector<URI> fragment_uris() const;

  /** Initializes the query. */
  Status init();

  /** Returns the last fragment uri. */
  URI last_fragment_uri() const;

  /** Returns the cell layout. */
  Layout layout() const;

  /** Processes a query. */
  Status process();

  /** Sets the array schema. */
  void set_array_schema(const ArraySchema* array_schema);

  /**
   * Sets the buffers to the query for a set of attributes.
   *
   * @param attributes The attributes the query will focus on.
   * @param attribute_num The number of attributes.
   * @param buffers The buffers that either have the input data to be written,
   *     or will hold the data to be read. Note that there is one buffer per
   *     fixed-sized attribute, and two buffers for each variable-sized
   *     attribute (the first holds the offsets, and the second the actual
   *     values).
   * @param buffer_sizes There must be an one-to-one correspondence with
   *     *buffers*. In the case of writes, they contain the sizes of *buffers*.
   *     In the case of reads, they initially contain the allocated sizes of
   *     *buffers*, but after the termination of the function they will contain
   *     the sizes of the useful (read) data in the buffers.
   * @return Status
   */
  Status set_buffers(
      const char** attributes,
      unsigned int attribute_num,
      void** buffers,
      uint64_t* buffer_sizes);

  /** Sets the query buffers. */
  void set_buffers(void** buffers, uint64_t* buffer_sizes);

  /**
   * Sets the callback function and its data input that will be called
   * upon the completion of an asynchronous query.
   */
  void set_callback(
      const std::function<void(void*)>& callback, void* callback_data);

  /** Sets the fragment metadata. */
  void set_fragment_metadata(
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
   */
  Status set_layout(Layout layout);

  /** Sets the storage manager. */
  void set_storage_manager(StorageManager* storage_manager);

  /**
   * Sets the query subarray. If it is null, then the subarray will be set to
   * the entire domain.
   *
   * @param subarray The subarray to be set.
   * @return Status
   */
  Status set_subarray(const void* subarray);

  /** Returns the query status. */
  QueryStatus status() const;

  /** Returns the query type. */
  QueryType type() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** A function that will be called upon the completion of an async query. */
  std::function<void(void*)> callback_;

  /** The data input to the callback function. */
  void* callback_data_;

  /** The query status. */
  QueryStatus status_;

  /** The query type. */
  QueryType type_;

  /** Query reader. */
  Reader reader_;

  /** Wuery writer. */
  Writer writer_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_H
