/**
 * @file   consolidator.h
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
 * This file defines class Consolidator.
 */

#ifndef TILEDB_CONSOLIDATOR_H
#define TILEDB_CONSOLIDATOR_H

#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/storage_manager/open_array.h"

#include <vector>

namespace tiledb {
namespace sm {

class ArraySchema;
class Query;
class StorageManager;
class URI;

/** Handles array consolidation. */
class Consolidator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param storage_manager The storage manager.
   */
  Consolidator(StorageManager* storage_manager);

  /** Destructor. */
  ~Consolidator();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Consolidates the fragments of the input array. */
  Status consolidate(const char* array_name);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The storage manager. */
  StorageManager* storage_manager_;

  /* ********************************* */
  /*          PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Copies the array by reading from the fragments to be consolidated
   * (with `query_r`) and writing to the new fragment (with `query_w`).
   *
   * @param query_r The read query.
   * @param query_w The write query.
   * @return Status
   */
  Status copy_array(Query* query_r, Query* query_w);

  /** Cleans up the inputs. */
  void clean_up(
      void* subarray,
      unsigned buffer_num,
      void** buffers,
      uint64_t* buffer_sizes,
      Query* query_r,
      Query* query_w) const;

  /**
   * Creates the buffers that will be used upon reading the input fragments and
   * writing into the new fragment. It also retrieves the number of buffers
   * created.
   *
   * @param array_schema The array schema.
   * @param buffers The buffers to be created.
   * @param buffer_sizes The corresponding buffer sizes.
   * @param buffer_num The number of buffers to be retrieved.
   * @return Status
   */
  Status create_buffers(
      const ArraySchema* array_schema,
      void*** buffers,
      uint64_t** buffer_sizes,
      unsigned int* buffer_num);

  /**
   * Creates the queries needed for consolidation. It also retrieves
   * the number of fragments to be consolidated and the URI of the
   * new fragment to be created.
   *
   * @param query_r This query reads from the fragments to be consolidated.
   * @param query_w This query writes to the new consolidated fragment.
   * @param write_subarray The subarray to write into.
   * @param open_array_for_reads The opened array for reading the fragments
   *     to be consolidated.
   * @param open_array_for_writes The opened array for writing the
   *     consolidated fragments.
   * @param snapshot The snapshot at which `open_array_for_reads_` got
   *     opened.
   * @param buffers The buffers to be passed in the queries.
   * @param buffer_sizes The corresponding buffer sizes.
   * @param fragment_num The number of fragments to be retrieved.
   * @param new_fragment_uri The URI of the new fragment to be created.
   * @return Status
   */
  Status create_queries(
      Query** query_r,
      Query** query_w,
      void* write_subarray,
      OpenArray* open_array_for_reads,
      OpenArray* open_array_for_writes,
      uint64_t snapshot,
      void** buffers,
      uint64_t* buffer_sizes,
      unsigned int* fragment_num,
      URI* new_fragment_uri);

  /** Creates the subarray that should represent the entire array domain. */
  Status create_subarray(
      OpenArray* open_array, uint64_t snapshot, void** subarray) const;

  /**
   * Deletes the fragment metadata files of the old fragments that
   * got consolidated. This renders the old fragments "invisible".
   *
   * @param uris The URIs of the old fragments.
   * @return Status
   */
  Status delete_old_fragment_metadata(const std::vector<URI>& uris);

  /**
   * Deletes the old fragments that got consolidated.
   *
   * @param uris The URIs of the old fragments.
   * @return Status
   */
  Status delete_old_fragments(const std::vector<URI>& uris);

  /**
   * Frees the input buffers.
   *
   * @param buffer_num The number of buffers.
   * @param buffers The buffers to be freed.
   * @param buffer_sizes The corresponding buffer sizes.
   */
  void free_buffers(
      unsigned int buffer_num, void** buffers, uint64_t* buffer_sizes) const;

  /**
   * Renames the new fragment URI. The new name has the format
   * `__<thread_id>_<timestamp>_<last_fragment_timestamp>`, where
   * `<thread_id>` is the id of the thread that performs the consolidation,
   * `<timestamp>` is the current timestamp in milliseconds, and
   * `<last_fragment_timestamp>` is the timestamp of the last of the
   * consolidated fragments.
   */
  Status rename_new_fragment_uri(URI* uri) const;

  /**
   * Sets the buffers to the query, using all the attributes in the
   * query schema. There is a 1-1 correspondence between the input `buffers`
   * and the attributes in the schema, considering also the coordinates
   * if the array is sparse in the end.
   */
  Status set_query_buffers(
      Query* query, void** buffers, uint64_t* buffer_sizes) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_H
