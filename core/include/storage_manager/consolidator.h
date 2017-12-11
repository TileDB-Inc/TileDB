/**
 * @file   consolidator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "status.h"

#include <vector>

namespace tiledb {

class ArrayMetadata;
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
   * (with *query_r*) and writing to the new fragment (with *query_w*).
   *
   * @param query_r The read query.
   * @param query_w The write query.
   * @return Status
   */
  Status copy_array(Query* query_r, Query* query_w);

  /**
   * Creates the buffers that will be used upon reading the input fragments and
   * writing into the new fragment. It also retrieves the number of buffers
   * created.
   *
   * @param array_meta The array metadata.
   * @param buffers The buffers to be created.
   * @param buffer_sizes The corresponding buffer sizes.
   * @param buffer_num The number of buffers to be retrieved.
   * @return Status
   */
  Status create_buffers(
      ArrayMetadata* array_meta,
      void*** buffers,
      uint64_t** buffer_sizes,
      unsigned int* buffer_num);

  /**
   * Creates the queries needed for consolidation. It also retrieves
   * the number of fragments to be consolidated.
   *
   * @param query_r This query reads from the fragments to be consolidated.
   * @param query_w This query writes to the new consolidated fragment.
   * @param array_name The array name.
   * @param buffers The buffers to be passed in the queries.
   * @param buffer_sizes The corresponding buffer sizes.
   * @param fragment_num The number of fragments to be retrieved.
   * @return Status
   */
  Status create_queries(
      Query* query_r,
      Query* query_w,
      const char* array_name,
      void** buffers,
      uint64_t* buffer_sizes,
      unsigned int* fragment_num);

  /**
   * Deletes the old fragments that got consolidated.
   * @param uris The URIs of the old fragments.
   * @return Status
   */
  Status delete_old_fragments(const std::vector<URI>& uris);

  /** Finalizes the input queries. */
  Status finalize_queries(Query* query_r, Query* query_w);

  /**
   * Frees the input buffers.
   *
   * @param buffer_num The number of buffers.
   * @param buffers The buffers to be freed.
   * @param buffer_sizes The corresponding buffer sizes.
   */
  void free_buffers(
      unsigned int buffer_num, void** buffers, uint64_t* buffer_sizes);

  /**
   * Renames the new fragment URI. If the working thread id is different
   * from the one that created the input URI, the function does nothing.
   * Otherwise, it appends an extra `_` after the thread id.
   */
  Status rename_new_fragment_uri(URI* uri) const;
};

}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_H
