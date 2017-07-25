/**
 * @file   array.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class Array.
 */

#ifndef TILEDB_ARRAY_H
#define TILEDB_ARRAY_H

#include <queue>
#include "array_schema.h"
#include "attribute_buffer.h"
#include "configurator.h"
#include "dimension_buffer.h"
#include "fragment_metadata.h"

namespace tiledb {

class Query;
class StorageManager;

/** Manages a TileDB array object. */
class Array {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Array(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /** Destructor. */
  ~Array();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  Status query_finalize(Query* query) const;

  Status query_init(Query* query) const;

  Status query_process(Query* query) const;

  void set_array_schema(const ArraySchema* array_schema);

  /** Handles an AIO request. */
  //  Status aio_handle_request(AIORequest* aio_request);

  /** Returns the storage manager object. */
  //  StorageManager* storage_manager() const;

  /** Returns the array clone. */
  //  Array* array_clone() const;

  /** Returns the ids of the attributes the array focuses on. */
  //  const std::vector<int>& attribute_ids() const;

  /** Returns the configuration parameters. */
  //  const Configurator* config() const;

  /** Returns the number of fragments in this array. */
  //  int fragment_num() const;

  /** Returns the fragment objects of this array. */
  //  std::vector<Fragment*> fragments() const;

  /** Returns the array mode. */
  //  ArrayMode mode() const;

  /**
   * Checks if *at least one* attribute buffer has overflown during a read
   * operation.
   *
   * @return *true* if at least one attribute buffer has overflown and *false*
   * otherwise.
   */
  //  bool overflow() const;

  /**
   * Checks if an attribute buffer has overflown during a read operation.
   *
   * @param attribute_id The id of the attribute that is being checked.
   * @return *true* if the attribute buffer has overflown and *false* otherwise.
   */
  //  bool overflow(int attribute_id) const;

  //  Status read(void** buffers, size_t* buffer_sizes);

  //  Status read_default(void** buffers, size_t* buffer_sizes);

  /** Returns true if the array is in read mode. */
  //  bool read_mode() const;

  /** Returns true if the array is in write mode. */
  //  bool write_mode() const;

  //  Status consolidate(
  //      Fragment*& new_fragment, std::vector<std::string>&
  //      old_fragment_names);

  /**
   * Consolidates all fragment into a new single one, focusing on a specific
   * attribute.
   *
   * @param new_fragment The new consolidated fragment object.
   * @param attribute_id The id of the target attribute.
   */
  //  Status consolidate(Fragment* new_fragment, int attribute_id);

  /**
   * Finalizes the array, properly freeing up memory space.
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  //  Status finalize();

  /**
   * Syncs all currently written files in the input array.
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  //  Status sync();

  /**
   * Syncs the currently written files associated with the input attribute
   * in the input array.
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  //  Status sync_attribute(const std::string& attribute);

  //  Status write(const void** buffers, const size_t* buffer_sizes);

  //  Status write_default(const void** buffers, const size_t* buffer_sizes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array schema. */
  const ArraySchema* array_schema_;

  const std::vector<FragmentMetadata*>& fragment_metadata_;

  StorageManager* storage_manager_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  std::string new_temp_fragment_name() const;

  Status read(Query* query) const;

  Status read_dense(Query* query) const;

  Status read_sparse(Query* query) const;

  Status read_sorted(Query* query) const;

  Status read_sorted_dense(Query* query) const;

  Status read_sorted_sparse(Query* query) const;

  Status rename_fragment(const std::string& temp_fragment_name) const;

  Status write(Query* query) const;

  Status write_dense(Query* query) const;

  Status write_dense(Query* query, const AttributeBuffer* abuf) const;

  Status write_sparse(Query* query) const;

  Status write_sparse(Query* query, const AttributeBuffer* abuf) const;

  Status write_sparse(Query* query, const DimensionBuffer* dbuf) const;

  Status write_sorted(Query* query) const;

  Status write_sorted_dense(Query* query) const;

  Status write_sorted_dense(Query* query, const AttributeBuffer* abuf) const;

  Status write_sorted_sparse(Query* query) const;

  Status write_unsorted(Query* query) const;

  Status write_unsorted_dense(Query* query) const;

  Status write_unsorted_sparse(Query* query) const;

  /**
   * Returns a new fragment name, which is in the form: <br>
   * .__MAC-address_thread-id_timestamp. For instance,
   *  __00332a0b8c6426153_1458759561320
   *
   * Note that this is a temporary name, initiated by a new write process.
   * After the new fragmemt is finalized, the array will change its name
   * by removing the leading '.' character.
   *
   * @return A new special fragment name on success, or "" (empty string) on
   *     error.
   */
  //  std::string new_fragment_name() const;

  /**
   * Opens the existing fragments.
   *
   * @param fragment_names The vector with the fragment names.
   * @param book_keeping The book-keeping of the array fragments.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  //  Status open_fragments(
  //      const std::vector<std::string>& fragment_names,
  //      const std::vector<BookKeeping*>& book_keeping);
};

}  // namespace tiledb

#endif  // TILEDB_ARRAY_H
