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

#include "array_read_state.h"
#include "array_schema.h"
#include "array_sorted_read_state.h"
#include "array_sorted_write_state.h"
#include "book_keeping.h"
#include "configurator.h"
#include "fragment.h"
#include "query.h"

namespace tiledb {

class AIORequest;
class ArrayReadState;
class ArraySortedReadState;
class ArraySortedWriteState;
class Fragment;
class Query;
class StorageManager;

/** Manages a TileDB array object. */
class Array {
 public:
  // TODO: will be remove from here
  Query* query_;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Array();

  /** Destructor. */
  ~Array();

  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  Status coords_buffer_i(int* coords_buffer_i) const;

  /** Handles an AIO request. */
  Status aio_handle_request(AIORequest* aio_request);

  /** Returns the storage manager object. */
  StorageManager* storage_manager() const;

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /** Returns the array clone. */
  Array* array_clone() const;

  /** Returns the ids of the attributes the array focuses on. */
  const std::vector<int>& attribute_ids() const;

  /** Returns the configuration parameters. */
  const Configurator* config() const;

  /** Returns the number of fragments in this array. */
  int fragment_num() const;

  /** Returns the fragment objects of this array. */
  std::vector<Fragment*> fragments() const;

  bool overflow() const;

  bool overflow(int attribute_id) const;

  Status read(void** buffers, size_t* buffer_sizes);

  Status read_default(void** buffers, size_t* buffer_sizes);

  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  Status consolidate(
      Fragment*& new_fragment, std::vector<uri::URI>* old_fragments);

  Status consolidate(Fragment* new_fragment, int attribute_id);

  Status finalize();

  Status init(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      const std::vector<std::string>& fragment_names,
      const std::vector<BookKeeping*>& book_keeping,
      QueryMode mode,
      const char** attributes,
      int attribute_num,
      const void* subarray,
      const Configurator* config,
      Array* array_clone = nullptr);

  Status reset_subarray(const void* subarray);

  Status reset_subarray_soft(const void* subarray);

  Status sync();

  Status sync_attribute(const std::string& attribute);

  Status write(const void** buffers, const size_t* buffer_sizes);

  Status write_default(const void** buffers, const size_t* buffer_sizes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  StorageManager* storage_manager_;

  /** Stores the id of the last handled AIO request. */
  uint64_t aio_last_handled_request_;

  /** An array clone, used in AIO requests. */
  Array* array_clone_;

  /** The array schema. */
  const ArraySchema* array_schema_;

  /** The read state of the array. */
  ArrayReadState* array_read_state_;

  /** The sorted read state of the array. */
  ArraySortedReadState* array_sorted_read_state_;

  /** The sorted write state of the array. */
  ArraySortedWriteState* array_sorted_write_state_;

  /**
   * The ids of the attributes the array is initialized with. Note that the
   * array may be initialized with a subset of attributes when writing or
   * reading.
   */
  std::vector<int> attribute_ids_;

  /** Configuration parameters. */
  const Configurator* config_;

  /** The array fragments. */
  std::vector<Fragment*> fragments_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

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
  std::string new_fragment_name() const;

  /**
   * Opens the existing fragments.
   *
   * @param fragment_names The vector with the fragment names.
   * @param book_keeping The book-keeping of the array fragments.
   * @return TILEDB_AR_OK for success and TILEDB_AR_ERR for error.
   */
  Status open_fragments(
      const std::vector<std::string>& fragment_names,
      const std::vector<BookKeeping*>& book_keeping);
};

}  // namespace tiledb

#endif  // TILEDB_ARRAY_H
