/**
 * @file   query.h
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
 * This file defines class Query.
 */

#ifndef TILEDB_QUERY_H
#define TILEDB_QUERY_H

#include "array_read_state.h"
#include "array_sorted_read_state.h"
#include "array_sorted_write_state.h"
#include "fragment.h"
#include "query_mode.h"
#include "query_status.h"
#include "status.h"
#include "storage_manager.h"

#include <vector>

namespace tiledb {

class Array;
class ArrayReadState;
class ArraySortedReadState;
class ArraySortedWriteState;
class Fragment;

class Query {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Query();

  ~Query();

  Query(const Query* query, QueryMode mode, const void* subarray);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  Status async_process();

  void clear_fragments();

  // TODO: remove
  ArrayReadState* array_read_state() const {
    return array_read_state_;
  }

  const ArraySchema* array_schema() const {
    return array_schema_;
  }

  const std::vector<int>& attribute_ids() const;

  const std::vector<Fragment*>& fragments() const {
    return fragments_;
  }

  const std::vector<FragmentMetadata*>& fragment_metadata() const {
    return fragment_metadata_;
  }

  StorageManager* storage_manager() const {
    return storage_manager_;
  }

  Status coords_buffer_i(int* coords_buffer_i) const;

  int fragment_num() const;

  bool overflow() const;

  bool overflow(int attribute_id) const;

  Status read();

  void set_buffers(void** buffers, uint64_t* buffer_sizes) {
    buffers_ = buffers;
    buffer_sizes_ = buffer_sizes;
  }

  Status write();

  Status init(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_metadata,
      QueryMode mode,
      const void* subarray,
      const char** attributes,
      int attribute_num,
      void** buffers,
      uint64_t* buffer_sizes);

  Status init(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_metadata,
      QueryMode mode,
      const void* subarray,
      const std::vector<int>& attribute_ids,
      void** buffers,
      uint64_t* buffer_sizes);

  /** Returns the query mode. */
  QueryMode mode() const;

  /** Returns the subarray in which the query is constrained. */
  const void* subarray() const;

  Status write_default(const void** buffers, const uint64_t* buffer_sizes);

  void add_coords();

  void set_mode(QueryMode mode) {
    mode_ = mode;
  }

  void set_status(QueryStatus status) {
    status_ = status;
  }

  void set_callback(void* (*callback)(void*), void* callback_data) {
    callback_ = callback;
    callback_data_ = callback_data;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  URI array_name_;
  const ArraySchema* array_schema_;
  QueryMode mode_;
  QueryStatus status_;
  void* subarray_;
  uint64_t subarray_size_;
  std::vector<std::string> attributes_;
  void** buffers_;
  uint64_t* buffer_sizes_;
  StorageManager* storage_manager_;

  std::vector<int> attribute_ids_;

  ArrayReadState* array_read_state_;
  ArraySortedReadState* array_sorted_read_state_;
  ArraySortedWriteState* array_sorted_write_state_;
  std::vector<Fragment*> fragments_;
  std::vector<FragmentMetadata*> fragment_metadata_;
  void* (*callback_)(void*);
  void* callback_data_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  Status set_subarray(const void* subarray);

  Status set_attributes(const char** attributes, int attribute_num);

  Status init_states();
  Status init_fragments(
      const std::vector<FragmentMetadata*>& fragment_metadata);
  Status new_fragment();

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
   * @param metadata The metadata of the array fragments.
   * @return Status
   */
  Status open_fragments(const std::vector<FragmentMetadata*>& metadata);
};

}  // namespace tiledb

#endif  // TILEDB_QUERY_H
