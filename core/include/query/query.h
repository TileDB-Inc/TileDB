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

#include "array.h"
#include "array_read_state.h"
#include "array_sorted_read_state.h"
#include "array_sorted_write_state.h"
#include "fragment.h"
#include "query_mode.h"
#include "status.h"

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

  Query(const Query* query, QueryMode mode, const void* subarray);

  ~Query();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  Status open_fragments(const std::vector<Fragment*>& fragments);

  void clear_fragments();

  Array* array() {
    return array_;
  }

  ArrayReadState* array_read_state() {
    return array_read_state_;
  }

  ArraySortedReadState* array_sorted_read_state() {
    return array_sorted_read_state_;
  }

  ArraySortedWriteState* array_sorted_write_state() {
    return array_sorted_write_state_;
  }

  const std::vector<int>& attribute_ids() const;

  Status coords_buffer_i(int* coords_buffer_i) const;

  int fragment_num() const;

  std::vector<Fragment*> fragments() const;

  bool overflow() const;

  bool overflow(int attribute_id) const;

  Status init(
      Array* array,
      QueryMode mode,
      const void* subarray,
      const char** attributes,
      int attribute_num,
      const std::vector<std::string>& fragment_names,
      const std::vector<FragmentMetadata*>& book_keeping);

  /** Returns the query mode. */
  QueryMode mode() const;

  /** Returns the subarray in which the query is constrained. */
  const void* subarray() const;

  Status write_default(const void** buffers, const size_t* buffer_sizes);

  void add_coords();

  void set_mode(QueryMode mode) {
    mode_ = mode;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  Array* array_;

  std::vector<int> attribute_ids_;

  QueryMode mode_;

  void* subarray_;

  ArrayReadState* array_read_state_;
  ArraySortedReadState* array_sorted_read_state_;
  ArraySortedWriteState* array_sorted_write_state_;
  std::vector<Fragment*> fragments_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  Status set_subarray(const void* subarray);

  Status set_attributes(const char** attributes, int attribute_num);

  Status init_states();
  Status init_fragments(
      const std::vector<std::string>& fragment_names,
      const std::vector<FragmentMetadata*>& bookkeeping);
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
   * @param fragment_names The vector with the fragment names.
   * @param metadata The metadata of the array fragments.
   * @return Status
   */
  Status open_fragments(
      const std::vector<std::string>& fragment_names,
      const std::vector<FragmentMetadata*>& metadata);
};

}  // namespace tiledb

#endif  // TILEDB_QUERY_H
