/**
 * @file   fragment.h
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
 * This file defines class Fragment.
 */

#ifndef TILEDB_FRAGMENT_H
#define TILEDB_FRAGMENT_H

#include "array_metadata.h"
#include "fragment_metadata.h"
#include "read_state.h"
#include "write_state.h"

#include <vector>

namespace tiledb {

class Query;
class FragmentMetadata;

/** Manages a TileDB fragment object for a particular query. */
class Fragment {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor for a particular query. */
  explicit Fragment(Query* query);

  /** Destructor. */
  ~Fragment();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the array metadata. */
  const ArrayMetadata* array_metadata() const;

  /** Returns the URI of the array the query focuses on. */
  URI attr_uri(unsigned int attribute_id) const;

  /** Returns a variable-sized attribute with the input id. */
  URI attr_var_uri(unsigned int attribute_id) const;

  /** Returns the URI of the coordinates file in this fragment. */
  URI coords_uri() const;

  /** Returns true if the fragment is dense, and false if it is sparse. */
  bool dense() const;

  /** Returns the size of the coordinates file. */
  uint64_t file_coords_size() const;

  /** Returns the size of the file of attribute with the input id. */
  uint64_t file_size(unsigned int attribute_id) const;

  /** Returns the size of the file of variable attribute with the input id. */
  uint64_t file_var_size(unsigned int attribute_id) const;

  /**
   * Finalizes the fragment, properly freeing up memory space.
   *
   * @return Status
   */
  Status finalize();

  /** Returns the fragment uri. */
  const URI& fragment_uri() const;

  /**
   * Initializes the fragment.
   *
   * @param uri The URI of the fragment directory.
   * @param subarray The subarray this fragment is constrained on. A *nullptr*
   *     implies the entire domain.
   * @param consolidation If true, then this fragment is the result of
   *     consolidation and, thus, it should not be renamed upon finalization.
   * @return Status
   */
  Status init(
      const URI& uri,
      const void* subarray = nullptr,
      bool consolidation = false);

  /**
   * Initializes the fragment.
   *
   * @param uri The URI of the fragment directory.
   * @param metadata The fragment metadata.
   * @return Status
   */
  Status init(const URI& uri, FragmentMetadata* metadata);

  /** Returns the fragment metadata. */
  FragmentMetadata* metadata() const;

  /** Returns the query that this fragment belongs to. */
  Query* query() const;

  /** Returns the read state of the fragment. */
  ReadState* read_state() const;

  /** Returns the tile size for a given attribute. */
  uint64_t tile_size(unsigned int attribute_id) const;

  /**
   * Writes a set of buffers into the write state. The buffers have a
   * one-to-one correspondence with the attributes specified in the
   * query.
   *
   * @param buffers The buffers to be written.
   * @param buffer_sizes The corresponding buffer sizes.
   * @return Status
   */
  Status write(void** buffers, uint64_t* buffer_sizes);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /**
   * If true, then this fragment is the result of consolidation and, thus, it
   * should not be renamed upon finalization.
   */
  bool consolidation_;

  /** Indicates whether the fragment is dense or sparse. */
  bool dense_;

  /** The fragment URI. */
  URI fragment_uri_;

  /** The fragment metadata. */
  FragmentMetadata* metadata_;

  /** The query this fragment belongs to. */
  Query* query_;

  /** The fragment read state. */
  ReadState* read_state_;

  /** The fragment write state. */
  WriteState* write_state_;
};

}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_H
