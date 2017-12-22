/**
 * @file   open_array.h
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
 * This file defines class OpenArray.
 */

#ifndef TILEDB_OPEN_ARRAY_H
#define TILEDB_OPEN_ARRAY_H

#include <map>
#include <mutex>
#include <vector>

#include "array_metadata.h"
#include "fragment_metadata.h"
#include "uri.h"

namespace tiledb {

/** Stores information about an open array. */
class OpenArray {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  OpenArray();

  /** Destructor. */
  ~OpenArray();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /** Returns the array metadata. */
  const ArrayMetadata* array_metadata() const;

  /** Returns the array URI. */
  const URI& array_uri() const;

  /** Decrements the counter indicating the times this array has been opened. */
  void decr_cnt();

  /** Returns the open array query counter. */
  uint64_t cnt() const;

  /** Adds a new entry to the fragment metadata map. */
  void fragment_metadata_add(FragmentMetadata* metadata);

  /**
   * Returns the stored metadata for a particular fragment uri (nullptr if not
   * found). If found, it also increments the respective counter of the
   * metadata.
   */
  FragmentMetadata* fragment_metadata_get(const URI& fragment_uri);

  /** Increments the counter indicating the times this array has been opened. */
  void incr_cnt();

  /** Locks the array mutex. */
  void mtx_lock();

  /** Unlocks the array mutex. */
  void mtx_unlock();

  /** Sets an array metadata. */
  void set_array_metadata(const ArrayMetadata* array_metadata);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array metadata. */
  const ArrayMetadata* array_metadata_;

  /** Counts the number of queries that opened the array. */
  uint64_t cnt_;

  /**
   * Enables searching for loaded fragment metadata by fragment name.
   * Format: <fragment_name> --> (fragment_metadata)
   */
  std::map<std::string, FragmentMetadata*> fragment_metadata_;

  /**
   * A mutex used to lock the array when loading the array metadata and
   * any fragment metadata structures from the disk.
   */
  std::mutex mtx_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_H
