/**
 * @file   consistency.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines classes ConsistencyController and ConsistencySentry.
 */

#ifndef TILEDB_CONSISTENCY_H
#define TILEDB_CONSISTENCY_H

#include <iterator>
#include <map>

#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/filesystem/uri.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class ConsistencyController;
class ConsistencySentry;
class WhiteboxConsistencyController;

using entry_type = std::multimap<const URI, Array&>::const_iterator;

/**
 * Tracks the open arrays on disk, considering that a given
 * URI can have multiple open arrays.
 */
class ConsistencyController {
  /**
   * WhiteboxConsistencyController makes available internals of
   * ConsistencyController for testing.
   */
  friend class WhiteboxConsistencyController;

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ConsistencyController() = default;

  /** Copy Constructor is deleted. */
  ConsistencyController(const ConsistencyController&) = delete;

  /** Move Constructor is deleted. */
  ConsistencyController(ConsistencyController&&) = delete;

  /** Copy assignment is deleted. */
  ConsistencyController& operator=(const ConsistencyController&) = delete;

  /** Move assignment is deleted. */
  ConsistencyController& operator=(ConsistencyController&&) = delete;

  /** Destructor. */
  ~ConsistencyController() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Create the sentry object for the Array. */
  ConsistencySentry make_sentry(const URI uri, Array& array);

  /** Returns the iterator to the multimap after adding an entry. */
  entry_type register_array(const URI uri, Array& array);

  /** Takes an entry out of the multimap. */
  void deregister_array(entry_type entry);

  /** Returns true if the array is open, i.e. registered in the multimap. */
  bool contains(const URI uri);

  /**
   * Returns true if the given URIs have the same "prefix" and could
   * potentially intersect one another.
   * i.e. The second URI is an element of the first (or vice versa).
   *
   * Note: The order of the arguments does not matter;
   * the API is checking for working tree intersection.
   *
   **/
  bool is_element_of(const URI uri, const URI intersecting_uri);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The open array registry. */
  std::multimap<const URI, Array&> array_registry_;

  /** Mutex for sentry class. */
  std::mutex mtx_;
};

/**
 * Sentry class for ConsistencyController
 */
class ConsistencySentry {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ConsistencySentry(ConsistencyController& registry, entry_type entry);

  /** Destructor. */
  ~ConsistencySentry();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The ConsistencyController instance. */
  ConsistencyController& array_controller_registry_;

  /** The map iterator. */
  entry_type entry_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONSISTENCY_H