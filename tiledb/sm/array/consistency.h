/**
 * @file   consistency.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/uri.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
class ConsistencySentry;

/**
 * Tracks the open arrays on disk, considering that a given
 * URI can have multiple open arrays.
 *
 * Intended to act as a singleton global.
 * There is only one (global) copy of this class used in practice.
 *
 * @invariant Each registry entry is contained in exactly one Sentry.
 */
class ConsistencyController {
  /**
   * Makes available the private APIs of ConsistencyController for array
   * registration and deregistration.
   */
  friend class ConsistencySentry;
  /**
   * Makes available internals of ConsistencyController for testing.
   */
  friend class WhiteboxConsistencyController;

 public:
  /**
   * Note: array openness and registration are not fully atomic. Upon array
   * registration, the array is considered to be only partially opened, so
   * requesting array data is prohibited. However, in order to maintain
   * consistency, registration must access the array's QueryType. As such, the
   * QueryType is stored in the multimap alongside its corresponding Array.
   */
  using array_entry = std::tuple<Array&, const QueryType>;
  using entry_type = std::multimap<const URI, array_entry>::const_iterator;

  /**
   * Constructor.
   *
   * Default constructed as the initial state of the singleton Controller with
   * no registered arrays.
   * Note: nothing can be registered in the singleton until an array is opened.
   */
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

  /**
   * Register the given Array as open.
   *
   * @return Sentry object whose lifespan is the same as the registration.
   */
  ConsistencySentry make_sentry(
      const URI uri, Array& array, const QueryType query_type);

  /** Returns true if the array is open, i.e. registered in the multimap. */
  bool is_open(const URI uri);

 private:
  /**
   * Wrapper around a multimap registration operation.
   *
   * Note: this function is private because it may only be called by the
   * ConsistencySentry constructor.
   *
   * Note: this function must not request any data from an array because array
   * openness and registration are not yet fully atomic.
   *
   * @pre the given URI is the root directory of the Array and is not empty.
   */
  entry_type register_array(
      const URI uri, Array& array, const QueryType query_type);

  /**
   * Wrapper around a multimap deregistration operation.
   *
   * Note: this function is private because it may only be called by the
   * ConsistencySentry destructor.
   *
   * Note: entry_type is passed as a value that is explicitly deleted from the
   * registration multimap upon ConsistencySentry destruction only.
   */
  void deregister_array(entry_type entry);

  /**
   * The open array registry.
   *
   * Note: QueryType must be stored in the multimap to avoid requesting data
   * from partially-opened arrays because array openness and registration are
   * not yet fully atomic.
   **/
  std::multimap<const URI, array_entry> array_registry_;

  /**
   * Mutex that protects atomicity between the existence of a
   * ConsistencySentry object and the multimap's registration.
   */
  std::mutex mtx_;
};

/**
 * Sentry class for ConsistencyController.
 *
 * @invariant Each Sentry contains exactly one Controller registration entry.
 * @invariant The ConsistencyController::entry_type must not be empty.
 * @pre invariant exception:
 * the entry_type may be empty ONLY in an rvalue during move construction.
 */
class ConsistencySentry {
 public:
  /** Constructor. */
  ConsistencySentry(
      ConsistencyController& registry, ConsistencyController::entry_type entry);

  /**
   * Move constructor.
   *
   * @pre uses transfer semantics to maintain the class invariants.
   **/
  ConsistencySentry(ConsistencySentry&& x);

  /** Copy Constructor is deleted to maintain the class invariants. */
  ConsistencySentry(const ConsistencySentry&) = delete;

  /** Copy assignment is deleted to maintain the class invariants. */
  ConsistencySentry& operator=(const ConsistencySentry&) = delete;

  /** Move assignment is unnecessary and thus deleted. */
  ConsistencySentry& operator=(ConsistencySentry&&) = delete;

  /** Destructor. */
  ~ConsistencySentry();

 private:
  /**
   * The ConsistencyController instance.
   *
   * @pre This MUST reference the same member that created the Sentry object.
   */
  ConsistencyController& parent_;

  /**
   * An entry within the registry structure of the parent
   * ConsistencyController object.
   */
  ConsistencyController::entry_type entry_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_CONSISTENCY_H
