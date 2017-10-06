/**
 * @file   locked_array.h
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
 * This file defines class LockedArray.
 */

#ifndef TILEDB_LOCKED_ARRAY_H
#define TILEDB_LOCKED_ARRAY_H

#include "status.h"
#include "vfs.h"

#include <condition_variable>
#include <mutex>

namespace tiledb {

/** Stores information about a locked array. */
class LockedArray {
 public:
  /* ****************************** */
  /*   CONSTRUCTORS & DESTRUCTORS   */
  /* ****************************** */

  /** Constructor.  */
  LockedArray();

  /* ****************************** */
  /*               API              */
  /* ****************************** */

  /** Decrements the total number of locks. */
  void decr_total_locks();

  /** Inrements the total number of locks. */
  void incr_total_locks();

  /**
   * Locks the array.
   *
   * @param vfs The virtual file system that will lock the filelock.
   * @param uri The URI of the file to be locked.
   * @param shared True if this is a shared lock, false if it is exclusive
   * @return Status
   */
  Status lock(VFS* vfs, const URI& uri, bool shared);

  /** Returns true if there are no locks. */
  bool no_locks() const;

  /**
   * Unlocks the array.
   *
   * @param vfs The virtual file system that will unlock the filelock.
   * @param uri The URI of the file to be unlocked.
   * @param shared True if this was a shared lock, false if it was exclusive
   * @return Status
   */
  Status unlock(VFS* vfs, const URI& uri, bool shared);

 private:
  /* ****************************** */
  /*        PRIVATE ATTRIBUTES      */
  /* ****************************** */

  /** The condition variable. */
  std::condition_variable cv_;

  /** True if the array is locked with an exclusive lock. */
  bool exclusive_lock_;

  /** Filelock handle. */
  int filelock_;

  /** The locked array mutex. */
  std::mutex mtx_;

  /** Number of shared locks. */
  unsigned int shared_locks_;

  /** Total number of locks. */
  unsigned int total_locks_;

  /* ****************************** */
  /*        PRIVATE ATTRIBUTES      */
  /* ****************************** */

  /**
   * Exclusive-locks the array.
   *
   * @param vfs The virtual file system that will lock the filelock.
   * @param uri The URI of the file to be locked.
   * @return Status
   */
  Status lock_exclusive(VFS* vfs, const URI& uri);

  /**
   * Share-locks the array.
   *
   * @param vfs The virtual file system that will lock the filelock.
   * @param uri The URI of the file to be locked.
   * @return Status
   */
  Status lock_shared(VFS* vfs, const URI& uri);

  /**
   * Share-unlocks the array.
   *
   * @param vfs The virtual file system that will unlock the filelock.
   * @param uri The URI of the file to be unlocked.
   * @return Status
   */
  Status unlock_shared(VFS* vfs, const URI& uri);

  /**
   * Exclusive-unlocks the array.
   *
   * @param vfs The virtual file system that will unlock the filelock.
   * @param uri The URI of the file to be unlocked.
   * @return Status
   */
  Status unlock_exclusive(VFS* vfs, const URI& uri);
};

}  // namespace tiledb

#endif  // TILEDB_LOCKED_ARRAY_H
