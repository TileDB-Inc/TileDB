/**
 * @file   rwlock.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines and implements a write-preferring read-write lock.
 */

#ifndef TILEDB_RWLOCK_H
#define TILEDB_RWLOCK_H

#include <condition_variable>
#include <mutex>

#include "tiledb/common/macros.h"

namespace tiledb {
namespace common {

class RWLock {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  RWLock()
      : writer_(false)
      , waiting_writers_(0)
      , readers_(0) {
  }

  /** Default destructor. */
  ~RWLock() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(RWLock);
  DISABLE_MOVE_AND_MOVE_ASSIGN(RWLock);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Blocks until acquiring a read lock. */
  void read_lock() {
    std::unique_lock<std::mutex> ul(mutex_);
    while (waiting_writers_ > 0 || writer_)
      cv_.wait(ul);
    ++readers_;
  }

  /**
   * Releases a read lock, must be called after
   * `read_lock`.
   */
  void read_unlock() {
    std::unique_lock<std::mutex> ul(mutex_);
    if (--readers_ == 0)
      cv_.notify_all();
  }

  /** Blocks until acquiring a write lock. */
  void write_lock() {
    std::unique_lock<std::mutex> ul(mutex_);
    ++waiting_writers_;
    while (writer_ || readers_ > 0)
      cv_.wait(ul);
    --waiting_writers_;
    writer_ = true;
  }

  /**
   * Releases a write lock, must be called after
   * `write_lock`.
   */
  void write_unlock() {
    std::unique_lock<std::mutex> ul(mutex_);
    writer_ = false;
    cv_.notify_all();
  }

  /** Synchronously downgrades a write lock to a read lock. */
  void write_downgrade() {
    std::unique_lock<std::mutex> ul(mutex_);
    ++readers_;
    writer_ = false;
    cv_.notify_all();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Protects all member variables. */
  std::mutex mutex_;

  /** Conditional variable to signal lock activity. */
  std::condition_variable cv_;

  /** True if a write lock is held. */
  bool writer_;

  /** The number of writers waiting in `write_lock`. */
  uint64_t waiting_writers_;

  /** The number of outstanding read locks. */
  uint64_t readers_;
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_RWLOCK_H
