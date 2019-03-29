/**
 * @file   unit_test_config.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This file declares the Spinlock class. Uncontended, a single lock+unlock cycle
 * performs 65% faster than a std::mutex. Benchmarked on an 3.4GHz Intel Core i5 (Kaby Lake).
 */

#ifndef TILEDB_SPINLOCK_H
#define TILEDB_SPINLOCK_H

namespace tiledb {
namespace sm {

class Spinlock {
 public:
  /** Constructor. */
  Spinlock() = default;

  /** Destructor. */
  ~Spinlock() = default;

  /** Block until the spinlock is aquired. */
  void lock() {
    while (flag_.test_and_set(std::memory_order_acquire));
  }

  /** Unlock spinlock. Must be holding the lock. */
  void unlock() {
    flag_.clear(std::memory_order_release);
  }

 private:
  DISABLE_COPY_AND_COPY_ASSIGN(Spinlock);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Spinlock);

  /** The atomic primitive used to implemented the test-and-set spinlock. */
  std::atomic_flag flag_ = ATOMIC_FLAG_INIT;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UNIT_TEST_CONFIG_H