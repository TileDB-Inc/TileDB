/**
 * @file   spinklock.h
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
 * This file defines some spinlocks.
 *
 * @todo Verify performance of spinlocks in comparison to `std::mutex` for fsm
 * imlementation in fsm.h.
 */

#include <atomic>

namespace tiledb::common {

/**
 * Basic spinlock using `std::atomic_flag`. Implements lock() and unlock(),
 * which are suffient for use with std::lock (and relatives).
 */
class spinlock_mutex {
  /* Use `std::atomic_flag`, which is always guaranteed to be lock-free
   * (actually atomic).
   */
  std::atomic_flag lock_;

 public:
  /**
   * Until C++20, `std::atomic_flag` is initialized in an unspecified state. Use
   * ATOMIC_FLAG_INIT macro to initialize.
   */
  spinlock_mutex()
      : lock_ ATOMIC_FLAG_INIT {
  }

  /**
   * Acquire lock. Spins until `lock_` is cleared.
   */
  void lock() {
    while (lock_.test_and_set(std::memory_order_acquire))
      ;
  }

  /**
   * Release lock.
   */
  void unlock() {
    lock_.clear(std::memory_order_release);
  }
};

/**
 * Test and test and set spinlock, using std::atomic<bool>.  Implements  lock()
 * and unlock(), which are suffient for use with std::lock (and relatives).
 */
struct ttas_bool_spinlock_mutex {
  std::atomic<bool> lock_ = {false};

  /**
   * Acquire lock.  This is optimized for the uncontended case.  Attempt to
   * acquire the lock.  If that fails, then spin.
   */
  void lock() noexcept {
    for (;;) {
      if (!lock_.exchange(true, std::memory_order_acquire)) {
        return;
      }
      while (lock_.load(std::memory_order_relaxed)) {
        ;
#if 0
        /**
	 * Possible architecture-specific optimization. using X86 PAUSE or ARM YIELD instruction to reduce contention between hyper-threads.
	 *
	 * @todo Test for appropriate compile-time macros to optionally include.
	 */
        __builtin_ia32_pause();
#endif
      }
    }
  }

  /**
   * Non-blocking attempt to lock.
   */
  bool try_lock() noexcept {
    /*
     * First do a relaxed load to check if lock is free in order to prevent
     * unnecessary cache misses if someone does while(!try_lock())
     */
    return !lock_.load(std::memory_order_relaxed) &&
           !lock_.exchange(true, std::memory_order_acquire);
  }

  /**
   * Release lock.
   */
  void unlock() noexcept {
    lock_.store(false, std::memory_order_release);
  }
};

/**
 * Test and test and set spinlock, using std::atomic<bool>.  Implements  lock()
 * and unlock(), which are suffient for use with std::lock (and relatives).
 */
struct ttas_flag_spinlock_mutex {
  /**
   * Use `std::atomic_flag`, which is always guaranteed to be lock-free
   * (actually atomic).
   */
  std::atomic_flag lock_;

  /**
   * Until C++20, `std::atomic_flag` is initialized in an unspecified state. Use
   * ATOMIC_FLAG_INIT macro to initialize.
   */
  ttas_flag_spinlock_mutex()
      : lock_ ATOMIC_FLAG_INIT {
  }

  /**
   * Acquire lock.  This is optimized for the uncontended case.  Attempt to
   * acquire the lock.  If that fails, then spin.
   */
  void lock() {
    for (;;) {
      if (!lock_.test_and_set(std::memory_order_acquire)) {
        return;
      }
      while (lock_.test_and_set(std::memory_order_acquire)) {
        ;
      }
      return;
    }
  }

  /**
   * Release lock;
   */
  void unlock() {
    lock_.clear(std::memory_order_release);
  }
};

}  // namespace tiledb::common
