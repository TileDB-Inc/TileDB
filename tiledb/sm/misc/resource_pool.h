/**
 * @file   resource_pool.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
 * This defines a pool of resources that can be shared for example amongst
 * threads.
 */

#ifndef RESOURCE_POOL_H
#define RESOURCE_POOL_H

#include <condition_variable>
#include <vector>

#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

template <class T, template <class> class P>
class ResourceHandle {
 public:
  ResourceHandle(P<T>& p, unsigned int n)
      : p_(p)
      , n_(n) {
  }

  T& get() {
    return p_.at(n_);
  }

  void release() {
    p_.release(n_);
  }

 private:
  /** The pool that issued this handle. */
  P<T>& p_;

  /** The index of a vector within the pool to the resource. */
  unsigned int n_;
};

/**
 * @tparam T The resource type
 * @tparam P Class P<T> provides instances of ResourceGuard<T>
 */
template <class T, template <class> class P>
class ResourceGuard {
 public:
  /** Constructor. */
  ResourceGuard(P<T>& p)
      : h_(p.take()) {
  }

  /** Destructor. */
  ~ResourceGuard() {
    h_.release();
  }

  /** Get the handle. */
  T& get() {
    return h_.get();
  }

 private:
  /** A resource handle from the provider. */
  typename P<T>::resource_handle h_;
};

template <class T>
class ResourcePool {
 public:
  using resource_handle = ResourceHandle<T, tiledb::sm::ResourcePool>;

  /** Constructor. */
  ResourcePool(unsigned int n)
      : resources_(n)
      , unused_(n)
      , unused_idx_(n - 1) {
    for (unsigned int i = 0; i < n; i++)
      unused_[i] = i;
  }

  /** Take a resource from the pool. */
  resource_handle take() {
    std::lock_guard x(m_);
    if (unused_idx_ == -1)
      throw std::runtime_error(
          std::string(
              "Ran out of resources in resource pool with contained type: ") +
          typeid(T).name());
    return ResourceHandle(*this, unused_[unused_idx_--]);
  }

 private:
  /** Release a resource from the pool. */
  void release(unsigned int n) {
    std::lock_guard x(m_);
    unused_[++unused_idx_] = n;
  }

  /** Access a resource from the internal vector. */
  T& at(unsigned int n) {
    return resources_.at(n);
  }

  /** Vector of resources. */
  std::vector<T> resources_;

  /** Vector of unused resource indexes. */
  std::vector<unsigned int> unused_;

  /** Index of the last valid resource in the unused vector. */
  int unused_idx_;

  /** Mutex protecting unused_ and unused_idx_. */
  std::mutex m_;

  friend class ResourceHandle<T, tiledb::sm::ResourcePool>;
};

template <class T>
class BlockingResourcePool {
 public:
  using resource_handle = ResourceHandle<T, tiledb::sm::BlockingResourcePool>;

  /** Constructor. */
  BlockingResourcePool(unsigned int n)
      : resources_(n)
      , unused_(n)
      , unused_idx_(n - 1)
      , num_blocked_threads_(0) {
    for (unsigned int i = 0; i < n; i++)
      unused_[i] = i;
  }

  /** Take a resource from the pool. */
  resource_handle take() {
    std::unique_lock<std::mutex> lck(m_);
    while (resources_exhausted()) {
      // block until available again
      num_blocked_threads_++;
      exhaustion_cv_.wait(lck);
      num_blocked_threads_--;
    }
    return {*this, unused_[unused_idx_--]};
  }

 private:
  /** Release a resource from the pool. */
  void release(unsigned int n) {
    std::unique_lock<std::mutex> lck(m_);
    unused_[++unused_idx_] = n;
    if (blocked_threads())
      exhaustion_cv_.notify_one();
  }

  /** Access a resource from the internal vector. */
  T& at(unsigned int n) {
    return resources_.at(n);
  }

  /** Check if resource pool is full without acquiring a lock */
  inline bool resources_exhausted() {
    return (unused_idx_ == -1);
  }

  /** Check if there is any blocked thread without acquiring a lock */
  inline bool blocked_threads() {
    return (num_blocked_threads_ > 0);
  }

  /** Vector of resources. */
  std::vector<T> resources_;

  /** Vector of unused resource indexes. */
  std::vector<unsigned int> unused_;

  /** Index of the last valid resource in the unused vector. */
  int unused_idx_;

  /** Number of threads blocked waiting for resource availability. */
  int num_blocked_threads_;

  /** For signal-and-waiting on resource availability. */
  std::condition_variable exhaustion_cv_;

  /** Mutex protecting unused_, unused_idx_, exhaustion_cv_ and
   * num_blocked_threads_ */
  std::mutex m_;

  friend class ResourceHandle<T, tiledb::sm::BlockingResourcePool>;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_MULTI_THREADED_RESOURCE_PROVIDER_H
