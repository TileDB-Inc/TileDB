/**
 * @file   s3_thread_pool_executor.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file declares the S3ThreadPoolExecutor class.
 */

#ifndef TILEDB_S3_THREAD_POOL_EXECUTOR_H
#define TILEDB_S3_THREAD_POOL_EXECUTOR_H

#ifdef HAVE_S3

#include <aws/core/utils/threading/Executor.h>
#include <unordered_set>

#include "tiledb/sm/misc/thread_pool.h"

namespace tiledb {
namespace sm {

class S3ThreadPoolExecutor : public Aws::Utils::Threading::Executor {
 public:
  /** Constructor. */
  explicit S3ThreadPoolExecutor(ThreadPool* thread_pool);

  /** Destructor. */
  ~S3ThreadPoolExecutor();

  S3ThreadPoolExecutor(const S3ThreadPoolExecutor&) = delete;
  S3ThreadPoolExecutor& operator=(const S3ThreadPoolExecutor&) = delete;
  S3ThreadPoolExecutor(S3ThreadPoolExecutor&&) = delete;
  S3ThreadPoolExecutor& operator=(S3ThreadPoolExecutor&&) = delete;

 protected:
  /** Derived from base class. */
  bool SubmitToThread(std::function<void()>&&) override;

 private:
  /** The underlying threadpool. */
  ThreadPool* const thread_pool_;

  /** All future handles associated with outstanding tasks. */
  std::unordered_set<std::shared_ptr<std::future<Status>>> tasks_;

  /** Protects 'tasks_'. */
  std::mutex tasks_lock_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_S3
#endif  // TILEDB_S3_THREAD_POOL_EXECUTOR_H
