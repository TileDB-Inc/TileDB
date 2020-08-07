/**
 * @file   tbb_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
 This file declares the Intel TBB threading scheduler state, if TileDB is built
 with Intel TBB.
 */

#include "tiledb/sm/global_state/tbb_state.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/misc/thread_pool.h"

#include <cassert>
#include <sstream>

#ifdef HAVE_TBB
#include <tbb/task_scheduler_init.h>
#include <cstdlib>
#include <memory>
#endif

namespace tiledb {
namespace sm {
namespace global_state {

Status get_nthreads(const Config* const config, int* const nthreads) {
  if (!config) {
    *nthreads = std::strtol(Config::SM_NUM_TBB_THREADS.c_str(), nullptr, 10);
  } else {
    bool found = false;
    RETURN_NOT_OK(config->get<int>("sm.num_tbb_threads", nthreads, &found));
    assert(found);
  }

#ifdef HAVE_TBB
  if (*nthreads == tbb::task_scheduler_init::automatic) {
    *nthreads = tbb::task_scheduler_init::default_num_threads();
  }
#else
  if (*nthreads <= 0) {
    *nthreads = std::thread::hardware_concurrency();
  }
#endif

  if (*nthreads < 1) {
    std::stringstream msg;
    msg << "TBB thread runtime must be initialized with >= 1 threads, got: "
        << *nthreads;
    return Status::Error(msg.str());
  }

  return Status::Ok();
}

#ifdef HAVE_TBB

/** The TBB scheduler, used for controlling the number of TBB threads. */
static std::unique_ptr<tbb::task_scheduler_init> tbb_scheduler_;

/** The number of TBB threads the scheduler was configured with **/
int tbb_nthreads_;

Status init_tbb(const Config* const config) {
  int nthreads;
  RETURN_NOT_OK(get_nthreads(config, &nthreads));

  if (!tbb_scheduler_) {
    // initialize scheduler in process for a custom number of threads (upon
    // first thread calling init_tbb)
    try {
      tbb_scheduler_ = std::unique_ptr<tbb::task_scheduler_init>(
          new tbb::task_scheduler_init(nthreads));
      tbb_nthreads_ = nthreads;
    } catch (std::exception& err) {
      std::stringstream msg;
      msg << "TBB thread runtime initialization error: " << err.what();
      return Status::Error(msg.str());
    }
  } else {
    // if the scheduler has been initialized, check per process TBB invariants
    if (nthreads != tbb_nthreads_) {
      // if the scheduler has been initialized and the number of scheduled
      // threads per process is different, error
      std::stringstream msg;
      msg << "TBB thread runtime must be initialized with the same number of "
             "threads per process: "
          << nthreads << " != " << tbb_nthreads_;
      return Status::Error(msg.str());
    }
  }

  return Status::Ok();
}

#else

/** The ThreadPool to use when TBB is disabled. */
std::shared_ptr<ThreadPool> global_tp_;

Status init_tbb(const Config* config) {
  int nthreads;
  RETURN_NOT_OK(get_nthreads(config, &nthreads));

  if (!global_tp_) {
    global_tp_ = std::make_shared<ThreadPool>();
    const Status st = global_tp_->init(nthreads);
    if (!st.ok()) {
      global_tp_ = nullptr;
    }
    RETURN_NOT_OK(st);
  } else {
    // If the thread pool has already been initialized, check
    // invariants.
    if (nthreads != static_cast<int>(global_tp_->concurrency_level())) {
      std::stringstream msg;
      msg << "Global thread pool must be initialized with the same number of "
             "threads: "
          << nthreads << " != " << global_tp_->concurrency_level();
      return Status::Error(msg.str());
    }
  }

  return Status::Ok();
}

#endif

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb
