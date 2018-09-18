/**
 * @file   context.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file implements class Context.
 */

#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Context::Context()
    : last_error_(Status::Ok())
    , storage_manager_(nullptr) {
}

Context::~Context() {
  delete storage_manager_;
}

/* ****************************** */
/*                API             */
/* ****************************** */

Status Context::init(Config* config) {
  if (storage_manager_ != nullptr)
    return LOG_STATUS(Status::ContextError(
        "Cannot initialize context; Context already initialized"));

  // Create storage manager
  storage_manager_ = new (std::nothrow) tiledb::sm::StorageManager();
  if (storage_manager_ == nullptr)
    return LOG_STATUS(Status::ContextError(
        "Cannot initialize contextl Storage manager allocation failed"));

  // Initialize storage manager
  return storage_manager_->init(config);
}

Status Context::last_error() {
  std::lock_guard<std::mutex> lock(mtx_);
  return last_error_;
}

void Context::save_error(const Status& st) {
  std::lock_guard<std::mutex> lock(mtx_);
  last_error_ = st;
}

StorageManager* Context::storage_manager() const {
  return storage_manager_;
}

}  // namespace sm
}  // namespace tiledb
