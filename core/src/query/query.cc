/**
 * @file   query.h
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
 * This file implements class Query.
 */

#include "query.h"
#include "logger.h"

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

namespace tiledb {

Query::Query() {
  subarray_ = nullptr;
}

Query::Query(Array* array, QueryMode mode, const void* subarray) {
  array_ = array;
  mode_ = mode;

  uint64_t subarray_size = 2 * array_->array_schema()->coords_size();
  subarray_ = malloc(subarray_size);
  if (subarray_ == nullptr) {
    LOG_STATUS(Status::QueryError("Memory allocation for subarray failed"));
    return;
  }

  if (subarray == nullptr)
    memcpy(subarray_, array_->array_schema()->domain(), subarray_size);
  else
    memcpy(subarray_, subarray, subarray_size);
}

Query::~Query() {
  if (subarray_ != nullptr)
    free(subarray_);
}

/* ****************************** */
/*               API              */
/* ****************************** */

QueryMode Query::mode() const {
  return mode_;
}

Status Query::reset_subarray(const void* subarray) {
  uint64_t subarray_size = 2 * array_->array_schema()->coords_size();
  if (subarray_ == nullptr)
    subarray_ = malloc(subarray_size);

  if (subarray_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Memory allocation when resetting subarray"));

  if (subarray == nullptr)
    memcpy(subarray_, array_->array_schema()->domain(), subarray_size);
  else
    memcpy(subarray_, subarray, subarray_size);

  return Status::Ok();
}

const void* Query::subarray() const {
  return subarray_;
}

}  // namespace tiledb
