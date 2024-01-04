/**
 * @file   tiledb/common/pmr.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * A centralized definition of the polymorphic resource types used by
 * TileDB.
 */

#ifndef TILEDB_COMMON_PMR_H
#define TILEDB_COMMON_PMR_H

// TODO: PJD - Le sigh
#include "../../external/memory_resource/pmr_vector.h"

namespace tiledb::common::pmr {

using memory_resource = cpp17::pmr::memory_resource;

template <class Tp>
using vector = cpp17::pmr::vector<Tp>;

class tracking_resource : public cpp17::pmr::memory_resource {
 public:
  explicit tracking_resource(
      cpp17::pmr::memory_resource* upstream =
          cpp17::pmr::get_default_resource());
  ~tracking_resource();

  cpp17::pmr::memory_resource* upstream() const {
    return upstream_;
  }

 protected:
  void* do_allocate(size_t bytes, size_t alignment) override;
  void do_deallocate(void* p, size_t bytes, size_t alignment) override;
  bool do_is_equal(
      const cpp17::pmr::memory_resource& other) const noexcept override;

 private:
  cpp17::pmr::memory_resource* upstream_;
};

}  // namespace tiledb::common::pmr

#endif  // TILEDB_COMMON_PMR_H
