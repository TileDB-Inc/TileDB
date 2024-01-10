/**
 * @file tiledb/common/resource/resource.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This header is for definitions needed for the declaration of managers of
 * individual resources.
 */

#ifndef TILEDB_COMMON_RESOURCE_INTERNAL_H
#define TILEDB_COMMON_RESOURCE_INTERNAL_H

namespace tiledb::common::resource_manager {

template <class P>
concept ResourceManagementPolicy = requires {
  typename P::memory_management_policy;
};

/**
 * A too-simple budget class
 *
 * @section Maturity
 *
 * This class is mostly a placeholder. It's used to exercise the construction
 * chain with different policies. Unbudgeted policies don't need budget objects
 * in the constructors of their managers; budget policies do.
 */
class Budget {
  size_t total_{0};
 public:
  Budget() = default;

  [[nodiscard]] size_t total() const {
    return total_;
  }
};

}  // namespace tiledb::common::resource_manager

#endif