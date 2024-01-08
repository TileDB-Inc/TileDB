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
 */

#ifndef TILEDB_COMMON_RESOURCE_INTERNAL_H
#define TILEDB_COMMON_RESOURCE_INTERNAL_H

namespace tiledb::common::resource_manager {

/**
 * Marker class for unbudgeted use of resource managers.
 */
struct UnbudgetedT {};
static constexpr UnbudgetedT Unbudgeted{};

struct ProductionT {};
static constexpr ProductionT Production{};

template <class T>
concept ResourceManagementPolicy = true;

/**
 * The unbudgeted resource management policy.
 *
 * This policy creates all resource managers as unbudgeted.
 */
struct RMPolicyUnbudgeted {
  static constexpr UnbudgetedT constructor_tag{};
};

struct RMPolicyProduction {
  static constexpr ProductionT constructor_tag{};
};

}  // namespace tiledb::common::resource_manager

#endif