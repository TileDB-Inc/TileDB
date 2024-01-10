/**
 * @file tiledb/common/resource/test/unit_resource.cc
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
 */

#include <test/support/tdb_catch.h>
#include "../resource.h"

namespace tiledb::common {}
namespace tdb = tiledb::common;
namespace tdbrm = tiledb::common::resource_manager;

namespace tiledb::common::resource_manager::test {
/**
 * The whitebox ResourceManager is implemented with specializations. The
 * non-specialized version is a stub.
 */
template <ResourceManagementPolicy P>
class WhiteboxResourceManager{
 public:
  WhiteboxResourceManager() = delete;
};

template <>
class WhiteboxResourceManager<tdbrm::RMPolicyUnbudgeted> : public tdbrm::ResourceManager<tdbrm::RMPolicyUnbudgeted> {
 public:
  explicit WhiteboxResourceManager()
      : tdbrm::ResourceManager<tdbrm::RMPolicyUnbudgeted>{} {};
};

template <>
class WhiteboxResourceManager<tdbrm::RMPolicyProduction> : public tdbrm::ResourceManager<tdbrm::RMPolicyProduction> {
 public:
  explicit WhiteboxResourceManager(const tdbrm::AllResourcesBudget& b)
      : tdbrm::ResourceManager<tdbrm::RMPolicyProduction>{b} {};
};

}  // namespace tiledb::common::resource_manager::test

template <tdbrm::ResourceManagementPolicy P>
using WhbxRM = tdbrm::test::WhiteboxResourceManager<P>;

TEST_CASE("Resource - unbudgeted constructor", "") {
  WhbxRM<tdbrm::RMPolicyUnbudgeted> x{};
  auto& m{x.memory()};
  (void)m.allocator();
}

TEST_CASE("Resource - production constructor", "") {
  WhbxRM<tdbrm::RMPolicyProduction> x{tdbrm::AllResourcesBudget{}};
  auto& m{x.memory()};
  (void)m.allocator();
}

#if false
using tiledb::common::pmr_allocator;
template <class T>
using tdb_vector = tiledb::common::vector<T>;
struct contents {
  int x{0};
};

TEST_CASE("tdb_vector - allocator-only constructor", "") {
  auto a{pmr_allocator(std::pmr::new_delete_resource())};
  tdb_vector<contents> x{a};
  CHECK(x.empty());
}
#endif
