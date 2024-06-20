/**
 * @file   unit_concurrent_set.cc
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
 * This file defines some pseudo_node classes for testing ports
 */

#include "experimental/tiledb/common/dag/utility/test/unit_concurrent_set.h"
#include <functional>
#include <future>
#include <type_traits>
#include "experimental/tiledb/common/dag/utility/concurrent_set.h"

namespace tiledb::common {}
using namespace tiledb::common;

TEST_CASE("ConcurrentSet: Construct", "[concurrent_set]") {
}

TEST_CASE("ConcurrentSet: Test empty", "[concurrent_set]") {
  ConcurrentSet<int> numbers;
  CHECK(numbers.empty() == true);

  numbers.insert(42);
  numbers.insert(13317);
  CHECK(numbers.empty() == false);
}

TEST_CASE("ConcurrentSet: Test size", "[concurrent_set]") {
  ConcurrentSet<int> numbers{1, 3, 5, 7, 11};
  CHECK(numbers.size() == 5);
}

TEST_CASE("ConcurrentSet: Test clear", "[concurrent_set]") {
  ConcurrentSet<int> numbers{1, 2, 3};
  CHECK(numbers.size() == 3);
  numbers.clear();
  CHECK(numbers.empty());
}

TEST_CASE("ConcurrentSet: Test insert", "[concurrent_set]") {
  ConcurrentSet<int> set;

  auto result_1 = set.insert(3);
  CHECK(result_1.first != set.end());
  CHECK(*result_1.first == 3);
  CHECK(result_1.second);
  auto result_2 = set.insert(3);
  CHECK(result_2.first == result_1.first);  // same iterator
  CHECK(*result_2.first == 3);
  CHECK(!result_2.second);
}

class Dew {
 private:
  int a;
  int b;
  int c;

 public:
  Dew(int _a, int _b, int _c)
      : a(_a)
      , b(_b)
      , c(_c) {
  }

  bool operator<(const Dew& other) const {
    if (a < other.a)
      return true;
    if (a == other.a && b < other.b)
      return true;
    return (a == other.a && b == other.b && c < other.c);
  }
};

const int nof_operations = 17;

size_t set_emplace() {
  ConcurrentSet<Dew> set;
  for (int i = 0; i < nof_operations; ++i)
    for (int j = 0; j < nof_operations; ++j)
      for (int k = 0; k < nof_operations; ++k)
        set.emplace(i, j, k);

  return set.size();
}

TEST_CASE("ConcurrentSet: Test emplace", "[concurrent_set]") {
  CHECK(set_emplace() == nof_operations * nof_operations * nof_operations);
}

TEST_CASE("ConcurrentSet: Test erase", "[concurrent_set]") {
  ConcurrentSet<int> numbers = {1, 2, 3, 4, 1, 2, 3, 4};

  CHECK(numbers.size() == 4);

  for (auto it = numbers.begin(); it != numbers.end();) {
    if (*it % 2 != 0)
      it = numbers.erase(it);
    else
      ++it;
  }
  CHECK(numbers.size() == 2);

  CHECK(numbers.erase(1) == 0);
  CHECK(numbers.erase(2) == 1);
  CHECK(numbers.erase(2) == 0);
}

TEST_CASE("ConcurrentSet: Test swap", "[concurrent_set]") {
  {
    ConcurrentSet<int> a1{3, 1, 3, 2, 7}, a2{5, 4, 5};

    const int& ref1 = *(a1.begin());
    const int& ref2 = *(a2.begin());

    CHECK(a1.size() == 4);
    CHECK(a2.size() == 2);

    CHECK(*(a1.begin()) == 1);
    CHECK(*(a2.begin()) == 4);

    CHECK(ref1 == 1);
    CHECK(ref2 == 4);

    a1.swap(a2);

    CHECK(a1.size() == 2);
    CHECK(a2.size() == 4);

    CHECK(*(a1.begin()) == 4);
    CHECK(*(a2.begin()) == 1);

    CHECK(ref1 == 1);
    CHECK(ref2 == 4);

    struct Cmp : std::less<> {
      int id{};
      explicit Cmp(int i)
          : id{i} {
      }
    };

    std::set<int, Cmp> s1{{2, 2, 1, 1}, Cmp{6}}, s2{{4, 4, 3, 3}, Cmp{9}};

    auto f = s1.key_comp().id;
    auto g = s2.key_comp().id;

    s1.swap(s2);

    CHECK(g == s1.key_comp().id);
    CHECK(f == s2.key_comp().id);
  }
}

TEST_CASE("ConcurrentSet: Test extract", "[concurrent_set]") {
  ConcurrentSet<int> cont0{1, 2, 3};
  ConcurrentSet<int> cont1{1, 2, 3};
  ConcurrentSet<int> cont2{2, 3};
  ConcurrentSet<int> cont3{2, 3, 4};

  CHECK(cont0 == cont1);

  auto nh = cont0.extract(1);
  nh.value() = 4;

  CHECK(cont0 == cont2);

  cont0.insert(std::move(nh));

  CHECK(cont0 == cont3);
}

struct FatKey {
  int x;
  int data[1000];
};
struct LightKey {
  int x;
};
// Note: as detailed above, the container must use std::less<> (or other
//   transparent Comparator) to access these overloads.
// This includes standard overloads, such as between std::string and
// std::string_view.
bool operator<(const FatKey& fk, const LightKey& lk) {
  return fk.x < lk.x;
}
bool operator<(const LightKey& lk, const FatKey& fk) {
  return lk.x < fk.x;
}
bool operator<(const FatKey& fk1, const FatKey& fk2) {
  return fk1.x < fk2.x;
}

TEST_CASE("ConcurrentSet: Test find", "[concurrent_set]") {
  // simple comparison demo
  ConcurrentSet<int> example = {1, 2, 3, 4};

  auto search = example.find(2);
  CHECK(search != example.end());

  // transparent comparison demo
  ConcurrentSet<FatKey, std::less<>> example2 = {
      {1, {}}, {2, {}}, {3, {}}, {4, {}}};

  LightKey lk = {2};
  auto search2 = example2.find(lk);
  CHECK(search2 != example2.end());
}
