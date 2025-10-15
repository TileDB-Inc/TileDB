/**
 * @file unit_indexed_list.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file tests the `IndexedList` data structure.
 */

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include "tiledb/common/indexed_list.h"
#include "tiledb/common/memory_tracker.h"

#include <iterator>
#include <numeric>

using namespace tiledb::common;
using namespace tiledb::test;

static_assert(std::input_iterator<IndexedList<uint64_t>::iterator>);
static_assert(std::input_iterator<IndexedList<uint64_t>::const_iterator>);

namespace tiledb::common::detail {

template <typename T>
struct WhiteboxIndexedList : public IndexedList<T> {
  using IndexedList<T>::list_;
  using IndexedList<T>::vec_;

  void integrity_check() const {
    REQUIRE(list_.size() == vec_.size());

    auto value = list_.begin();
    for (uint64_t i = 0; i < vec_.size(); ++i, ++value) {
      REQUIRE(vec_[i] == &*value);
    }
  }
};

}  // namespace tiledb::common::detail

template <typename T>
void integrity_check(const IndexedList<T>& value) {
  static_cast<const detail::WhiteboxIndexedList<T>&>(value).integrity_check();
}

template <typename T>
std::vector<T> instance_iterator(std::vector<T> values_in) {
  auto mem = get_test_memory_tracker();

  IndexedList<T> ii(mem, tiledb::sm::MemoryType::WRITER_DATA);
  for (const auto& value : values_in) {
    ii.emplace_back(value);
  }

  const std::vector<T> values_out(ii.begin(), ii.end());

  CHECK(values_in == values_out);

  integrity_check<T>(ii);

  return values_out;
}

TEST_CASE("IndexedList iterator", "[algorithm]") {
  const uint64_t num_values = GENERATE(0, 1, 2, 4, 8, 16, 32);

  DYNAMIC_SECTION("num_values = " << num_values) {
    std::vector<uint64_t> values;
    values.resize(num_values);
    std::iota(values.begin(), values.end(), 0);

    instance_iterator<uint64_t>(values);
  }
}

template <typename T>
std::pair<std::list<T>, std::list<T>> instance_splice(
    std::list<T> values_in,
    uint64_t insert_pos,
    std::list<T> values_splice,
    uint64_t splice_first,
    uint64_t splice_last) {
  auto mem = get_test_memory_tracker();

  IndexedList<T> idst(mem, tiledb::sm::MemoryType::WRITER_DATA);
  IndexedList<T> isplice(mem, tiledb::sm::MemoryType::WRITER_DATA);

  for (const auto& value : values_in) {
    idst.emplace_back(value);
  }
  for (const auto& value : values_splice) {
    isplice.emplace_back(value);
  }

  idst.splice(
      std::next(idst.begin(), insert_pos),
      isplice,
      std::next(isplice.begin(), splice_first),
      std::next(isplice.begin(), splice_last));

  const std::list<T> values_out(idst.begin(), idst.end());
  const std::list<T> splice_out(isplice.begin(), isplice.end());

  // check generic correctness
  values_in.splice(
      std::next(values_in.begin(), insert_pos),
      values_splice,
      std::next(values_splice.begin(), splice_first),
      std::next(values_splice.begin(), splice_last));
  CHECK(values_out == values_in);
  CHECK(splice_out == values_splice);

  integrity_check<T>(idst);
  integrity_check<T>(isplice);

  return std::make_pair(values_out, splice_out);
}

TEST_CASE("IndexedList splice", "[algorithm]") {
  SECTION("Trivial") {
    const auto r = instance_splice<uint64_t>({}, 0, {}, 0, 0);
    CHECK(r.first == std::list<uint64_t>{});
    CHECK(r.second == std::list<uint64_t>{});
  }

  SECTION("Transfer all to empty") {
    const auto r = instance_splice<uint64_t>({}, 0, {0, 1, 2, 3}, 0, 4);
    CHECK(r.first == std::list<uint64_t>{0, 1, 2, 3});
    CHECK(r.second == std::list<uint64_t>{});
  }

  SECTION("Transfer subset to empty") {
    const auto r = instance_splice<uint64_t>({}, 0, {0, 1, 2, 3}, 1, 3);
    CHECK(r.first == std::list<uint64_t>{1, 2});
    CHECK(r.second == std::list<uint64_t>{0, 3});
  }

  SECTION("Transfer empty to nonempty") {
    const auto r = instance_splice<uint64_t>({0, 1, 2, 3}, 0, {}, 0, 0);
    CHECK(r.first == std::list<uint64_t>{0, 1, 2, 3});
    CHECK(r.second == std::list<uint64_t>{});
  }

  SECTION("Transfer to nonempty end") {
    const auto r =
        instance_splice<uint64_t>({0, 1, 2, 3}, 4, {4, 5, 6, 7}, 1, 3);
    CHECK(r.first == std::list<uint64_t>{0, 1, 2, 3, 5, 6});
    CHECK(r.second == std::list<uint64_t>{4, 7});
  }

  SECTION("Transfer to nonempty intermediate position") {
    const auto r =
        instance_splice<uint64_t>({0, 1, 2, 3}, 2, {4, 5, 6, 7}, 1, 3);
    CHECK(r.first == std::list<uint64_t>{0, 1, 5, 6, 2, 3});
    CHECK(r.second == std::list<uint64_t>{4, 7});
  }
}

TEST_CASE("IndexedList splice rapidcheck", "[algorithm][rapidcheck]") {
  rc::prop(
      "Splice correctness",
      [](std::list<uint64_t> target, std::list<uint64_t> src) {
        const uint64_t targetpos =
            *rc::gen::inRange<uint64_t>(0, target.size() + 1);
        const uint64_t srcfirst =
            *rc::gen::inRange<uint64_t>(0, src.size() + 1);
        const uint64_t srclast =
            *rc::gen::inRange<uint64_t>(srcfirst, src.size() + 1);

        instance_splice<uint64_t>(target, targetpos, src, srcfirst, srclast);
      });
}
