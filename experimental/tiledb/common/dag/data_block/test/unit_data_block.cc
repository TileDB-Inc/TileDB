/**
 * @file unit_data_block.cc
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
 * Unit tests for the data_block class.  We test with 3 sizes: size equal to a
 * complete chunk, a size equal to half a chunk - 1, and a size equal to half a
 * chunk + 1.  The latter two are to check for some corner cases.  We implement
 * tests as templated functions to accommodate the differently sized
 * `DataBlocks`.
 *
 * In addition to tests of `DataBlock`s, we also include tests of `DataBlock`s
 * that have been joined into a virtual contiguous range.  The implementation of
 * `join` is in dag/utils/range_join.h.
 */

#include "unit_data_block.h"
#include <algorithm>
#include <forward_list>
#include <list>
#include <numeric>

/**
 * @todo Use proper checks for preprocessor directives to selectively include
 * test with `std::execution` policy.
 */
#if 0
#include <execution>
#endif

#include <list>
#include <memory>
#include <vector>
#include "experimental/tiledb/common/dag/data_block/data_block.h"
#include "experimental/tiledb/common/dag/utility/range_join.h"
using namespace tiledb::common;

TEST_CASE(
    "DataBlock: Test create DataBlock with constructors", "[data_block]") {
  [[maybe_unused]] auto da = DataBlock<4'096>{};
  CHECK(da.size() == 0);
  CHECK(da.capacity() == 4096);
}

TEST_CASE("DataBlock: Test create DataBlock new / delete", "[data_block]") {
  auto da = new DataBlock<4'096>;
  CHECK(da->size() == 0);
  CHECK(da->capacity() == 4096);
  delete da;
}

TEST_CASE("DataBlock: Test create DataBlock with make_unique", "[data_block]") {
  auto da = std::make_unique<DataBlock<4'096>>();
  CHECK(da->size() == 0);
  CHECK(da->capacity() == 4096);
}

TEST_CASE("DataBlock: Test create DataBlock with make_shared", "[data_block]") {
  auto da = std::make_shared<DataBlock<4'096>>();
  CHECK(da->size() == 0);
  CHECK(da->capacity() == 4096);
}

TEST_CASE("DataBlock: Test with forward_list", "[data_block]") {
  std::forward_list<DataBlock<4'096>> slist;
  slist.emplace_front();
  CHECK(slist.front().size() == 0);
  CHECK(slist.front().capacity() == 4096);

  // This is okay since `DataBlock`s are shared ptrs to underlying memory.
  // We don't need an array new.
  std::vector<DataBlock<4'096>> vec{4};
}

/**
 * Test constructor
 */
TEST_CASE("DataBlock: Test create DataBlock", "[data_block]") {
  SECTION("Constructor compilation tests") {
    using DataBlock = DataBlock<4'096>;
    DataBlock da;
    CHECK(da.size() == 0);
    CHECK(da.capacity() == DataBlock::max_size());

    DataBlock db{};
    CHECK(db.size() == 0);
    CHECK(db.capacity() == DataBlock::max_size());

    DataBlock dc{0};
    CHECK(dc.size() == 0);
    CHECK(dc.capacity() == DataBlock::max_size());

    DataBlock dd{DataBlock::max_size()};
    CHECK(dd.size() == DataBlock::max_size());
    CHECK(dd.capacity() == DataBlock::max_size());

    auto de = DataBlock();
    CHECK(de.size() == 0);
    CHECK(de.capacity() == DataBlock::max_size());

    auto df = DataBlock{};
    CHECK(df.size() == 0);
    CHECK(df.capacity() == DataBlock::max_size());

    auto dg = DataBlock(0);
    CHECK(dg.size() == 0);
    CHECK(dg.capacity() == DataBlock::max_size());

    auto dh = DataBlock{0};
    CHECK(dh.size() == 0);
    CHECK(dh.capacity() == DataBlock::max_size());

    auto di = DataBlock(DataBlock::max_size());
    CHECK(di.size() == DataBlock::max_size());
    CHECK(di.capacity() == DataBlock::max_size());

    auto dj = DataBlock{DataBlock::max_size()};
    CHECK(dj.size() == DataBlock::max_size());
    CHECK(dj.capacity() == DataBlock::max_size());
  }
}

/**
 * Some simple tests of the DataBlock interface.
 *
 * @tparam The `DataBlock` type
 */
template <class DB>
void db_test_0(DB& db) {
  auto a = db.begin();
  auto b = db.cbegin();
  auto c = db.end();
  auto d = db.cend();

  CHECK(db.size() != 0);
  CHECK(a == b);
  CHECK(++a == ++b);
  CHECK(a++ == b++);
  CHECK(a == b);
  CHECK(++a != b);
  CHECK(a == ++b);
  CHECK(c == d);
  auto e = c + 5;
  auto f = d + 5;
  CHECK(c == e - 5);
  CHECK(d == f - 5);
  CHECK(e == f);
  CHECK(e - 5 == f - 5);
  auto g = a + 1;
  CHECK(g > a);
  CHECK(g >= a);
  CHECK(a < g);
  CHECK(a <= g);
}

template <class DB>
void db_test_1(const DB& db) {
  auto a = db.begin();
  auto b = db.cbegin();
  auto c = db.end();
  auto d = db.cend();

  CHECK(db.size() != 0);
  CHECK(a == b);
  CHECK(++a == ++b);
  CHECK(a++ == b++);
  CHECK(a == b);
  CHECK(++a != b);
  CHECK(a == ++b);
  CHECK(c == d);
  auto e = c + 5;
  auto f = d + 5;
  CHECK(c == e - 5);
  CHECK(d == f - 5);
  CHECK(e == f);
  CHECK(e - 5 == f - 5);
  auto g = a + 1;
  CHECK(g > a);
  CHECK(g >= a);
  CHECK(a < g);
  CHECK(a <= g);
}

/**
 * Invoke simple API tests with a basic `DataBlock`
 */
TEST_CASE("DataBlock: Test API of variously sized DataBlock", "[data_block]") {
  using DataBlock = DataBlock<4'096>;
  size_t chunk_size_ = 4'096;

  SECTION("Specific size constructed") {
    auto db = DataBlock{1};
    db_test_0(db);
    db_test_1(db);
  }
  SECTION("Specific size constructed") {
    auto db = DataBlock{chunk_size_};
    db_test_0(db);
    db_test_1(db);
  }
  SECTION("Specific size constructed") {
    auto db = DataBlock{chunk_size_ - 1};
    db_test_0(db);
    db_test_1(db);
  }
  SECTION("Specific size constructed") {
    auto db = DataBlock{chunk_size_ / 2};
    db_test_0(db);
    db_test_1(db);
  }
  SECTION("Specific size constructed") {
    auto db = DataBlock{chunk_size_ / 2 - 1};
    db_test_0(db);
    db_test_1(db);
  }
  SECTION("Specific size constructed") {
    auto db = DataBlock{chunk_size_ / 2 + 1};
    db_test_0(db);
    db_test_1(db);
  }
  SECTION("Max size constructed") {
    auto db = DataBlock{DataBlock::max_size()};
    db_test_0(db);
    db_test_1(db);
  }
}

/**
 * Test iterating through a `DataBlock`
 */
template <class DB>
void db_test_2(DB& db) {
  for (auto& j : db) {
    j = std::byte{255};
  }
  auto e = std::find_if_not(
      db.begin(), db.end(), [](auto a) { return std::byte{255} == a; });
  CHECK(e == db.end());

  for (auto& j : db) {
    j = std::byte{13};
  }
  auto f = std::find_if_not(
      db.begin(), db.end(), [](auto a) { return std::byte{13} == a; });
  CHECK(f == db.end());
}

/**
 * Run iteration test on DataBlock allocated with `std::allocator` and with our
 * `PoolAllocator`.
 */
TEST_CASE("DataBlock: Iterate through data_block", "[data_block]") {
  static constexpr size_t chunk_size_ = 4'096;
  using DataBlock = DataBlock<chunk_size_>;
  auto db = DataBlock{};
  db_test_2(db);
  auto dc = DataBlock{chunk_size_};
  db_test_2(dc);
}

/**
 * Run iteration test on multiple DataBlocks, again allocated with
 * `std::allocator` and with our `PoolAllocator`.
 */
TEST_CASE("DataBlock: Iterate through 8 data_blocks", "[data_block]") {
  using DataBlock = DataBlock<4'096>;
  for (size_t i = 0; i < 8; ++i) {
    auto db = DataBlock{};
    db_test_2(db);
  }
  for (size_t i = 0; i < 8; ++i) {
    auto db = DataBlock{4'096};
    db_test_2(db);
  }
}

/**
 * Verify some properties of `DataBlock`s
 */
TEST_CASE("DataBlock: Get span", "[data_block]") {
  using DataBlock = DataBlock<4'194'304>;

  auto a = DataBlock{};
  auto b = DataBlock{};
  auto c = DataBlock{};

  CHECK(a.size() == 0);
  CHECK(b.size() == 0);
  CHECK(c.size() == 0);

  CHECK(a.data() != b.data());
  CHECK(a.data() != c.data());
  CHECK(b.data() != c.data());

  CHECK(a.capacity() == 4'194'304);
  CHECK(b.capacity() == 4'194'304);
  CHECK(c.capacity() == 4'194'304);

  SECTION("entire_span") {
    auto span_a = a.entire_span();
    auto span_b = b.entire_span();
    auto span_c = c.entire_span();

    CHECK(span_a.data() == a.data());
    CHECK(span_b.data() == b.data());
    CHECK(span_c.data() == c.data());

    CHECK(span_a.size() == 4'194'304);
    CHECK(span_b.size() == 4'194'304);
    CHECK(span_c.size() == 4'194'304);
  }
  SECTION("span") {
    auto span_a = a.span();
    auto span_b = b.span();
    auto span_c = c.span();

    CHECK(span_a.data() == a.data());
    CHECK(span_b.data() == b.data());
    CHECK(span_c.data() == c.data());

    CHECK(span_a.size() == 0);
    CHECK(span_b.size() == 0);
    CHECK(span_c.size() == 0);
  }
}

/**
 * Test resizing.
 */
TEST_CASE("DataBlock: Test resize", "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<4'194'304>;

  auto a = DataBlock{chunk_size_};
  auto b = DataBlock{chunk_size_};
  auto c = DataBlock{chunk_size_};

  a.resize(1'000'000);
  b.resize(2'000'000);
  c.resize(3'000'000);

  CHECK(a.size() == 1'000'000);
  CHECK(b.size() == 2'000'000);
  CHECK(c.size() == 3'000'000);

  auto span_a = a.span();
  auto span_b = b.span();
  auto span_c = c.span();

  CHECK(span_a.size() == 1'000'000);
  CHECK(span_b.size() == 2'000'000);
  CHECK(span_c.size() == 3'000'000);
}

TEST_CASE("DataBlock: Test (shallow) copying", "[data_block]") {
  using DataBlock = DataBlock<4'194'304>;

  auto a = DataBlock{DataBlock::max_size()};
  auto b = DataBlock{DataBlock::max_size()};
  auto c = DataBlock{DataBlock::max_size()};

  [[maybe_unused]] auto da = a.data();
  [[maybe_unused]] auto db = b.data();
  [[maybe_unused]] auto dc = c.data();

  CHECK(a.size() == DataBlock::max_size());
  CHECK(b.size() == DataBlock::max_size());
  CHECK(c.size() == DataBlock::max_size());
  CHECK(a.capacity() == DataBlock::max_size());
  CHECK(b.capacity() == DataBlock::max_size());
  CHECK(c.capacity() == DataBlock::max_size());

  /* Hmm...  Can't do arithmetic on std::byte */
  std::iota(
      reinterpret_cast<uint8_t*>(a.begin()),
      reinterpret_cast<uint8_t*>(a.end()),
      uint8_t{0});
  std::iota(
      reinterpret_cast<uint8_t*>(b.begin()),
      reinterpret_cast<uint8_t*>(b.end()),
      uint8_t{0});
  std::iota(
      reinterpret_cast<uint8_t*>(c.begin()),
      reinterpret_cast<uint8_t*>(c.end()),
      uint8_t{0});

  auto check_iota = [](const DataBlock& y) {
    uint8_t b{0};
    for (auto&& j : y) {
      if (static_cast<uint8_t>(j) != b) {
        return false;
      }
      ++b;
    }
    return true;
  };
  auto check_zero = [](const DataBlock& y) {
    uint8_t b{0};
    for (auto&& j : y) {
      if (static_cast<uint8_t>(j) != b) {
        return false;
      }
    }
    return true;
  };

  CHECK(check_iota(a));
  CHECK(check_iota(b));
  CHECK(check_iota(c));
  CHECK(!check_zero(a));
  CHECK(!check_zero(b));
  CHECK(!check_zero(c));

  auto verify_da = [&check_iota, &check_zero](DataBlock& d, DataBlock& a) {
    CHECK(a.data() == d.data());
    CHECK(d.size() == a.size());
    a.resize(1);
    CHECK(a.size() == 1);
    CHECK(d.size() == DataBlock::max_size());

    a.resize(DataBlock::max_size());
    CHECK(a.size() == DataBlock::max_size());
    CHECK(d.size() == DataBlock::max_size());

    /**
     * Test changes to a reflecting in d
     */
    CHECK(check_iota(a));
    CHECK(check_iota(d));
    std::fill(a.begin(), a.end(), std::byte{0});
    CHECK(check_zero(a));
    CHECK(check_zero(d));

    /**
     * Test changes to d reflecting in a
     */
    std::iota(
        reinterpret_cast<uint8_t*>(a.begin()),
        reinterpret_cast<uint8_t*>(a.end()),
        uint8_t{0});

    CHECK(check_iota(a));
    CHECK(check_iota(d));
    std::fill(d.begin(), d.end(), std::byte{0});
    CHECK(check_zero(a));
    CHECK(check_zero(d));
  };

  SECTION("Test copy constructor") {
    auto d = a;
    verify_da(d, a);
  }
  SECTION("Test copy constructor, ii") {
    DataBlock d(a);
    verify_da(d, a);
  }
  SECTION("Test assignment") {
    DataBlock d;
    d = a;
    verify_da(d, a);
  }

  /**
   * Test move constructors.  This is dangerous because in general, pointers may
   * not be valid.
   */
  SECTION("Test move constructor, ii") {
    DataBlock tmp = b;
    CHECK(tmp.use_count() == 2);
    CHECK(b.use_count() == 2);

    DataBlock d(std::move(b));

    CHECK(d.use_count() == 2);
    CHECK(b.use_count() == 0);
    CHECK(tmp.use_count() == 2);

    // Don't verify against something that has been moved from
    //    verify_da(d, b);
    CHECK(db == tmp.data());
    CHECK(db == d.data());

    verify_da(d, tmp);
  }

  SECTION("Test move assignment") {
    DataBlock tmp = c;
    DataBlock d;
    CHECK(c.use_count() == 2);
    d = std::move(c);
    CHECK(dc == d.data());

    CHECK(c.use_count() == 0);  // Can't use c any longer

    // Don't verify against something that has been moved from
    //    verify_da(d, c);
    verify_da(d, tmp);
  }
}

#if 0
/**
 * Attempt to test destructor by invoking it explicitly.  Unfortunately, the destructor will be invoked again when the variable goes out of scope.  Which is bad.
 */
TEST_CASE(
    "DataBlock: Test allocation and deallocation of DataBlock", "[data_block") {
  auto a = DataBlock{chunk_size_};
  auto ptr_a = a.data();

  // Don't do this!  It will call destructor again when `a` goes out of scope
  // (which would be bad)
  a.~DataBlock();

  auto b = DataBlock{chunk_size_};
  auto ptr_b = b.data();
  CHECK(ptr_a == ptr_b);
}
#endif

/**
 * Verify that blocks are deallocated properly when the destructor is called. We
 * leverage the fact (and also check) that the `PoolAllocator` is a FIFO buffer
 * and check that when we deallocate a block, that will be the block that is
 * returned on next call to allocate.
 */
TEST_CASE(
    "DataBlock: Test deallocation of DataBlock on destruction",
    "[data_block]") {
  std::byte* x;
  std::byte* y;
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  {
    auto a = DataBlock{chunk_size_};
    auto b = DataBlock{chunk_size_};
    x = a.data();
    y = b.data();
    CHECK(x != y);
  }

  // b is deallocated first, then a is deallocated (LIFO)

  // allocate a first, then b
  auto a = DataBlock{chunk_size_};
  auto b = DataBlock{chunk_size_};

  auto ptr_a = a.data();
  auto ptr_b = b.data();
  CHECK(ptr_a != ptr_b);

  CHECK(x == ptr_a);
  CHECK(y == ptr_b);
}

TEST_CASE(
    "DataBlock: Test DataBlock allocation/deallocation from pool, etc",
    "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  auto a = DataBlock{chunk_size_};
  auto b = DataBlock{chunk_size_};
  auto c = DataBlock{chunk_size_};
  auto da = a.data();
  auto db = b.data();
  auto dc = c.data();

  CHECK(da != db);
  CHECK(da != dc);
  CHECK(db != dc);

  decltype(da) dd = nullptr;
  {
    auto d = DataBlock{chunk_size_};
    dd = d.data();
    CHECK(da != dd);
    CHECK(db != dd);
    CHECK(dc != dd);

    auto e = DataBlock{chunk_size_};
    auto de = e.data();
    CHECK(da != de);
    CHECK(db != de);
    CHECK(dc != de);
    CHECK(dd != de);
  }
  {
    auto d = DataBlock{chunk_size_};
    auto de = d.data();
    CHECK(dd == de);
    CHECK(da != de);
    CHECK(db != de);
    CHECK(dc != de);
  }
}

TEST_CASE(
    "DataBlock: Test DataBlock allocation/deallocation on copying, etc",
    "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  auto a = DataBlock{chunk_size_};
  auto b = DataBlock{chunk_size_};
  auto c = DataBlock{chunk_size_};
  auto da = a.data();
  auto db = b.data();
  auto dc = c.data();

  CHECK(da != db);
  CHECK(da != dc);
  CHECK(db != dc);

  auto test_use_counts = [](DataBlock& d, DataBlock& a) {
    auto dd = d.data();
    auto da = a.data();
    CHECK(da == dd);

    CHECK(d.use_count() == 2);
    CHECK(a.use_count() == 2);
    {
      auto e = a;
      auto de = e.data();
      CHECK(da == de);
      CHECK(e.use_count() == 3);
      CHECK(d.use_count() == 3);
      CHECK(a.use_count() == 3);
    }
    CHECK(d.use_count() == 2);
    CHECK(a.use_count() == 2);
  };
  auto test_use_counts_move = [](DataBlock& d, DataBlock& a) {
    auto dd = d.data();
    auto da = a.data();
    CHECK(da == dd);

    CHECK(d.use_count() == 1);
    CHECK(a.use_count() == 0);
    {
      auto e = d;
      auto de = e.data();
      CHECK(dd == de);
      CHECK(e.use_count() == 2);
      CHECK(d.use_count() == 2);
      CHECK(a.use_count() == 0);
    }
    CHECK(d.use_count() == 1);
    CHECK(a.use_count() == 0);
  };

  SECTION("Test uses with copy constructor") {
    {
      auto d = a;
      test_use_counts(d, a);
    }
    CHECK(a.use_count() == 1);
  }

  SECTION("Test copy constructor, ii") {
    {
      DataBlock d(a);
      test_use_counts(d, a);
    }
    CHECK(a.use_count() == 1);
  }
  SECTION("Test assignment") {
    {
      DataBlock d;
      d = a;
      test_use_counts(d, a);
    }
    CHECK(a.use_count() == 1);
  }

  SECTION("Test move constructor, ii") {
    {
      DataBlock d(std::move(b));
      test_use_counts_move(d, b);
    }
    CHECK(b.use_count() == 0);
  }
  SECTION("Test move assignment") {
    {
      DataBlock d;
      d = std::move(c);
      test_use_counts_move(d, c);
    }
    CHECK(c.use_count() == 0);
  }
}
/**
 * Verify the random-access range interface of a `DataBlock` by using
 * `std::fill` algorithm.
 */
void test_std_fill(size_t test_size) {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  auto a = DataBlock{test_size};
  auto b = DataBlock{test_size};
  auto c = DataBlock{test_size};
  CHECK(a.begin() + test_size == a.end());
  CHECK(b.begin() + test_size == b.end());
  CHECK(c.begin() + test_size == c.end());

  CHECK(a.size() == test_size);
  CHECK(b.size() == test_size);
  CHECK(c.size() == test_size);

  std::fill(a.begin(), a.end(), std::byte{0});
  std::fill(b.begin(), b.end(), std::byte{0});
  std::fill(c.begin(), c.end(), std::byte{0});

  a[33] = std::byte{19};
  CHECK(a[32] == std::byte{0});
  CHECK(a[33] == std::byte{19});
  CHECK(a[34] == std::byte{0});

  b[127] = std::byte{23};
  CHECK(b[126] == std::byte{0});
  CHECK(b[127] == std::byte{23});
  CHECK(b[128] == std::byte{0});

  c[432] = std::byte{29};
  CHECK(c[431] == std::byte{0});
  CHECK(c[432] == std::byte{29});
  CHECK(c[433] == std::byte{0});

  for (auto& j : a) {
    j = std::byte{23};
  }
  for (auto& j : b) {
    j = std::byte{23};
  }
  for (auto& j : c) {
    j = std::byte{29};
  }

  std::fill(a.begin(), a.end(), std::byte{0});
  std::fill(b.begin(), b.end(), std::byte{0});
  std::fill(c.begin(), c.end(), std::byte{0});
  CHECK(a.end() == std::find_if_not(a.begin(), a.end(), [](auto e) {
          return (std::byte{0} == e);
        }));
  CHECK(b.end() == std::find_if_not(b.begin(), b.end(), [](auto e) {
          return (std::byte{0} == e);
        }));
  CHECK(c.end() == std::find_if_not(c.begin(), c.end(), [](auto e) {
          return (std::byte{0} == e);
        }));

  std::fill(a.begin(), a.end(), std::byte{19});
  std::fill(b.begin(), b.end(), std::byte{23});
  std::fill(c.begin(), c.end(), std::byte{29});
  CHECK(a.end() == std::find_if_not(a.begin(), a.end(), [](auto e) {
          return (std::byte{19} == e);
        }));
  CHECK(b.end() == std::find_if_not(b.begin(), b.end(), [](auto e) {
          return (std::byte{23} == e);
        }));
  CHECK(c.end() == std::find_if_not(c.begin(), c.end(), [](auto e) {
          return (std::byte{29} == e);
        }));
}

/**
 * Verify the random-access range interface of a `DataBlock` by using
 * `std::fill` algorithm.
 */
TEST_CASE("DataBlock: Fill with std::fill", "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;

  SECTION("chunk_size") {
    test_std_fill(chunk_size_);
  }
  SECTION("chunk_size / 2 + 1") {
    test_std_fill(chunk_size_ / 2 + 1);
  }
  SECTION("chunk_size / 2 -1") {
    test_std_fill(chunk_size_ / 2 - 1);
  }
}

/**
 * Verify some properties of joined `DataBlock`s.
 */
void test_join(size_t test_size) {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  auto a = DataBlock{test_size};
  auto b = DataBlock{test_size};
  auto c = DataBlock{test_size};
  std::list<DataBlock> x{a, b, c};
  auto y = join(x);

  CHECK(a.begin() + test_size == a.end());
  CHECK(b.begin() + test_size == b.end());
  CHECK(c.begin() + test_size == c.end());

  CHECK(a.size() == test_size);
  CHECK(b.size() == test_size);
  CHECK(c.size() == test_size);

  CHECK(y.size() == (a.size() + b.size() + c.size()));

  for (auto& j : a) {
    j = std::byte{19};
  }
  for (auto& j : b) {
    j = std::byte{23};
  }
  for (auto& j : c) {
    j = std::byte{29};
  }
  auto e = std::find_if_not(y.begin(), y.end(), [](auto a) {
    return (std::byte{19} == a) || (std::byte{23} == a) || (std::byte{29} == a);
  });
  CHECK(e == y.end());

  size_t k = 0;
  for ([[maybe_unused]] auto& j : y) {
    ++k;
  }
  CHECK(k == y.size());

  for (auto& j : y) {
    j = std::byte{89};
  }

  auto f = std::find_if_not(
      a.begin(), a.end(), [](auto a) { return (std::byte{89} == a); });
  CHECK(f == a.end());

  for (auto& j : y) {
    j = std::byte{91};
  }
  auto g = std::find_if_not(
      b.begin(), b.end(), [](auto a) { return (std::byte{91} == a); });
  CHECK(g == b.end());

  for (auto& j : y) {
    j = std::byte{103};
  }
  auto h = std::find_if_not(
      c.begin(), c.end(), [](auto a) { return (std::byte{103} == a); });
  CHECK(h == c.end());
}

TEST_CASE("DataBlock: Join data_blocks (join view)", "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;

  SECTION("chunk_size") {
    test_join(chunk_size_);
  }
  SECTION("chunk_size / 2") {
    test_join(chunk_size_ / 2 + 1);
  }
  SECTION("chunk_size / 2") {
    test_join(chunk_size_ / 2 - 1);
  }
}

/**
 * Verify that joined `DataBlock`s operate as a forward range by using standard
 * library algorithms.
 */
void test_join_std_fill(size_t test_size) {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  auto a = DataBlock{test_size};
  auto b = DataBlock{test_size};
  auto c = DataBlock{test_size};
  CHECK(a.begin() + test_size == a.end());
  CHECK(b.begin() + test_size == b.end());
  CHECK(c.begin() + test_size == c.end());

  std::fill(a.begin(), a.end(), std::byte{0});
  std::fill(b.begin(), b.end(), std::byte{0});
  std::fill(c.begin(), c.end(), std::byte{0});

  std::list<DataBlock> x{a, b, c};
  auto y = join(x);

  CHECK(y.size() == (a.size() + b.size() + c.size()));

  auto e = std::find_if_not(
      y.begin(), y.end(), [](auto e) { return (std::byte{0} == e); });
  CHECK(e == y.end());

  std::fill(y.begin(), y.end(), std::byte{77});
  auto f = std::find_if_not(
      y.begin(), y.end(), [](auto e) { return (std::byte{77} == e); });
  CHECK(f == y.end());
  auto g = std::find_if_not(
      a.begin(), a.end(), [](auto e) { return (std::byte{77} == e); });
  CHECK(g == a.end());
  auto h = std::find_if_not(
      b.begin(), b.end(), [](auto e) { return (std::byte{77} == e); });
  CHECK(h == b.end());
  auto i = std::find_if_not(
      c.begin(), c.end(), [](auto e) { return (std::byte{77} == e); });
  CHECK(i == c.end());
}

TEST_CASE("DataBlock: Join data_blocks std::fill", "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;

  SECTION("chunk_size") {
    test_join_std_fill(chunk_size_);
  }
  SECTION("chunk_size / 2") {
    test_join_std_fill(chunk_size_ / 2 + 1);
  }
  SECTION("chunk_size / 2") {
    test_join_std_fill(chunk_size_ / 2 - 1);
  }
}

/**
 * Test `operator[]` of joined `DataBlock`s.
 */
void test_join_operator_bracket(size_t test_size) {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  auto a = DataBlock{test_size};
  auto b = DataBlock{test_size};
  auto c = DataBlock{test_size};
  CHECK(a.begin() + test_size == a.end());
  CHECK(b.begin() + test_size == b.end());
  CHECK(c.begin() + test_size == c.end());

  std::fill(a.begin(), a.end(), std::byte{33});
  std::fill(b.begin(), b.end(), std::byte{66});
  std::fill(c.begin(), c.end(), std::byte{99});

  std::vector<DataBlock> x{a, b, c};
  auto y = join(x);

  a[33] = std::byte{19};
  CHECK(a[32] == std::byte{33});
  CHECK(a[33] == std::byte{19});
  CHECK(a[34] == std::byte{33});

  b[127] = std::byte{23};
  CHECK(b[126] == std::byte{66});
  CHECK(b[127] == std::byte{23});
  CHECK(b[128] == std::byte{66});

  c[432] = std::byte{29};
  CHECK(c[431] == std::byte{99});
  CHECK(c[432] == std::byte{29});
  CHECK(c[433] == std::byte{99});

  CHECK(y[0] == std::byte{33});
  CHECK(y[32] == std::byte{33});
  CHECK(y[33] == std::byte{19});
  CHECK(y[34] == std::byte{33});
  CHECK(y[test_size + 126] == std::byte{66});
  CHECK(y[test_size + 127] == std::byte{23});
  CHECK(y[test_size + 128] == std::byte{66});
  CHECK(y[2 * test_size + 431] == std::byte{99});
  CHECK(y[2 * test_size + 432] == std::byte{29});
  CHECK(y[2 * test_size + 433] == std::byte{99});
}

TEST_CASE("DataBlock: Join data_blocks operator[]", "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;

  SECTION("chunk_size") {
    test_join_operator_bracket(chunk_size_);
  }
  SECTION("chunk_size / 2") {
    test_join_operator_bracket(chunk_size_ / 2 + 1);
  }
  SECTION("chunk_size / 2") {
    test_join_operator_bracket(chunk_size_ / 2 - 1);
  }
}

/**
 * Additional standard library uses of joined `DataBlock`s.
 */
void test_operator_bracket_loops(size_t test_size) {
  constexpr size_t chunk_size_ = 4'194'304;
  using DataBlock = DataBlock<chunk_size_>;

  auto a = DataBlock{test_size};
  auto b = DataBlock{test_size};
  auto c = DataBlock{test_size};

  CHECK(a.size() == test_size);
  CHECK(b.size() == test_size);
  CHECK(c.size() == test_size);

  std::vector<DataBlock> x{a, b, c};
  CHECK(x.size() == 3);

  auto y = join(x);

  CHECK(y.size() == a.size() + b.size() + c.size());
  CHECK(y.size() == 3 * test_size);

  /* Hmm...  Can't do arithmetic on std::byte */
  std::iota(
      reinterpret_cast<uint8_t*>(a.begin()),
      reinterpret_cast<uint8_t*>(a.end()),
      uint8_t{0});
  std::iota(
      reinterpret_cast<uint8_t*>(b.begin()),
      reinterpret_cast<uint8_t*>(b.end()),
      static_cast<uint8_t>(a.back()) + 1);
  std::iota(
      reinterpret_cast<uint8_t*>(c.begin()),
      reinterpret_cast<uint8_t*>(c.end()),
      static_cast<uint8_t>(b.back()) + 1);

  CHECK([&]() {
    uint8_t b{0};
    for (size_t i = 0; i < y.size(); ++i) {
      if (static_cast<uint8_t>(y[i]) != b) {
        std::cout << i << " " << static_cast<uint8_t>(y[i]) << " " << b
                  << std::endl;
        return false;
      }
      ++b;
    }
    return true;
  }());

  CHECK([&]() {
    uint8_t b{0};
    for (auto&& j : y) {
      if (static_cast<uint8_t>(j) != b) {
        std::cout << static_cast<uint8_t>(j) << " " << b << std::endl;
        return false;
      }
      ++b;
    }
    return true;
  }());

  uint8_t d{0};
  auto z = std::find_if_not(y.begin(), y.end(), [&d](auto e) {
    return static_cast<uint8_t>(e) == d++;
  });
  CHECK(z == y.end());

  /**
   * @todo Use proper checks for preprocessor directives to selectively include
   * test with `std::execution` policy.
   */
#if 0
  d = 0;
  auto u = std::find_if_not(
      std::execution::par_unseq, y.begin(), y.end(), [&d](auto e) {
        return static_cast<uint8_t>(e) == d++;
      });
  CHECK(u == y.end());
#endif
}

TEST_CASE("DataBlock: Join data_blocks loops operator[]", "[data_block]") {
  constexpr size_t chunk_size_ = 4'194'304;

  SECTION("chunk_size") {
    test_operator_bracket_loops(chunk_size_);
  }
  SECTION("chunk_size / 2") {
    test_operator_bracket_loops(chunk_size_ / 2 + 1);
  }
  SECTION("chunk_size / 2") {
    test_operator_bracket_loops(chunk_size_ / 2 - 1);
  }
}
