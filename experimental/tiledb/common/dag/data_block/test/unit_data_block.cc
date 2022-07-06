/**
 * @file unit_data_blok.cc
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
 * Tests the data_block class.
 */

#include "unit_data_block.h"
#include <list>
#include <memory>
#include <vector>
#include "experimental/tiledb/common/dag/data_block/data_block.h"
#include "experimental/tiledb/common/dag/utils/range_join.h"

using namespace tiledb::common;

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

TEST_CASE("DataBlock: Test create DataBlock", "[data_block]") {
  auto db = DataBlock{chunk_size_};
  db_test_0(db);
  db_test_1(db);
}



TEST_CASE(
    "DataBlock: Test create DataBlock with std::allocator<std::byte>",
    "[data_block]") {
  auto db = DataBlockImpl<std::allocator<std::byte>>{chunk_size_};
  db_test_0(db);
  db_test_1(db);

  auto dc = DataBlock{chunk_size_};
  db_test_0(dc);
  db_test_1(dc);
}


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

TEST_CASE("DataBlock: Iterate through data_block", "[data_block]") {
  auto db = DataBlockImpl<std::allocator<std::byte>>{};
  db_test_2(db);
  auto dc = DataBlock{chunk_size_};
  db_test_2(dc);
}

TEST_CASE("DataBlock: Iterate through 8 data_blocks", "[data_block]") {
  for (size_t i = 0; i < 8; ++i) {
    auto db = DataBlockImpl<std::allocator<std::byte>>{};
    db_test_2(db);
  }
  for (size_t i = 0; i < 8; ++i) {
    auto db = DataBlock{chunk_size_};
    db_test_2(db);
  }
}

TEST_CASE("DataBlock: Get span", "[data_block]") {
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

TEST_CASE("DataBlock: Test resize", "[data_block]") {
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

#if 0
TEST_CASE(
    "DataBlock: Test allocation and deallocation of DataBlock", "[data_block") {
  auto a = DataBlock{chunk_size_};
  auto ptr_a = a.data();

  // Don't do this -- it will call destructor again when a goes out of scope
  // (which would be bad)
  a.~DataBlock();


  auto b = DataBlock{chunk_size_};
  auto ptr_b = b.data();
  CHECK(ptr_a == ptr_b);
}
#endif

TEST_CASE(
    "DataBlock: Test deallocation of DataBlock on destruction",
    "[data_block]") {
  std::byte* x;
  std::byte* y;

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

void test_std_fill(size_t test_size) {
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

TEST_CASE("DataBlock: Fill with std::fill", "[data_block]") {
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

void test_join(size_t test_size) {
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

void test_join_std_fill(size_t test_size) {
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

void test_join_operator_bracket(size_t test_size) {
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

void test_operator_bracket_loops(size_t test_size) {
  auto a = DataBlock{test_size};
  auto b = DataBlock{test_size};
  auto c = DataBlock{test_size};

  std::vector<DataBlock> x{a, b, c};
  auto y = join(x);

  CHECK(y.size() == a.size() + b.size() + c.size());

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
}

TEST_CASE("DataBlock: Join data_blocks loops operator[]", "[data_block]") {
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
