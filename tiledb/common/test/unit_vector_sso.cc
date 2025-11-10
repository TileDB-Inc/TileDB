/**
 * @file unit_vector_sso.cc
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
 * This file tests the `vector_sso` container.
 */

#include "tiledb/common/vector_sso.h"

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

#include <memory_resource>
#include <numeric>
#include <vector>

static constexpr uint64_t SSO_LENGTH = 4;

/**
 * Event representation an interaction with a memory allocator.
 */
struct alloc_or_dealloc {
  bool is_alloc_;
  uint64_t size_;

  static alloc_or_dealloc alloc(uint64_t size) {
    return alloc_or_dealloc{.is_alloc_ = true, .size_ = size};
  }

  static alloc_or_dealloc dealloc(uint64_t size) {
    return alloc_or_dealloc{.is_alloc_ = false, .size_ = size};
  }

  bool operator==(const alloc_or_dealloc&) const = default;
};

/*
static uint64_t v_construct = 0;
static uint64_t v_move = 0;
static uint64_t v_destruct = 0;

template <typename T>
struct vector_which_logs_constructor_destructor : public std::vector<T> {
  using Base = std::vector<T>;
  using Self = vector_which_logs_constructor_destructor<T>;

  vector_which_logs_constructor_destructor() {
    v_construct++;
  }

  vector_which_logs_constructor_destructor(std::initializer_list<T> init)
      : Base(init) {
    v_construct++;
  }

  vector_which_logs_constructor_destructor(
      vector_which_logs_constructor_destructor<T>&& movefrom)
      : Base(std::move(movefrom)) {
    v_move++;
  }

  template <std::input_iterator InputIt>
  vector_which_logs_constructor_destructor(InputIt start, InputIt end)
      : Base(start, end) {
    v_construct++;
  }

  ~vector_which_logs_constructor_destructor() {
    v_destruct++;
  }

  Self& operator=(const std::initializer_list<T>& init) {
    this->assign(init.begin(), init.end());
    return *this;
  }
};
*/

struct logging_memory_resource : public std::pmr::memory_resource {
  void* do_allocate(size_t bytes, size_t) override {
    events_.push_back(alloc_or_dealloc::alloc(bytes));
    return impl_.allocate(bytes);
  }

  void do_deallocate(void* p, size_t bytes, size_t) override {
    events_.push_back(alloc_or_dealloc::dealloc(bytes));
    impl_.deallocate(static_cast<uint8_t*>(p), bytes);
  }

  bool do_is_equal(
      const std::pmr::memory_resource& other) const noexcept override {
    // meh, not for production
    return &other == this;
  }

  /**
   * @return the total amount of memory which has been allocated but not freed
   */
  uint64_t outstanding() const {
    uint64_t r = 0;
    for (const auto& event : events_) {
      if (event.is_alloc_) {
        r += event.size_;
      } else {
        r -= event.size_;
      }
    }
    return r;
  }

  std::allocator<uint8_t> impl_;
  std::vector<alloc_or_dealloc> events_;
};

template <typename T, uint64_t N>
using my_vector_sso =
    tiledb::common::vector_sso<T, N, std::pmr::polymorphic_allocator<T>>;

TEST_CASE("vector_sso push_back") {
  logging_memory_resource mem;

  SECTION("uint64_t") {
    my_vector_sso<uint64_t, SSO_LENGTH> elts(
        std::pmr::polymorphic_allocator<uint64_t>{&mem});
    CHECK(elts.empty());
    CHECK(elts.size() == 0);

    for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
      elts.push_back(i);

      CHECK(!elts.empty());
      CHECK(elts.size() == i + 1);

      CHECK(elts.back() == i);

      for (uint64_t j = 0; j <= i; j++) {
        CHECK(elts.at(j) == j);
        CHECK(elts[j] == j);
      }

      // iterator test
      uint64_t pos = 0;
      for (const uint64_t& j : elts) {
        CHECK(j == elts.at(pos++));
      }
      CHECK(pos == i + 1);

      CHECK_THROWS_AS(elts.at(i + 1), std::out_of_range);

      if (i < SSO_LENGTH) {
        CHECK(mem.events_.empty());
      } else if (i < SSO_LENGTH * 2) {
        CHECK(
            mem.events_ ==
            std::vector<alloc_or_dealloc>{
                alloc_or_dealloc::alloc(sizeof(uint64_t) * SSO_LENGTH * 2)});
      } else {
        CHECK(
            mem.events_ ==
            std::vector<alloc_or_dealloc>{
                alloc_or_dealloc::alloc(sizeof(uint64_t) * SSO_LENGTH * 2),
                alloc_or_dealloc::alloc(sizeof(uint64_t) * SSO_LENGTH * 4),
                alloc_or_dealloc::dealloc(sizeof(uint64_t) * SSO_LENGTH * 2),
            });
      }
    }
  }

  SECTION("std::shared_ptr") {
    using E = std::shared_ptr<uint64_t>;
    static_assert(!std::is_trivially_copyable_v<E>);
    static_assert(std::is_move_constructible_v<E>);

    // make a std::vector of the same stuff for comparison
    std::vector<std::shared_ptr<uint64_t>> expect;
    for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
      expect.emplace_back(new uint64_t(i));
    }
    for (const auto& ptr : expect) {
      REQUIRE(ptr.use_count() == 1);
    }

    {
      my_vector_sso<E, SSO_LENGTH> elts(
          std::pmr::polymorphic_allocator<E>{&mem});
      CHECK(elts.empty());
      CHECK(elts.size() == 0);

      for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
        elts.emplace_back(expect[i]);

        CHECK(!elts.empty());
        CHECK(elts.size() == i + 1);

        for (uint64_t j = 0; j <= i; j++) {
          CHECK(elts.at(j) == expect[j]);
          CHECK(elts[j] == expect[j]);
          CHECK(expect[j].use_count() == 2);
        }

        // iterator test
        uint64_t pos = 0;
        for (const auto& elt : elts) {
          CHECK(elt == expect[pos++]);
        }
        CHECK(pos == i + 1);

        CHECK_THROWS_AS(elts.at(i + 1), std::out_of_range);
      }
    }

    // all of the elements should have been destructed
    for (const auto& ptr : expect) {
      REQUIRE(ptr.use_count() == 1);
    }
  }

  REQUIRE(mem.outstanding() == 0);
}

TEST_CASE("vector_sso copy constructor") {
  logging_memory_resource mem;

  SECTION("uint64_t") {
    my_vector_sso<uint64_t, SSO_LENGTH> elts(
        std::pmr::polymorphic_allocator<uint64_t>{&mem});

    SECTION("SSO") {
      for (uint64_t i = 0; i < SSO_LENGTH; i++) {
        elts.push_back((i + 1) * (i + 1));
      }

      my_vector_sso<uint64_t, SSO_LENGTH> copy(elts);
      CHECK(elts == copy);
      CHECK(elts.size() == copy.size());
      CHECK(elts.capacity() == copy.capacity());
      CHECK(elts.next_capacity() == copy.next_capacity());
    }

    SECTION("Heap") {
      for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
        elts.push_back((i + 1) * (i + 1));
      }

      my_vector_sso<uint64_t, SSO_LENGTH> copy(elts);
      CHECK(elts == copy);
      CHECK(elts.size() == copy.size());
      CHECK(elts.capacity() == copy.capacity());
      CHECK(elts.next_capacity() == copy.next_capacity());
    }
  }

  SECTION("std::shared_ptr") {
    using E = std::shared_ptr<uint64_t>;

    std::vector<E> ref;
    for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
      ref.emplace_back(new uint64_t((i + 1) * (i + 1)));
    }

    my_vector_sso<E, SSO_LENGTH> elts(std::pmr::polymorphic_allocator<E>{&mem});

    SECTION("SSO") {
      for (uint64_t i = 0; i < SSO_LENGTH; i++) {
        elts.push_back(ref[i]);
      }
      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < elts.size()) {
          REQUIRE(ref[p].use_count() == 2);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }

      my_vector_sso<E, SSO_LENGTH> copy(elts);
      CHECK(elts == copy);
      CHECK(elts.size() == copy.size());
      CHECK(elts.capacity() == copy.capacity());
      CHECK(elts.next_capacity() == copy.next_capacity());
      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < elts.size()) {
          REQUIRE(ref[p].use_count() == 3);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }
    }

    SECTION("Heap") {
      for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
        elts.push_back(ref[i]);
      }
      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < elts.size()) {
          REQUIRE(ref[p].use_count() == 2);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }

      my_vector_sso<E, SSO_LENGTH> copy(elts);
      CHECK(elts == copy);
      CHECK(elts.size() == copy.size());
      CHECK(elts.capacity() == copy.capacity());
      CHECK(elts.next_capacity() == copy.next_capacity());
      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < elts.size()) {
          REQUIRE(ref[p].use_count() == 3);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }
    }
  }
}

TEST_CASE("vector_sso move constructor") {
  logging_memory_resource mem;

  SECTION("uint64_t") {
    my_vector_sso<uint64_t, SSO_LENGTH> movesrc(
        std::pmr::polymorphic_allocator<uint64_t>{&mem});

    SECTION("SSO") {
      std::vector<uint64_t> ref;
      for (uint64_t i = 0; i < SSO_LENGTH; i++) {
        ref.push_back((i + 1) * (i + 1));
      }

      movesrc.assign(ref.begin(), ref.end());
      REQUIRE(movesrc == ref);

      my_vector_sso<uint64_t, SSO_LENGTH> movedst(std::move(movesrc));
      CHECK(movedst == ref);
      CHECK(movedst.capacity() == SSO_LENGTH);

      CHECK(movesrc.empty());
    }

    SECTION("Heap") {
      std::vector<uint64_t> ref;
      for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
        ref.push_back((i + 1) * (i + 1));
      }

      movesrc.assign(ref.begin(), ref.end());
      REQUIRE(movesrc == ref);

      my_vector_sso<uint64_t, SSO_LENGTH> movedst(std::move(movesrc));
      CHECK(movedst == ref);
      CHECK(movedst.capacity() == SSO_LENGTH * 4);

      CHECK(movesrc.empty());
      CHECK(movesrc.capacity() == SSO_LENGTH);
    }
  }

  SECTION("std::shared_ptr") {
    using E = std::shared_ptr<uint64_t>;

    my_vector_sso<E, SSO_LENGTH> movesrc(
        std::pmr::polymorphic_allocator<E>{&mem});

    SECTION("SSO") {
      std::vector<E> ref;
      for (uint64_t i = 0; i < SSO_LENGTH; i++) {
        ref.emplace_back(new uint64_t((i + 1) * (i + 1)));
      }

      for (uint64_t i = 0; i < SSO_LENGTH; i++) {
        movesrc.push_back(ref[i]);
      }
      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < movesrc.size()) {
          REQUIRE(ref[p].use_count() == 2);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }

      my_vector_sso<E, SSO_LENGTH> movedst(std::move(movesrc));
      CHECK(movedst == ref);
      CHECK(movedst.capacity() == SSO_LENGTH);

      CHECK(movesrc.empty());
      CHECK(movesrc.capacity() == SSO_LENGTH);

      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < movedst.size()) {
          REQUIRE(ref[p].use_count() == 2);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }
    }

    SECTION("Heap") {
      std::vector<E> ref;
      for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
        ref.emplace_back(new uint64_t((i + 1) * (i + 1)));
      }

      for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
        movesrc.push_back(ref[i]);
      }
      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < movesrc.size()) {
          REQUIRE(ref[p].use_count() == 2);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }

      my_vector_sso<E, SSO_LENGTH> movedst(std::move(movesrc));
      CHECK(movedst == ref);
      CHECK(movedst.capacity() == SSO_LENGTH * 4);

      CHECK(movesrc.empty());
      CHECK(movesrc.capacity() == SSO_LENGTH);

      for (uint64_t p = 0; p < ref.size(); p++) {
        if (p < movedst.size()) {
          REQUIRE(ref[p].use_count() == 2);
        } else {
          REQUIRE(ref[p].use_count() == 1);
        }
      }
    }
  }
}

TEST_CASE("vector_sso reserve") {
  logging_memory_resource mem;

  SECTION("uint64_t") {
    my_vector_sso<uint64_t, SSO_LENGTH> elts(
        std::pmr::polymorphic_allocator<uint64_t>{&mem});

    CHECK(elts.capacity() == SSO_LENGTH);
    CHECK(elts.next_capacity() == SSO_LENGTH * 2);

    SECTION("Empty up to N") {
      elts.reserve(SSO_LENGTH);
      CHECK(elts.capacity() == SSO_LENGTH);
      CHECK(elts.next_capacity() == SSO_LENGTH * 2);
    }

    SECTION("Empty to more than N") {
      elts.reserve(SSO_LENGTH * 8);
      CHECK(elts.capacity() == SSO_LENGTH);
      CHECK(elts.next_capacity() == SSO_LENGTH * 8);

      for (uint64_t i = 0; i < SSO_LENGTH + 1; i++) {
        elts.push_back(i);

        if (i < SSO_LENGTH) {
          CHECK(elts.capacity() == SSO_LENGTH);
          CHECK(elts.next_capacity() == SSO_LENGTH * 8);
        } else {
          CHECK(elts.capacity() == SSO_LENGTH * 8);
          CHECK(elts.next_capacity() == SSO_LENGTH * 16);
        }
      }

      CHECK(
          mem.events_ == std::vector<alloc_or_dealloc>{alloc_or_dealloc::alloc(
                             sizeof(uint64_t) * SSO_LENGTH * 8)});

      std::vector<uint64_t> expect(SSO_LENGTH + 1);
      std::iota(expect.begin(), expect.end(), 0);
      CHECK(elts == expect);
    }

    SECTION("Alloc to less than N") {
    }

    SECTION("Alloc to same capacity") {
    }

    SECTION("Alloc to more than N") {
    }
  }

  SECTION("std::shared_ptr") {
  }

  REQUIRE(mem.outstanding() == 0);
}

TEST_CASE("vector_sso resize") {
  logging_memory_resource mem;

  SECTION("uint64_t") {
    my_vector_sso<uint64_t, SSO_LENGTH> elts(
        std::pmr::polymorphic_allocator<uint64_t>{&mem});

    elts.resize(0);
    CHECK(elts.empty());

    elts.resize(1);
    CHECK(elts == std::initializer_list<uint64_t>{0});

    elts.resize(0);
    CHECK(elts.empty());

    elts.resize(SSO_LENGTH);
    CHECK(elts == std::vector<uint64_t>(SSO_LENGTH, 0));

    elts.resize(1);
    CHECK(elts == std::initializer_list<uint64_t>{0});

    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == std::vector<uint64_t>(SSO_LENGTH + 1, 0));

    elts[2] = 123;
    elts.resize(3);
    CHECK(elts == std::initializer_list<uint64_t>{0, 0, 123});

    elts.resize(2);
    CHECK(elts == std::initializer_list<uint64_t>{0, 0});

    elts.resize(3);
    CHECK(elts == std::initializer_list<uint64_t>{0, 0, 0});

    elts.resize(SSO_LENGTH * 2);
    CHECK(elts == std::vector<uint64_t>(SSO_LENGTH * 2, 0));

    std::vector<uint64_t> expect321(SSO_LENGTH * 2, 0);
    expect321[SSO_LENGTH + 1] = 321;
    elts[SSO_LENGTH + 1] = 321;
    CHECK(elts == expect321);

    expect321.resize(SSO_LENGTH + 1);
    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == expect321);

    elts.resize(SSO_LENGTH);
    CHECK(elts == std::vector<uint64_t>(SSO_LENGTH, 0));

    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == std::vector<uint64_t>(SSO_LENGTH + 1, 0));
  }

  SECTION("std::shared_ptr") {
    using E = std::shared_ptr<uint64_t>;
    static_assert(!std::is_trivially_copyable_v<E>);
    static_assert(std::is_move_constructible_v<E>);

    my_vector_sso<E, SSO_LENGTH> elts(std::pmr::polymorphic_allocator<E>{&mem});

    elts.resize(0);
    CHECK(elts.empty());

    elts.resize(1);
    CHECK(elts == std::initializer_list<E>{{}});

    elts.resize(0);
    CHECK(elts.empty());

    elts.resize(SSO_LENGTH);
    CHECK(elts == std::vector<E>(SSO_LENGTH));

    elts.resize(1);
    CHECK(elts == std::initializer_list<E>{{}});

    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == std::vector<E>(SSO_LENGTH + 1));

    std::shared_ptr<uint64_t> s123(new uint64_t(123));
    elts[2] = s123;
    CHECK(s123.use_count() == 2);
    elts.resize(3);
    CHECK(elts == std::initializer_list<E>{{}, {}, s123});
    CHECK(s123.use_count() == 2);

    elts.resize(2);
    CHECK(elts == std::initializer_list<E>{{}, {}});
    CHECK(s123.use_count() == 1);

    elts.resize(3);
    CHECK(elts == std::initializer_list<E>{{}, {}, {}});

    elts.resize(SSO_LENGTH * 2);
    CHECK(elts == std::vector<E>(SSO_LENGTH * 2));

    std::shared_ptr<uint64_t> s321(new uint64_t(321));
    std::vector<E> expect321(SSO_LENGTH * 2);
    expect321[SSO_LENGTH + 1] = s321;
    CHECK(s321.use_count() == 2);
    elts[SSO_LENGTH + 1] = s321;
    CHECK(s321.use_count() == 3);
    CHECK(elts == expect321);

    expect321.resize(SSO_LENGTH + 1);
    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == expect321);
    CHECK(s321.use_count() == 1);

    elts.resize(SSO_LENGTH);
    CHECK(elts == std::vector<E>(SSO_LENGTH));

    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == std::vector<E>(SSO_LENGTH + 1));

    CHECK(s321.use_count() == 1);
  }

  REQUIRE(mem.outstanding() == 0);
}

TEST_CASE("vector_sso shrink_to_fit") {
  logging_memory_resource mem;

  SECTION("uint64_t") {
    my_vector_sso<uint64_t, SSO_LENGTH> elts(
        std::pmr::polymorphic_allocator<uint64_t>{&mem});

    std::vector<uint64_t> expect;
    for (uint64_t i = 0; i < SSO_LENGTH * 2; i++) {
      expect.push_back((i + 1) * (i + 1));
    }

    elts.shrink_to_fit();
    CHECK(elts.empty());
    CHECK(elts.capacity() == SSO_LENGTH);

    elts.resize(SSO_LENGTH * 2);
    for (uint64_t i = 0; i < elts.size(); i++) {
      elts[i] = (i + 1) * (i + 1);
    }
    CHECK(elts == expect);
    CHECK(elts.capacity() == SSO_LENGTH * 2);

    elts.shrink_to_fit();
    CHECK(elts == expect);
    CHECK(elts.capacity() == SSO_LENGTH * 2);

    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == std::span(expect.begin(), expect.begin() + SSO_LENGTH + 1));
    CHECK(elts.capacity() == SSO_LENGTH * 2);

    elts.shrink_to_fit();
    CHECK(elts == std::span(expect.begin(), expect.begin() + SSO_LENGTH + 1));
    CHECK(elts.capacity() == SSO_LENGTH + 1);
  }

  SECTION("std::shared_ptr") {
    using E = std::shared_ptr<uint64_t>;
    static_assert(!std::is_trivially_copyable_v<E>);
    static_assert(std::is_move_constructible_v<E>);

    // make a std::vector of the same stuff for comparison
    std::vector<std::shared_ptr<uint64_t>> expect;
    for (uint64_t i = 0; i < SSO_LENGTH * 4; i++) {
      expect.emplace_back(new uint64_t((i + 1) * (i + 1)));
    }
    for (const auto& ptr : expect) {
      REQUIRE(ptr.use_count() == 1);
    }

    my_vector_sso<E, SSO_LENGTH> elts(
        std::pmr::polymorphic_allocator<uint64_t>{&mem});
    elts.shrink_to_fit();
    CHECK(elts.empty());
    CHECK(elts.capacity() == SSO_LENGTH);

    elts.resize(SSO_LENGTH * 4);
    for (uint64_t i = 0; i < elts.size(); i++) {
      elts[i] = expect[i];
    }
    for (const auto& ptr : expect) {
      REQUIRE(ptr.use_count() == 2);
    }
    CHECK(elts == expect);
    CHECK(elts.capacity() == SSO_LENGTH * 4);

    elts.shrink_to_fit();
    CHECK(elts == expect);
    CHECK(elts.capacity() == SSO_LENGTH * 4);
    for (const auto& ptr : expect) {
      REQUIRE(ptr.use_count() == 2);
    }

    elts.resize(SSO_LENGTH + 1);
    CHECK(elts == std::span(expect.begin(), expect.begin() + SSO_LENGTH + 1));
    CHECK(elts.capacity() == SSO_LENGTH * 4);
    for (uint64_t i = 0; i < expect.size(); i++) {
      if (i < SSO_LENGTH + 1) {
        REQUIRE(expect[i].use_count() == 2);
      } else {
        REQUIRE(expect[i].use_count() == 1);
      }
    }

    elts.shrink_to_fit();
    CHECK(elts == std::span(expect.begin(), expect.begin() + SSO_LENGTH + 1));
    CHECK(elts.capacity() == SSO_LENGTH + 1);
    for (uint64_t i = 0; i < expect.size(); i++) {
      if (i < SSO_LENGTH + 1) {
        REQUIRE(expect[i].use_count() == 2);
      } else {
        REQUIRE(expect[i].use_count() == 1);
      }
    }
  }

  REQUIRE(mem.outstanding() == 0);
}

TEST_CASE("vector_sso reverse iteration") {
  vector_sso<uint64_t, SSO_LENGTH> elts;

  const uint64_t num_elements = GENERATE(0, 1, SSO_LENGTH, SSO_LENGTH * 2);
  for (uint64_t i = 0; i < num_elements; i++) {
    elts.push_back(i);
  }

  std::vector<uint64_t> expect;
  for (uint64_t i = num_elements; i > 0; i--) {
    expect.push_back(i - 1);
  }

  std::vector<uint64_t> rev(elts.rbegin(), elts.rend());
  CHECK(rev == expect);
}
