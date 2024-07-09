/**
 * @file   unit_iterator_facade.cc
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
 * This file implements unit tests for `iterator_facade` that will be used as
 * part of TileDB external sort.  The tests include those from
 * `iterator_facade.test.cpp` from the `vectorofbool/neo-fun` github repository,
 * (https://github.com/vector-of-bool/neo-fun/commit/b6c38c8 Mar 21, 2024). It
 * is used here under the terms of the Boost Software License 1.0 and is
 * Copyright (c) the author(s).
 *
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <numeric>
#include <ranges>
#include "tiledb/common/util/detail/iterator_facade.h"

TEST_CASE("iterator_facade: Null test", "[iterator_facade][null_test]") {
  REQUIRE(true);
}

struct null_iterator : public iterator_facade<null_iterator> {};

TEST_CASE("iterator_facade: null_iterator", "[iterator_facade]") {
  null_iterator it;
  (void)it;
}

struct minimal_iterator : public iterator_facade<minimal_iterator> {
  int value = 0;

  int dereference() const {
    return value;
  }
  int distance_to(minimal_iterator o) const {
    return *o - value;
  }
  void advance(int off) {
    value += off;
  }

  // This should be inferred, but it seems to be necessary to specify it here.
  // @todo Implement inference for this in iterator_facade.
  bool operator==(minimal_iterator o) const noexcept {
    return *o == **this;
  }
};

TEST_CASE("iterator_facade: minimal_iterator", "[iterator_facade]") {
  CHECK(std::input_iterator<minimal_iterator>);
  CHECK(!std::output_iterator<minimal_iterator, int>);
  CHECK(std::forward_iterator<minimal_iterator>);
  CHECK(std::bidirectional_iterator<minimal_iterator>);
  CHECK(std::random_access_iterator<minimal_iterator>);
}

/*
 * The following are from vector-of-bool neo-fun/iterator_facade.test.cc
 */
namespace {

template <typename Iter>
class as_string_iterator
    : public iterator_wrapper_facade<as_string_iterator<Iter>, Iter> {
  using as_string_iterator::iterator_wrapper_facade::iterator_wrapper_facade;

 public:
  std::string dereference() const noexcept {
    return std::to_string(*this->wrapped_iterator);
  }
};

template <typename It>
as_string_iterator(It) -> as_string_iterator<It>;

class iota_iterator : public iterator_facade<iota_iterator> {
  int _value = 0;

 public:
  iota_iterator() = default;
  explicit iota_iterator(int i)
      : _value(i) {
  }

  // Since this is a fake iterator, and is maintaining its "value" itself,
  // we can't return a reference to the value.
  int dereference() const noexcept {
    return _value;
  }

  // Trying to do this breaks iterator_traits for iterator_facade.
  // Since this is just a contrived situation, it's not worth trying to fix.
  // Note that it will work -- we can set values, but the iterator_traits
  // will not be correct -- and wil cause compiler error if we try to use.
  // int& dereference() noexcept { return _value; }

  void advance(int off) noexcept {
    _value += off;
  }
  int distance_to(iota_iterator o) const noexcept {
    return *o - **this;
  }
  bool operator==(iota_iterator o) const noexcept {
    return *o == **this;
  }
};

}  // namespace

TEST_CASE("Create an iota_iterator") {
  iota_iterator it;
  iota_iterator stop{44};

  CHECK(std::input_iterator<iota_iterator>);
  CHECK(!std::output_iterator<iota_iterator, int>);
  CHECK(std::forward_iterator<iota_iterator>);
  CHECK(std::bidirectional_iterator<iota_iterator>);
  CHECK(std::random_access_iterator<iota_iterator>);

  CHECK((stop - it) == 44);
  CHECK(*(stop - 4) == 40);
  CHECK(it < stop);
  CHECK(it <= stop);
  CHECK_FALSE(it > stop);
  CHECK_FALSE(it >= stop);
  CHECK(std::distance(it, stop) == 44);

  CHECK(it[33] == 33);
  CHECK(it[-9] == -9);
  CHECK(stop[2] == 46);
  CHECK(stop[-44] == 0);

  CHECK((stop - it) == 44);
  CHECK((it - stop) == -44);

  CHECK(it != stop);
  CHECK((it + 44) == stop);
  CHECK(it == (stop - 44));
}

namespace {

class mutable_iota_iterator : public iterator_facade<mutable_iota_iterator> {
  mutable int _value{0};

 public:
  mutable_iota_iterator() = default;
  explicit mutable_iota_iterator(int i)
      : _value(i) {
  }

  // This is the same kind of contrived iterator as before, but here the
  // iterator is referring to an external value, which it can return by
  // reference, even if the iterator is const.
  int& dereference() const noexcept {
    return _value;
  }

  void advance(int off) noexcept {
    _value += off;
  }
  int distance_to(mutable_iota_iterator o) const noexcept {
    return *o - **this;
  }
  bool operator==(mutable_iota_iterator o) const noexcept {
    return *o == **this;
  }
};
}  // namespace

TEST_CASE("mutable_iota_iterator") {
  CHECK(std::input_iterator<mutable_iota_iterator>);
  CHECK(std::output_iterator<mutable_iota_iterator, int>);
  CHECK(std::forward_iterator<mutable_iota_iterator>);
  CHECK(std::bidirectional_iterator<mutable_iota_iterator>);
  CHECK(std::random_access_iterator<mutable_iota_iterator>);

  mutable_iota_iterator it;
  CHECK(*it == 0);
  *it = 42;
  CHECK(*it == 42);
}

TEST_CASE("arrow_proxy") {
  arrow_proxy<std::string> s{""};
  s->append("Hello, ");
  s->append("world!");
  CHECK(*s.operator->() == "Hello, world!");
}

TEST_CASE("Trivial iterator") {
  struct deref_iter : iterator_facade<deref_iter> {
    int* value = nullptr;
    auto& dereference() /*const*/ noexcept {
      // cppcheck-suppress nullPointer
      return *value;
    }
    auto dereference() const noexcept {
      return *value;
    }

    deref_iter(int& i)
        : value(&i) {
    }
  };

  // Just some very simple tests.  The trivial iterator does not even have
  // increment or decrement.

  int i = 12;
  deref_iter it{i};

  CHECK(*it == 12);
  i = 7;
  CHECK(*it == 7);
}

TEST_CASE("Single-pass iterator") {
  struct in_iter : iterator_facade<in_iter> {
    int value = 0;
    enum { single_pass_iterator = true };

    const int& dereference() const noexcept {
      return value;
    }
    void increment() noexcept {
      ++value;
    }
  };

  in_iter it;
  CHECK(*it == 0);
  static_assert(std::is_same_v<decltype(++it), in_iter&>);
  ++it;
  CHECK(*it == 1);
  static_assert(std::is_void_v<decltype(it++)>);
}

TEST_CASE("Transforming iterator") {
  std::vector<int> values = {1, 2, 3, 4};
  as_string_iterator it{values.begin()};

  CHECK(*it == "1");
  ++it;
  CHECK(*it == "2");
  CHECK_FALSE(it == as_string_iterator(values.begin()));
  // Post-increment returns a copy of the iterator
  auto copy = it++;
  CHECK(*copy == "2");

  static_assert(
      std::is_same_v<decltype(it.operator->()), arrow_proxy<std::string>>);
  // Even though we are acting on a temporary, the append() will return a new
  // string
  auto thirty_four = it->append("4");
  CHECK(thirty_four == "34");

  copy = copy - 1;
  CHECK(*copy == "1");
  CHECK(*(copy + 3) == "4");
  CHECK(*(3 + copy) == "4");

  ++copy;
  auto copy2 = copy--;
  CHECK(*copy == "1");
  CHECK(*copy2 == "2");

  // Advance by a negative number created from an unsigned
  CHECK(*copy == "1");
  ++copy;
  copy -= 1u;
  CHECK(*copy == "1");
}

TEST_CASE("Sentinel support") {
  struct until_7_iter : iterator_facade<until_7_iter> {
    int value = 0;
    struct sentinel_type {};

    auto dereference() const noexcept {
      return value;
    }
    auto increment() noexcept {
      ++value;
    }

    auto distance_to(sentinel_type) const noexcept {
      return 7 - value;
    }
    bool operator==(sentinel_type s) const noexcept {
      return distance_to(s) == 0;
    }
  };

  struct seven_range {
    auto begin() {
      return until_7_iter();
    }
    auto end() {
      return until_7_iter::sentinel_type();
    }
  };

  int sum = 0;
  for (auto i : seven_range()) {
    sum += i;
    CHECK(i < 7);
  }
  CHECK(sum == (1 + 2 + 3 + 4 + 5 + 6));

  auto it = seven_range().begin();
  auto stop = seven_range().end();
  CHECK(it != stop);
  CHECK(stop != it);
  CHECK_FALSE(it == stop);
  CHECK_FALSE(stop == it);

#if !_MSC_VER
  /// XXX: Last checked, MSVC has an issue finding the correct operator-() via
  /// ADL. If you're using MSVC and reading this comment in the future, revive
  /// this snippet and try again.
  CHECK((until_7_iter::sentinel_type() - it) == 7);
#endif
}

namespace {
enum class month : int {
  january,
  february,
  march,
  april,
  may,
  june,
  july,
  august,
  september,
  october,
  november,
  december,
  end,
};

/*
 * Example of a simple iterator that iterates over the months of the year,
 * copied from
 * `https://vector-of-bool.github.io/2020/06/13/cpp20-iter-facade.html`
 */
class month_iterator : public iterator_facade<month_iterator> {
  month _cur = month::january;

 public:
  month_iterator() = default;
  explicit month_iterator(month m)
      : _cur(m) {
  }

  auto begin() const {
    return *this;
  }
  auto end() const {
    return month_iterator(month(int(month::december) + 1));
  }

  void increment() {
    _cur = month(int(_cur) + 1);
  }
  void decrement() {
    _cur = month(int(_cur) - 1);
  }
  void advance(int off) {
    _cur = month(int(_cur) + off);
  }

  const month& dereference() const {
    return _cur;
  }

  int distance_to(month_iterator other) const {
    return int(other._cur) - int(_cur);
  }
  // This shouldn't be necessary, but seems to be required to determine
  // the iterator category.
  bool operator==(month_iterator other) const {
    return _cur == other._cur;
  }
};
}  // namespace

TEMPLATE_TEST_CASE(
    "iterator_facade: month_iterator", "[iterator_facade]", month_iterator) {
  CHECK(!std::output_iterator<month_iterator, month>);

  CHECK(std::forward_iterator<TestType>);
  CHECK(std::bidirectional_iterator<TestType>);
  CHECK(std::random_access_iterator<TestType>);

  TestType it;
  CHECK(*it == month::january);
  ++it;
  CHECK(*it == month::february);
  ++it;
  CHECK(*it == month::march);
  ++it;
  CHECK(*it == month::april);
  ++it;
  CHECK(*it == month::may);
  ++it;
  CHECK(*it == month::june);
  ++it;
  CHECK(*it == month::july);
  ++it;
  CHECK(*it == month::august);
  ++it;
  CHECK(*it == month::september);
  ++it;
  CHECK(*it == month::october);
  ++it;
  CHECK(*it == month::november);
  ++it;
  CHECK(*it == month::december);

  // We should be able to increment once more and get the sentinel value,
  // but this is not working for some reason.
  // @todo Fix this.
  // ++it;
  //  CHECK(it == month_iterator::sentinel_type());
}

/*
 * Test arrow and arrow_proxy
 */
struct bar {
  int val = -1;
  bar()
      : val(199) {
  }
  explicit bar(int x)
      : val(x) {
  }
  int set(int x) {
    auto tmp = val;
    val = x;
    return tmp;
  }
  int get() {
    return val;
  }
};

struct foo {
  std::vector<bar> values{
      bar{0},
      bar{0},
      bar{0},
      bar{0},
      bar{0},
      bar{0},
      bar{0},
      bar{0},
      bar{0},
      bar{0}};

  class iterator : public iterator_facade<iterator> {
    size_t index_ = 0;
    std::vector<bar>::iterator values_iter;

   public:
    iterator() = delete;
    iterator(std::vector<bar>::iterator values_iter, size_t index = 0)
        : index_(index)
        , values_iter(values_iter) {
    }

    auto& dereference() const {
      return values_iter[index_];
    }
    // auto &dereference() { return values_iter[index_]; }

    bool equal_to(iterator o) const {
      return index_ == o.index_;
    }
    void advance(int n) {
      index_ += n;
    }
    size_t distance_to(iterator o) const {
      return o.index_ - index_;
    }
  };

  auto begin() {
    return iterator{values.begin()};
  }
  auto end() {
    return iterator{values.begin(), size(values)};
  }
};

TEST_CASE("iterator_facade: arrow") {
  std::vector<int> x(10);
  foo f;
  auto it = f.begin();
  CHECK((*it).get() == 0);
  CHECK(it[0].get() == 0);
  CHECK(it->get() == 0);

  CHECK((*(it + 1)).get() == 0);
  CHECK(it[1].get() == 0);
  CHECK((*(it + 1)).set(42) == 0);
  CHECK((*(it + 1)).get() == 42);
  CHECK(it[1].get() == 42);
  ++it;
  CHECK((*(it + 0)).get() == 42);
  CHECK(it[0].get() == 42);
  --it;
  CHECK((*(it + 1)).get() == 42);
  CHECK(it[1].get() == 42);

  CHECK(it[0].get() == 0);
  CHECK(it->set(43) == 0);
  CHECK(it->get() == 43);
  CHECK(it->set(41) == 43);
  CHECK(it->get() == 41);
  CHECK(it[0].get() == 41);
  CHECK(it[1].get() == 42);

  CHECK(f.values[0].get() == 41);
  CHECK(f.values[1].get() == 42);
  CHECK(f.values[2].get() == 0);
}

namespace {

/**
 * This structure wraps an std::vector and provides a simple iterator over it.
 * The iterator that is wrapped is always the plain iterator, not a
 * const_iterator; we define a const_iterator also using iterator_facade.  Note
 * that if the simple_mutable_struct is const, the internally stored vector will
 * also be const, and so the begin() used below will return a const_iterator to
 * the vector. We can either conditionally set the wrapped iterator type to be
 * const or non-const, or we can mark the internal vector as mutable. We could
 * also make the simple_mutable_iterator prameterized on iterator type. Below we
 * use the second option.
 */
template <class SValue>
struct simple_mutable_struct {
  static_assert(!std::is_const_v<SValue>);

  // One option: Wrap just iterator -- need to make this mutable so that begin()
  // returns just an iterator even if the simple_mutable_struct is const
  /* mutable */ std::vector<SValue> value;

  simple_mutable_struct() {
    value.resize(10);
  }

  template <class V>
  class simple_mutable_iterator
      : public iterator_facade<simple_mutable_iterator<V>> {
    // Second option, wrap the iterator using the appropriate const or non-const
    using Iter = std::conditional_t<
        std::is_const_v<V>,
        typename std::vector<std::remove_cvref_t<V>>::const_iterator,
        typename std::vector<std::remove_cvref_t<V>>::iterator>;
    Iter current_;

   public:
    simple_mutable_iterator() = default;
    explicit simple_mutable_iterator(Iter i)
        : current_(i) {
    }

    template <class U>
      requires(!std::is_const_v<U>)
    explicit simple_mutable_iterator(simple_mutable_iterator<U> i)
        : current_(i.current_) {
    }

    V& dereference() const noexcept {
      return *current_;
    }

    void advance(int off) noexcept {
      current_ += off;
    }

    auto distance_to(simple_mutable_iterator o) const noexcept {
      return o.current_ - current_;
    }

    bool operator==(simple_mutable_iterator o) const noexcept {
      return o.current_ == current_;
    }
  };

  using iterator = simple_mutable_iterator<SValue>;
  using const_iterator = simple_mutable_iterator<const SValue>;

  using value_type = typename std::iterator_traits<iterator>::value_type;
  using reference = typename std::iterator_traits<iterator>::reference;
  using const_reference =
      typename std::iterator_traits<const_iterator>::reference;

  auto begin() {
    return iterator{value.begin()};
  }
  auto end() {
    return iterator{value.end()};
  }
  auto begin() const {
    return const_iterator{value.begin()};
  }
  auto end() const {
    return const_iterator{value.end()};
  }
  auto cbegin() const {
    return const_iterator{value.begin()};
  }
  auto cend() const {
    return const_iterator{value.end()};
  }
  auto size() const {
    return value.size();
  }
};

template <class Value>
class pointer_wrapper : public iterator_facade<pointer_wrapper<Value>> {
  Value* current_ = nullptr;

 public:
  pointer_wrapper() = default;
  explicit pointer_wrapper(Value* i)
      : current_(i) {
  }

  Value& dereference() const noexcept {
    return *current_;
  }

  constexpr void advance(int off) noexcept {
    current_ += off;
  }

  constexpr auto distance_to(pointer_wrapper o) const noexcept {
    return o.current_ - current_;
  }

  // This is supposed to be inferred....  Either from equal_to or from
  // distance_to -- but it seems to be necessary to specify it here.
  constexpr bool operator==(pointer_wrapper o) const noexcept {
    return o.current_ == current_;
  }
};
}  // namespace

template <class Iterator>
void iterator_test(Iterator begin, [[maybe_unused]] Iterator end) {
  Iterator it = begin;
  ++it;
  CHECK(*it == 14);
  it++;
  CHECK(*it == 15);
  it += 3;
  CHECK(*it == 18);
  CHECK(*it++ == 18);
  CHECK(*it == 19);
  CHECK(*it-- == 19);
  CHECK(*it == 18);
  CHECK(*++it == 19);
  CHECK(*--it == 18);
  it -= 2;
  CHECK(*it == 16);
  it--;
  CHECK(*it == 15);
  --it;
  CHECK(*it == 14);

  CHECK(it[0] == 14);

  it = begin;
  for (int i = 0; i < 10; ++i) {
    CHECK(it[i] == 13 + i);
  }

  it = begin;
  CHECK(it == it);
  CHECK(it <= it);
  CHECK(it >= it);

  auto it2 = it;
  CHECK(it == it2);
  CHECK(it2 == it);
  CHECK(it2 <= it);
  CHECK(it2 >= it);
  ++it;
  CHECK(it != it2);
  CHECK(it2 != it);
  CHECK(it2 < it);
  CHECK(it > it2);
  CHECK(!(it <= it2));
  CHECK(!(it2 >= it));
}

template <class S>
void struct_test(S& s) {
  auto it = s.begin();

  std::iota(s.begin(), s.end(), 13);
  CHECK(std::equal(s.begin(), s.end(), s.value.begin()));
  CHECK(std::equal(s.cbegin(), s.cend(), s.value.cbegin()));
  CHECK(std::equal(s.begin(), s.end(), s.cbegin()));
  CHECK(std::equal(s.cbegin(), s.cend(), s.begin()));
  CHECK(std::equal(
      s.begin(),
      s.end(),
      std::vector<int>{13, 14, 15, 16, 17, 18, 19, 20, 21, 22}.begin()));

  iterator_test(s.begin(), s.end());
  iterator_test(s.cbegin(), s.cend());

  int counter = 0;
  for (auto i : s) {
    CHECK(i == 13 + counter);
    ++counter;
  }
  it = s.begin();
  *it = 17;
  CHECK(*it == 17);

  std::iota(s.begin(), s.end(), 13);
  [[maybe_unused]] auto cit = s.cbegin();
  counter = 0;
  for (auto c = s.cbegin(); c != s.cend(); ++c) {
    CHECK(*c == 13 + counter);
    ++counter;
  }

  // Error: trying to assign to const_iterator
  // *cit = 0;
};

template <class S>
void const_struct_test(const S& s) {
  [[maybe_unused]] auto it = s.begin();

  CHECK(std::equal(s.begin(), s.end(), s.value.begin()));
  CHECK(std::equal(s.cbegin(), s.cend(), s.value.cbegin()));
  CHECK(std::equal(s.begin(), s.end(), s.cbegin()));
  CHECK(std::equal(s.cbegin(), s.cend(), s.begin()));
  CHECK(std::equal(
      s.begin(),
      s.end(),
      std::vector<int>{13, 14, 15, 16, 17, 18, 19, 20, 21, 22}.begin()));

  iterator_test(s.begin(), s.end());
  iterator_test(s.cbegin(), s.cend());
};

TEST_CASE("simple_mutable_struct", "[iterator_facade]") {
  simple_mutable_struct<int> s;

  struct_test(s);
  const_struct_test(s);

  std::iota(s.begin(), s.end(), 13);

  iterator_test(s.begin(), s.end());
  iterator_test(s.cbegin(), s.cend());
}

template <class R, bool is_output = true>
void range_test() {
  CHECK(std::ranges::range<R>);
  CHECK(std::ranges::input_range<R>);

  if (is_output) {
    CHECK(std::ranges::output_range<R, typename R::value_type>);
  } else {
    CHECK(!std::ranges::output_range<R, typename R::value_type>);
  }

  CHECK(std::ranges::forward_range<R>);
  CHECK(std::ranges::bidirectional_range<R>);
  CHECK(std::ranges::random_access_range<R>);

  if (std::same_as<
          std::remove_cvref_t<R>,
          std::vector<typename R::value_type>>) {
    CHECK(std::ranges::contiguous_range<R>);
  } else {
    CHECK(!std::ranges::contiguous_range<R>);
  }

  CHECK(std::ranges::sized_range<R>);
}

TEST_CASE("simple_mutable_struct range concepts", "[iterator_facade]") {
  range_test<std::vector<int>>();
  range_test<simple_mutable_struct<int>>();
  range_test<simple_mutable_struct<int> const, false>();

  range_test<const std::vector<int>, false>();
  range_test<const simple_mutable_struct<int>, false>();
  range_test<const simple_mutable_struct<int>, false>();
}

template <class I, bool is_output = true>
void iterator_test() {
  CHECK(std::input_iterator<I>);

  if (is_output) {
    CHECK(
        std::output_iterator<I, typename std::iterator_traits<I>::value_type>);
  } else {
    CHECK(
        !std::output_iterator<I, typename std::iterator_traits<I>::value_type>);
  }

  CHECK(std::forward_iterator<I>);
  CHECK(std::bidirectional_iterator<I>);
  CHECK(std::random_access_iterator<I>);
  if constexpr (
      std::is_same_v<
          std::remove_cvref_t<I>,
          typename std::vector<
              typename std::iterator_traits<I>::value_type>::iterator> ||
      std::is_pointer_v<I>) {
    CHECK(std::contiguous_iterator<I>);
  } else {
    CHECK(!std::contiguous_iterator<I>);
  }
}

TEST_CASE("simple_mutable_struct iterator concepts", "[iterator_facade]") {
  iterator_test<std::vector<int>::iterator>();
  iterator_test<simple_mutable_struct<int>::iterator>();
  iterator_test<simple_mutable_struct<int>::const_iterator, false>();

  iterator_test<pointer_wrapper<int>>();
  iterator_test<pointer_wrapper<const int>, false>();
  iterator_test<pointer_wrapper<int const>, false>();

  iterator_test<int*>();
  iterator_test<const int*, false>();
  iterator_test<int const*, false>();

  // These will all fail (evidently) since the container is const.
  // The range tests pass, however, even though the range concepts are
  // defined in terms of the iterator concepts....
  // @todo Fix so that iterator and range checks are consistent.
#if 0
  iterator_test<const std::vector<int>::iterator, false>();
  iterator_test<const simple_mutable_struct<int>::iterator, false>();
  iterator_test<const simple_mutable_struct<int>::const_iterator, false>();
  iterator_test<const pointer_wrapper<int>>();
  iterator_test<const pointer_wrapper<const int>, false>();
  iterator_test<const pointer_wrapper<int const>, false>();
#endif
}

template <
    class I,
    class Value,
    class Category = std::random_access_iterator_tag,
    class Reference = Value&,
    class ConstReference = const Reference,
    class RValueReference = Value&&,
    class Difference = std::ptrdiff_t>
void iterator_types_test() {
  CHECK(std::is_same_v<std::iter_value_t<I>, std::remove_cvref_t<Value>>);
  CHECK(std::is_same_v<std::iter_reference_t<I>, Reference>);
  // This is a C++23 concept -- enable once we move to C++23
  // CHECK(std::is_same_v<std::iter_const_reference_t<I>, Reference>);
  CHECK(std::is_same_v<std::iter_difference_t<I>, Difference>);
  CHECK(std::is_same_v<std::iter_rvalue_reference_t<I>, RValueReference>);
  CHECK(std::is_same_v<
        typename std::iterator_traits<I>::iterator_category,
        Category>);
}

TEST_CASE("simple_mutable_struct iterator types", "[iterator_facade]") {
  iterator_types_test<std::vector<int>::iterator, int>();
  iterator_types_test<std::vector<int>::const_iterator, const int>();
  iterator_types_test<simple_mutable_struct<int>::iterator, int>();
  iterator_types_test<simple_mutable_struct<int>::const_iterator, const int>();
}

TEST_CASE("pointer wrapper iterator types", "[iterator_facade]") {
  iterator_types_test<pointer_wrapper<int>, int>();
  iterator_types_test<int*, int>();

  iterator_types_test<pointer_wrapper<const int>, const int>();
  iterator_types_test<const int*, const int>();
  iterator_types_test<pointer_wrapper<int const>, int const>();
  iterator_types_test<int const*, int const>();

  iterator_types_test<const std::vector<int>::iterator, int>();
  iterator_types_test<const std::vector<int>::const_iterator, const int>();
  // @todo: Why are these only forward iterators?
  iterator_types_test<
      const simple_mutable_struct<int>::iterator,
      int,
      std::forward_iterator_tag>();
  iterator_types_test<
      const simple_mutable_struct<int>::const_iterator,
      const int,
      std::forward_iterator_tag>();

  // @todo: Why are thest only input iterators?
  iterator_types_test<
      const pointer_wrapper<int>,
      int,
      std::forward_iterator_tag>();
  iterator_types_test<
      const pointer_wrapper<const int>,
      const int,
      std::forward_iterator_tag>();

  iterator_types_test<
      const pointer_wrapper<int const>,
      int const,
      std::forward_iterator_tag>();
}
