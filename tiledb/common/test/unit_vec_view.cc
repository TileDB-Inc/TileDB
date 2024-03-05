/**
 * @file unit_vec_view.cc
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
 * This file tests`is_experimental_build=true` when
 * `TILEDB_EXPERIMENTAL_FEATURES=ON`
 */
#include <test/support/tdb_catch.h>
#include "tiledb/common/pmr.h"
#include "tiledb/common/vec_view.h"

using tiledb::stdx::vec_view;

template <typename T>
struct value_generator;

template <>
struct value_generator<int32_t> {
  static int32_t make_value(int iter) {
    return (iter * iter) % 131071;
  }
};

template <>
struct value_generator<int64_t> {
  static int32_t make_value(int iter) {
    return (iter * iter) % 131071;
  }
};

template <>
struct value_generator<std::string> {
  static std::string make_value(int iter) {
    const std::string chars(reinterpret_cast<const char*>(&iter), sizeof(int));
    std::string result;
    for (int i = 0; i < (iter * iter) % 223; i++) {
      result += chars;
    }
    return result;
  }
};

template <typename T>
struct std_vector_provider {
  using vector_type = std::vector<T>;

  static std::vector<T> empty_vector() {
    return std::vector<T>();
  }
};

template <typename T>
struct pmr_vector_provider {
  using vector_type = std::vector<T, std::pmr::polymorphic_allocator<T>>;

  static vector_type empty_vector() {
    return vector_type(tiledb::common::pmr::get_default_resource());
  }
};

template <typename T, typename VectorProvider>
void do_vec_view_tests() {
  SECTION("empty") {
    auto values = VectorProvider::empty_vector();
    vec_view<T> view(values);

    CHECK(view.empty());
    CHECK(view.size() == 0);
    CHECK(view.begin() == view.end());

    typename VectorProvider::vector_type copy = view;
    CHECK(copy == values);
  }

  SECTION("one element") {
    const T value = value_generator<T>::make_value(0);
    auto values = VectorProvider::empty_vector();
    values.push_back(value);

    vec_view<T> view(values);

    CHECK(!view.empty());
    CHECK(view.size() == 1);
    CHECK(view.begin() != view.end());
    CHECK(view.begin() + 1 == view.end());

    CHECK(view.at(0) == value);
    CHECK(view[0] == value);
    CHECK(view.front() == value);
    CHECK(view.back() == value);
    CHECK(*view.begin() == value);
    CHECK(*(view.end() - 1) == value);

    int niters = 0;
    for (const auto& v : view) {
      CHECK(v == value);
      ++niters;
    }
    CHECK(niters == 1);

    typename VectorProvider::vector_type copy = view;
    CHECK(copy == values);
  }

  SECTION("two elements") {
    const T v1 = value_generator<T>::make_value(1),
            v2 = value_generator<T>::make_value(2);
    auto values = VectorProvider::empty_vector();
    values.push_back(v1);
    values.push_back(v2);

    vec_view<T> view(values);

    CHECK(!view.empty());
    CHECK(view.size() == 2);
    CHECK(view.begin() != view.end());
    CHECK(view.begin() + 2 == view.end());

    CHECK(view.at(0) == v1);
    CHECK(view.at(1) == v2);
    CHECK(view[0] == v1);
    CHECK(view[1] == v2);
    CHECK(view.front() == v1);
    CHECK(view.back() == v2);
    CHECK(*view.begin() == v1);
    CHECK(*(view.end() - 1) == v2);

    int niters = 0;
    for (const auto& v : view) {
      CHECK(v == values.at(niters));
      ++niters;
    }
    CHECK(niters == 2);

    typename VectorProvider::vector_type copy = view;
    CHECK(copy == values);
  }

  SECTION("more elements") {
    const int nelements = 1000;
    auto values = VectorProvider::empty_vector();
    for (int i = 0; i < nelements; i++) {
      values.push_back(value_generator<T>::make_value(i));
    }

    vec_view<T> view(values);

    CHECK(!view.empty());
    CHECK(view.size() == nelements);
    CHECK(view.begin() != view.end());
    CHECK(view.begin() + nelements == view.end());

    for (int i = 0; i < nelements; i++) {
      CHECK(view.at(i) == values.at(i));
      CHECK(view[i] == values[i]);
      CHECK(*(view.begin() + i) == *(values.begin() + i));
      CHECK(*(view.end() - (i + 1)) == *(values.end() - (i + 1)));
    }

    typename vec_view<T>::size_type niters = 0;
    for (const auto& v : view) {
      CHECK(v == values.at(niters));
      ++niters;
    }
    CHECK(niters == values.size());

    typename VectorProvider::vector_type copy = view;
    CHECK(copy == values);
  }
}

TEST_CASE("vec_view, std, int32_t") {
  using T = int32_t;
  do_vec_view_tests<T, std_vector_provider<T>>();
}

TEST_CASE("vec_view, pmr, int32_t") {
  using T = int32_t;
  do_vec_view_tests<T, pmr_vector_provider<T>>();
}

TEST_CASE("vec_view, std, int64_t") {
  using T = int64_t;
  do_vec_view_tests<T, std_vector_provider<T>>();
}

TEST_CASE("vec_view, pmr, int64_t") {
  using T = int64_t;
  do_vec_view_tests<T, pmr_vector_provider<T>>();
}

TEST_CASE("vec_view, std, std::string") {
  using T = std::string;
  do_vec_view_tests<T, std_vector_provider<T>>();
}

TEST_CASE("vec_view, pmr, std::string") {
  using T = std::string;
  do_vec_view_tests<T, pmr_vector_provider<T>>();
}

struct TestUserSession {
  int64_t userId;
  int64_t sessionId;
  int64_t currentTransactionId;
  std::string lastQueryLabel;
  std::map<std::string, std::string> sessionOverrides;

  bool operator==(const TestUserSession&) const = default;
};

template <>
struct value_generator<TestUserSession> {
  static TestUserSession make_value(int iter) {
    const int64_t userId = (iter * iter) % 71;
    const int64_t sessionId = (iter * iter) % 7919;
    const int64_t currentTransactionId = (sessionId + 1);
    const std::string lastQueryLabel =
        "query" + std::to_string(currentTransactionId);

    std::map<std::string, std::string> sessionOverrides;
    for (int i = 0; i < iter % 5; i++) {
      sessionOverrides["parameter" + std::to_string((iter + i) % 37)] =
          "value" + std::to_string((iter + i) % 51);
    }
    return TestUserSession{
        userId,
        sessionId,
        currentTransactionId,
        lastQueryLabel,
        sessionOverrides};
  }
};

TEST_CASE("vec_view, std, multi-member struct") {
  using T = TestUserSession;
  do_vec_view_tests<T, std_vector_provider<T>>();
}

TEST_CASE("vec_view, pmr, multi-member struct") {
  using T = TestUserSession;
  do_vec_view_tests<T, pmr_vector_provider<T>>();
}
