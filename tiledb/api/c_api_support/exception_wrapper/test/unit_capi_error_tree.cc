/**
 * @file c_api_support/exception_wrapper/test/unit_capi_error_tree.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include <catch2/catch_test_macros.hpp>

#include <variant>

#include "../exception_wrapper.h"
#include "tiledb/common/tag.h"
#include "tiledb/common/unreachable.h"

using namespace tiledb::api;
using namespace tiledb::api::detail;

//----------------------------------
// Error
//----------------------------------
/**
 * An error instance, irrespective of ownership
 *
 * At base an error is two strings in a pair:
 * 1. An origin
 * 2. An error message
 *
 * The motivation for using more than `std::string` is to allow constant error
 * object in support of allocation-free messages for `bad_alloc`. Another aspect
 * of this goal is that this class is only data. It does not convert to string,
 * not only to allow the details of formatting to be decided by a visitor, but
 * also to avoid baking `std::string` operations, which would entail using
 * the allocator, into this class.
 *
 * @section Maturity Notes
 *
 * This class has incubation status. It's currently only used in testing and
 * validating the error tree model. This class can inform how to evolve `class
 * StatusException`, which needs to unhook itself from `class Status` more.
 */
class Error {
  using string_pair = std::pair<std::string, std::string>;
  using string_view_pair = std::pair<std::string_view, std::string_view>;
  /**
   * A primitive error message (i.e. an item in an ErrorTree) is one of three
   * things:
   * index 0: Not actually an error (std::monostate)
   * index 1: An ordinary error where this object holds the data (string pair)
   * index 2: A view to an ordinary error in another object (string_view pair)
   */
  using storage_type =
      std::variant<std::monostate, string_pair, string_view_pair>;

  storage_type x_;

 public:
  [[nodiscard]] bool has_error() const {
    return x_.index() != 0;
  }

  /**
   * Retrieve the whole error.
   */
  [[nodiscard]] string_view_pair error() const noexcept {
    switch (x_.index()) {
      case 0:
        /*
         * Rather than throw when there's no actual stored error, we return a
         * default object. This function is called as part of exception handling
         * so we don't want it throwing.
         */
        return {};
      case 1: {
        /*
         * The return expression calls the conversion constructor from
         * `std::string` twice.
         */
        const auto& y{get<1>(x_)};
        return {y.first, y.second};
      }
      case 2:
        return get<2>(x_);
      default:
        stdx::unreachable();
    }
  }

  /**
   * Default constructor makes a "no error" instance.
   */
  Error()
      : x_{std::monostate{}} {
  }

  /**
   * Constructor from a pair of strings
   */
  Error(const std::string& origin, const std::string& message)
      : x_{string_pair{origin, message}} {
  }

  /**
   * Constructor from a pair of string views.
   *
   * The string_view constructor is tagged to prevent the compiler from
   * inferring a string_view from what might be a transient object. Use this
   * constructor only when life span assumptions are clearly documented.
   */
  Error(
      Tag<std::string_view>, std::string_view origin, std::string_view message)
      : x_{string_view_pair{origin, message}} {
  }
};

TEST_CASE("Error - default constructor", "[error]") {
  Error x{};
  auto y{x.error()};
  /*
   * Default constructor makes an empty object, but constructs a pair of empty
   * string views for `error()`.
   */
  CHECK(y.first.empty());
  CHECK(y.second.empty());
}

TEST_CASE("Error - string constructor 0", "[error]") {
  std::string origin{"origin"};
  std::string message{"message"};
  Error x{origin, message};
  auto y{x.error()};
  CHECK(y.first == "origin");
  CHECK(y.second == "message");
}

TEST_CASE("Error - string constructor 1", "[error]") {
  std::string ori{"ori"};
  std::string gin{"gin"};
  std::string message{"message"};
  Error x{ori + gin, message};
  auto y{x.error()};
  CHECK(y.first == "origin");
  CHECK(y.second == "message");
}

TEST_CASE("Error - string_view constructor", "[error]") {
  std::string origin{"origin"};
  std::string message{"message"};
  Error x{
      Tag<std::string_view>(),
      std::string_view(origin),
      std::string_view(message)};
  auto y{x.error()};
  CHECK(y.first == "origin");
  CHECK(y.second == "message");
}

//----------------------------------
// Skeleton Visitor
//----------------------------------
/**
 * This visitor emits a skeleton of the parse structure, ignoring the content
 * of the nodes.
 *
 * @tparam T The node type, which is ignored.
 */
template <class T>
class SkeletonVisitor {
  std::string s_{};

 public:
  SkeletonVisitor() = default;
  void start_level() noexcept {
    s_ += "(";
  }
  void end_level() noexcept {
    s_ += ")";
  }
  void separator() noexcept {
    s_ += ",";
  }
  using item_type = T;
  void item(const T&) noexcept {
    /*
     * It's a skeleton because it ignores each actual item and just marks the
     * presence of one.
     */
    s_ += "x";
  }

  [[nodiscard]] std::string value() const {
    return s_;
  }
};
static_assert(ErrorTreeVisitor<SkeletonVisitor<std::exception>>);

//----------------------------------
// ErrorTreeTest
//----------------------------------
/**
 * An element within the tree of `ErrorTreeTest` along with all its children.
 *
 * @tparam E The type contained within each node.
 */
template <class E>
class ETTElement {
 public:
  using self_type = E;
  using vector_type = std::vector<ETTElement>;

 private:
  E e_;
  vector_type x_;

 public:
  ETTElement() = delete;
  ETTElement(const E& e, const vector_type& x)
      : e_{e}
      , x_{x} {
  }
  const self_type& self() const {
    return e_;
  }
  const vector_type& children() const {
    return x_;
  }
};

/**
 * An error graph for testing visitors.
 *
 * Each node in a tree is a pair (an Error and a vector of children) manifested
 * as an instance of `class ETTElement`. The graph as a whole is multi-rooted
 * for generality, although it's unlikely it will be used that way in practice.
 */
class ErrorTreeTest {
  using vector_type = ETTElement<Error>::vector_type;
  vector_type x_;

  template <ErrorTreeVisitor V>
  void visit_recursive(V& v, const vector_type nodes) const {
    bool separating{false};
    for (const auto& node : nodes) {
      if (separating) {
        v.separator();
      } else {
        separating = true;
      }
      v.item(node.self());
      auto children{node.children()};
      if (!children.empty()) {
        v.start_level();
        visit_recursive(v, node.children());
        v.end_level();
      }
    }
  }

 public:
  ErrorTreeTest()
      : x_{vector_type{}} {
  }
  ErrorTreeTest(Error e)
      : x_{ETTElement<Error>{e, {}}} {
  }
  ErrorTreeTest(vector_type x)
      : x_(x) {
  }

  template <ErrorTreeVisitor V>
  void visit(V& v) const {
    visit_recursive(v, x_);
  }
};

TEST_CASE("ErrorTreeTest - empty", "[error_tree_test]") {
  ErrorTreeTest x{};
  SkeletonVisitor<Error> v{};
  x.visit(v);
  CHECK(v.value() == "");
}

TEST_CASE("ErrorTreeTest - single node", "[error_tree_test]") {
  ErrorTreeTest x{Error{"a", "b"}};
  SkeletonVisitor<Error> v{};
  x.visit(v);
  CHECK(v.value() == "x");
}

TEST_CASE("ErrorTreeTest - two wide", "[error_tree_test]") {
  ErrorTreeTest x{{{Error{"a", "b"}, {}}, {Error{"c", "d"}, {}}}};
  SkeletonVisitor<Error> v{};
  x.visit(v);
  CHECK(v.value() == "x,x");
}

TEST_CASE("ErrorTreeTest - two deep", "[error_tree_test]") {
  ErrorTreeTest x{{{Error{"a", "b"}, {{Error{"c", "d"}, {}}}}}};
  SkeletonVisitor<Error> v{};
  x.visit(v);
  CHECK(v.value() == "x(x)");
}

//----------------------------------
// ErrorTreeStdException
//----------------------------------
TEST_CASE("ErrorTreeStdException - direct throw std::exception") {
  SkeletonVisitor<std::exception> v{};
  try {
    throw std::exception();
  } catch (const std::exception& e) {
    ErrorTreeStdException x{e};
    x.visit(v);
  }
  CHECK(v.value() == "x");
}

/**
 * Throws nested exceptions of different levels
 *
 * - level negative: does nothing
 * - level zero: throw a non-nested exception
 * - level N: throws an exception nested N times, all around a level-0 exception
 *
 * @param level The number of nesting levels
 */
void throw_nested_by_level(int level) {
  /*
   * Throw nothing if the level is negative
   */
  if (level < 0) {
    return;
  } else if (level == 0) {
    throw std::runtime_error("level 0");
  }
  // Assert: level > 0
  try {
    throw_nested_by_level(level - 1);
  } catch (...) {
    std::throw_with_nested(
        std::runtime_error("level " + std::to_string(level)));
  }
};

std::exception_ptr nested_by_level(int level) {
  if (level < 0) {
    throw std::invalid_argument("level may not be negative");
  }
  try {
    throw_nested_by_level(level);
    throw std::logic_error("throw_nested_by_level did not throw");
  } catch (...) {
    return std::current_exception();
  }
};

std::exception_ptr se0() {
  try {
    throw StatusException("here", "now");
  } catch (...) {
    return std::current_exception();
  }
}

std::exception_ptr se1() {
  try {
    try {
      throw StatusException("here", "now");
    } catch (...) {
      std::throw_with_nested(std::runtime_error("later"));
    }
  } catch (...) {
    return std::current_exception();
  }
}

std::exception_ptr se2() {
  try {
    try {
      throw std::runtime_error("now");
    } catch (...) {
      std::throw_with_nested(StatusException("here", "later"));
    }
  } catch (...) {
    return std::current_exception();
  }
}

struct ETSETestVector {
  std::string name_;
  std::exception_ptr exception_;
  std::string skeleton_;
  std::string log_;
};

TEST_CASE("ErrorTreeStdException - skeleton & log") {
  using TV = ETSETestVector;
  std::array<TV, 6> vectors{
      TV{"level 0", nested_by_level(0), "x", "TileDB internal: level 0"},
      TV{"level 1",
         nested_by_level(1),
         "x(x)",
         "TileDB internal: level 1 (TileDB internal: level 0)"},
      TV{"level 2",
         nested_by_level(2),
         "x(x(x))",
         "TileDB internal: level 2"
         " (TileDB internal: level 1"
         " (TileDB internal: level 0))"},
      TV{"SE 0", se0(), "x", "here: now"},
      TV{"SE 1", se1(), "x(x)", "TileDB internal: later (here: now)"},
      TV{"SE 2", se2(), "x(x)", "here: later (TileDB internal: now)"},
  };

  for (const auto& vector : vectors) {
    DYNAMIC_SECTION(vector.name_) {
      SkeletonVisitor<std::exception> v{};
      ETVisitorStdException v2{};
      try {
        std::rethrow_exception(vector.exception_);
      } catch (const std::exception& e) {
        ErrorTreeStdException x{e};
        x.visit(v);
        x.visit(v2);
      } catch (...) {
        throw std::logic_error("unexpected exception");
      }
      SECTION("skeleton") {
        CHECK(v.value() == vector.skeleton_);
      }
      SECTION("log message") {
        CHECK(v2.value() == vector.log_);
      }
    }
  }
}
