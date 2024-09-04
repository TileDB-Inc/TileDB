/*
 * @file tiledb/common/registry/test/unit_registry.cpp
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
 * This file provides unit tests for `template <class T> class Registry`.
 */

#include <catch2/catch_test_macros.hpp>

#include "../registry.h"

struct Item;
using item_registry_type = tiledb::common::Registry<Item>;
using item_handle_type = item_registry_type::registry_handle_type;

struct Item {
  item_handle_type handle_;

  explicit Item(item_registry_type& registry)
      : handle_(registry.register_item(*this)) {
  }

  void register_shared_ptr(std::shared_ptr<Item> p) {
    handle_.register_shared_ptr(p);
  }
};

/**
 * Check that (1) the size of the registry argument matches the size argument
 * and (2) that the size found through iteration also matches.
 */
void check_size(const item_registry_type& r, item_registry_type::size_type n) {
  CHECK(r.size() == n);
  item_registry_type::size_type m{0};
  auto f{[&m](const item_registry_type::value_type&) -> void { ++m; }};
  r.for_each(f);
  CHECK(r.size() == m);
}

TEST_CASE("Registry - construct") {
  item_registry_type r;
}

TEST_CASE("Registry - construct and add, single") {
  item_registry_type r;
  CHECK(r.size() == 0);
  {
    Item i{r};
    CHECK(r.size() == 1);
  }
  CHECK(r.size() == 0);
}

TEST_CASE("Registry - construct and add, two nested") {
  item_registry_type r;
  CHECK(r.size() == 0);
  {
    Item i{r};
    CHECK(r.size() == 1);
    {
      Item i2{r};
      CHECK(r.size() == 2);
    }
    CHECK(r.size() == 1);
  }
  CHECK(r.size() == 0);
}

/*
 * This test also exercises `register_shared_ptr` and `for_each`.
 */
TEST_CASE("Registry - construct and add, two interleaved") {
  item_registry_type r;
  check_size(r, 0);
  {
    auto i1{std::make_shared<Item>(r)};
    i1->register_shared_ptr(i1);
    check_size(r, 1);
    {
      auto i2{std::make_shared<Item>(r)};
      i2->register_shared_ptr(i2);
      check_size(r, 2);
      i1.reset();
      check_size(r, 1);
    }
    check_size(r, 0);
  }
  check_size(r, 0);
}
