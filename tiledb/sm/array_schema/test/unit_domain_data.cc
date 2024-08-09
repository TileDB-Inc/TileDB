/**
 * @file tiledb/sm/array_schema/test/unit_domain_data.cc
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
 * This file defines a test `main()`
 */

#include <catch2/catch_test_macros.hpp>
#include "../domain_typed_data_view.h"
#include "../dynamic_array.h"
#include "src/mem_helpers.h"

/*
 * Instantiating the class `Domain` requires a full definition of `Dimension` so
 * that its destructor is visible. The need to include this header indicates
 * some kind of trouble with definition order that needs to be fixed.
 */
#include "tiledb/sm/array_schema/dimension.h"

using namespace tiledb::common;
using namespace tiledb::sm;

/**
 * Null initializer takes any argument, so it's suitable for both
 * DynamicArrayStorage and for DomainTypedDataView.
 */
struct NullInitializer {
  template <class... Args>
  inline static void initialize(Args&&...) {
  }
};

TEST_CASE("DynamicArray::DynamicArray, no initializer") {
  DynamicArray<int> x{3, tdb::allocator<int>{}};
}

TEST_CASE("DynamicArray::DynamicArray, null initializer") {
  DynamicArray<int> x{3, tdb::allocator<int>{}, Tag<NullInitializer>{}};
}

struct X1 {
  static int counter;
  X1() {
    ++counter;
  }
};
int X1::counter{0};
TEST_CASE("DynamicArray::DynamicArray, default initializer") {
  X1::counter = 0;
  DynamicArray<X1> x{3, tdb::allocator<X1>{}, Tag<void>{}};
  CHECK(X1::counter == 3);
}

TEST_CASE("DynamicArray::DynamicArray, simple initializer") {
  struct X {
    int x_;
    X() = delete;
    X(int x)
        : x_(x) {
    }
  };
  struct Initializer {
    static inline void initialize(X* item, int i) {
      new (item) X{i};
    }
  };
  DynamicArray<X> x{3, tdb::allocator<X>{}, Tag<Initializer>{}};
  CHECK(x[0].x_ == 0);
  CHECK(x[1].x_ == 1);
  CHECK(x[2].x_ == 2);
}

namespace tiledb::sm {
class WhiteboxDomainTypedDataView : public DomainTypedDataView {
 public:
  template <class I, class... Args>
  WhiteboxDomainTypedDataView(const Domain& domain, Tag<I>, Args&&... args)
      : DomainTypedDataView(domain, Tag<I>{}, std::forward<Args>(args)...) {
  }
};

struct TestNullInitializer {
  inline static void initialize(
      UntypedDatumView*, unsigned int, const Domain&) {
  }
};

TEST_CASE("DomainTypedDataView::DomainTypedDataView, null initializer") {
  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  Domain d{memory_tracker};
  //  tiledb::sm::Dimension dim{"", tiledb::sm::Datatype::INT32};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(),
      "",
      tiledb::sm::Datatype::INT32,
      tiledb::test::get_test_memory_tracker())};
  CHECK(d.add_dimension(dim).ok());
  CHECK(d.add_dimension(dim).ok());
  CHECK(d.add_dimension(dim).ok());
  WhiteboxDomainTypedDataView x{d, Tag<TestNullInitializer>{}};
  CHECK(x.size() == 3);
}

TEST_CASE("DomainTypedDataView::DomainTypedDataView, simple initializer") {
  /*
   * To verify that the initializer runs, we use only the size element.
   */
  struct Initializer {
    static inline void initialize(
        UntypedDatumView* item, unsigned int i, const Domain&) {
      new (item) UntypedDatumView{nullptr, i};
    }
  };

  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  Domain d{memory_tracker};
  auto dim{make_shared<tiledb::sm::Dimension>(
      HERE(),
      "",
      tiledb::sm::Datatype::INT32,
      tiledb::test::get_test_memory_tracker())};
  CHECK(d.add_dimension(dim).ok());
  CHECK(d.add_dimension(dim).ok());
  CHECK(d.add_dimension(dim).ok());
  WhiteboxDomainTypedDataView x{d, Tag<Initializer>{}};
  CHECK(x.size() == 3);
  CHECK(x[0].size() == 0);
  CHECK(x[1].size() == 1);
  CHECK(x[2].size() == 2);
}

}  // namespace tiledb::sm
