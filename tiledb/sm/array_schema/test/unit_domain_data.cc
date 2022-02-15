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

#include <catch.hpp>
#include "../domain_typed_data_view.h"
#include "../dynamic_array.h"

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

TEST_CASE("DynamicArrayStorage::DynamicArrayStorage") {
  DynamicArray<int> x{3, std::allocator<int>{}, Tag<NullInitializer>{}};
}

namespace tiledb::sm {
class WhiteboxDomainTypedDataView : public DomainTypedDataView {
 public:
  template <class I, class... Args>
  WhiteboxDomainTypedDataView(const Domain& domain, Tag<I>, Args&&... args)
      : DomainTypedDataView(domain, Tag<I>{}, std::forward<Args>(args)...) {
  }
};

//-------------
/*
 * Substitute definitions. Some definition of Domain needs to be present to
 * test DomainTypedDataView, or else these tests won't link.
 */
Status Domain::add_dimension(const Dimension*) {
  ++dim_num_;
  return Status::Ok();
}
Domain::Domain()
    : dim_num_(0) {
}
unsigned int Domain::dim_num() const {
  return dim_num_;
}
class Dimension {};
//-------------

TEST_CASE("DomainTypedDataView::DomainTypedDataView, null initializer") {
  struct Initializer {
    static inline void initialize(
        UntypedDatumView*, unsigned int, const Domain&) {
    }
  };

  Domain d{};
  d.add_dimension(nullptr);
  d.add_dimension(nullptr);
  d.add_dimension(nullptr);
  WhiteboxDomainTypedDataView x{d, Tag<Initializer>{}};
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

  Domain d{};
  d.add_dimension(nullptr);
  d.add_dimension(nullptr);
  d.add_dimension(nullptr);
  WhiteboxDomainTypedDataView x{d, Tag<Initializer>{}};
  CHECK(x.size() == 3);
  CHECK(x[0].size() == 0);
  CHECK(x[1].size() == 1);
  CHECK(x[2].size() == 2);
}

}  // namespace tiledb::sm
