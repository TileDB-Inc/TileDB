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

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

TEST_CASE("DynamicArrayStorage::DynamicArrayStorage") {
  DynamicArrayStorage<int> x{HERE(), 3};
}

namespace tiledb::sm {
class WhiteboxDomainTypedDataView : public DomainTypedDataView {
 public:
  WhiteboxDomainTypedDataView(
      const std::string_view& origin, const Domain& domain, size_t n)
      : DomainTypedDataView(origin, domain, n) {
  }
};
}  // namespace tiledb::sm

TEST_CASE("DomainTypedDataView::DomainTypedDataView") {
  Domain d;
  // Domain ignored when dimension given
  WhiteboxDomainTypedDataView x{HERE(), d, 3};
}
