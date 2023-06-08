/**
 * @file tiledb/sm/filter/test/unit_typed_view_input_validation.cc
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
 *
 * This file tests the TypedViewFilter.
 *
 */
#include <cmath>
#include <iostream>

#include <test/support/tdb_catch.h>
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/filter/typed_view_filter.h"

using namespace tiledb::sm;

TEST_CASE("TypedViewFilter: test basic functionality", "[filter][typed-view]") {
  // Test that the filter is created with the correct default type.
  TypedViewFilter filter;
  CHECK_THROWS(filter.output_datatype());

  // Test that the filter is created with the correct output type using the
  // typed constructor
  TypedViewFilter filter2(Datatype::UINT32);
  CHECK(filter2.output_datatype() == Datatype::UINT32);

  // Test that the filter is created with the correct output type using the
  // filter set option method
  TypedViewFilter filter3;
  Datatype t = Datatype::UINT32;
  CHECK(filter3.set_option(FilterOption::TYPED_VIEW_OUTPUT_DATATYPE, &t).ok());
  CHECK(filter3.output_datatype() == Datatype::UINT32);
  Datatype lookup_t = Datatype::ANY;
  CHECK(filter3.get_option(FilterOption::TYPED_VIEW_OUTPUT_DATATYPE, &lookup_t)
            .ok());
  CHECK(lookup_t == t);
}