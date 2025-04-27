/**
 * @file tiledb/sm/array_schema/test/unit_enumertion.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2025 TileDB, Inc.
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
 * This file contains unit tests for enumerations.
 */

#include "enumeration.h"
#include <test/support/tdb_catch.h>
#include "cxxbridge/enumeration/src/lib.rs.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/array_schema/enumeration.h"

using namespace tiledb::sm;

namespace tiledb::sm::test {

/**
 * @return an Enumeration
 */
std::shared_ptr<ConstEnumeration> create_enumeration(const std::string& name) {
  auto tracker = tiledb::test::create_test_memory_tracker();
  return Enumeration::create(
      name, Datatype::INT32, 1, false, nullptr, 0, nullptr, 0, tracker);
}

}  // namespace tiledb::sm::test

TEST_CASE("Enumeration empty name", "[oxidized][enumeration][name]") {
  REQUIRE(tiledb::oxidized::test::check_empty_enumeration_name());
}

/*
 * Every attribute must appear in both attribute containers. Indices and
 * pointers must be coherent. For example, if attribute(i).name() == s, then
 * attribute(i) == attribute(s).
 */
TEST_CASE("Enumeration name", "[oxidized][enumeration][name]") {
  REQUIRE(tiledb::oxidized::test::enumeration_name_prop());
}
