/**
 * @file test-cppapi-current-domain.cc
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
 * Tests the currentDomain C++ API
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <test/support/tdb_catch.h>

using namespace tiledb::test;

struct CurrentDomainFx {
  VFSTestSetup vfs_test_setup_;

  // Constructors/destructors.
  CurrentDomainFx();

  // TileDB context
  tiledb_ctx_t* ctx_c_;
  tiledb::Context ctx_;
};

CurrentDomainFx::CurrentDomainFx() {
  tiledb::Config config;
  ctx_c_ = vfs_test_setup_.ctx_c;
  ctx_ = vfs_test_setup_.ctx();
}

TEST_CASE_METHOD(
    CurrentDomainFx,
    "C++ API: CurrentDomain - Integer dimensions",
    "[cppapi][ArraySchema][currentDomain]") {
  // Create domain
  tiledb::Domain domain(ctx_);
  auto d1 = tiledb::Dimension::create<int32_t>(ctx_, "x", {{0, 100}}, 10);
  auto d2 = tiledb::Dimension::create<int32_t>(ctx_, "y", {{0, 100}}, 10);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create an NDRectangle and set ranges
  tiledb::NDRectangle ndrect(ctx_, domain);
  int range_one[] = {10, 20};
  int range_two[] = {30, 40};
  ndrect.set_range(0, range_one[0], range_one[1]);
  ndrect.set_range(1, range_two[0], range_two[1]);

  // Get and check ranges
  auto range = ndrect.range<int>(0);
  CHECK(range[0] == 10);
  CHECK(range[1] == 20);
  range = ndrect.range<int>(1);
  CHECK(range[0] == 30);
  CHECK(range[1] == 40);

  // Create a currentDomain and set the NDRectangle
  tiledb::CurrentDomain current_domain(ctx_);
  current_domain.set_ndrectangle(ndrect);

  CHECK_FALSE(current_domain.is_empty());

  auto rect = current_domain.ndrectangle();

  // Get and check ranges
  range = rect.range<int>(0);
  CHECK(range[0] == 10);
  CHECK(range[1] == 20);
  range = rect.range<int>(1);
  CHECK(range[0] == 30);
  CHECK(range[1] == 40);
}

TEST_CASE_METHOD(
    CurrentDomainFx,
    "C++ API: CurrentDomain - String dimensions",
    "[cppapi][ArraySchema][string-dims][currentDomain]") {
  // Create domain
  auto d1 = tiledb::Dimension::create(
      ctx_, "d1", TILEDB_STRING_ASCII, nullptr, nullptr);
  auto d2 = tiledb::Dimension::create(
      ctx_, "d2", TILEDB_STRING_ASCII, nullptr, nullptr);
  tiledb::Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create an NDRectangle and set ranges
  tiledb::NDRectangle ndrect(ctx_, domain);
  ndrect.set_range(0, std::string("a"), std::string("c"));
  ndrect.set_range(1, std::string("b"), std::string("db"));

  // Get and check ranges
  std::array<std::string, 2> range = ndrect.range(0);
  CHECK(range[0] == "a");
  CHECK(range[1] == "c");
  range = ndrect.range(1);
  CHECK(range[0] == "b");
  CHECK(range[1] == "db");

  // Create a currentDomain and set the NDRectangle
  tiledb::CurrentDomain current_domain(ctx_);
  current_domain.set_ndrectangle(ndrect);

  CHECK_FALSE(current_domain.is_empty());

  auto rect = current_domain.ndrectangle();

  // Get and check ranges
  range = ndrect.range(0);
  CHECK(range[0] == "a");
  CHECK(range[1] == "c");
  range = ndrect.range(1);
  CHECK(range[0] == "b");
  CHECK(range[1] == "db");
}

TEST_CASE_METHOD(
    CurrentDomainFx,
    "C++ API: CurrentDomain - Add to ArraySchema",
    "[cppapi][ArraySchema][currentDomain]") {
  // Create domain.
  tiledb::Domain domain(ctx_);
  auto d = tiledb::Dimension::create<int32_t>(ctx_, "d", {{1, 999}}, 2);
  domain.add_dimension(d);

  // Create array schema.
  tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(tiledb::Attribute::create<int>(ctx_, "a"));

  // Create NDRectangle and set ranges
  tiledb::NDRectangle ndrect(ctx_, domain);
  int range_one[] = {10, 20};
  ndrect.set_range(0, range_one[0], range_one[1]);

  // Create the currentDomain and set NDRectangle
  tiledb::CurrentDomain current_domain(ctx_);
  current_domain.set_ndrectangle(ndrect);

  CHECK_NOTHROW(tiledb::ArraySchemaExperimental::set_current_domain(
      ctx_, schema, current_domain));

  auto cd = tiledb::ArraySchemaExperimental::current_domain(ctx_, schema);
  CHECK(!cd.is_empty());

  // Check if ranges are the same
  CHECK(cd.ndrectangle().range<int>(0)[0] == ndrect.range<int>(0)[0]);
  CHECK(cd.ndrectangle().range<int>(0)[1] == ndrect.range<int>(0)[1]);

  CHECK_THROWS(cd.ndrectangle().range<int>(1));
}

TEST_CASE_METHOD(
    CurrentDomainFx,
    "C++ API: CurrentDomain - Evolve",
    "[cppapi][ArraySchema][currentDomain]") {
  const std::string array_name = "test_current_domain_expansion";

  tiledb::VFS vfs(ctx_);
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create domain.
  tiledb::Domain domain(ctx_);
  auto d = tiledb::Dimension::create<int32_t>(ctx_, "d", {{1, 999}}, 2);
  domain.add_dimension(d);

  // Create array schema.
  tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(tiledb::Attribute::create<int>(ctx_, "a"));

  // Create NDRectangle and set ranges
  tiledb::NDRectangle ndrect(ctx_, domain);
  int range_one[] = {10, 20};
  ndrect.set_range(0, range_one[0], range_one[1]);

  // Create the currentDomain and set NDRectangle
  tiledb::CurrentDomain current_domain(ctx_);
  current_domain.set_ndrectangle(ndrect);

  tiledb::ArraySchemaExperimental::set_current_domain(
      ctx_, schema, current_domain);

  // Create array
  tiledb::Array::create(array_name, schema);

  // Create new currentDomain to test evolution
  tiledb::CurrentDomain new_current_domain(ctx_);
  int range_two[] = {5, 30};
  tiledb::NDRectangle ndrect_two(ctx_, domain);
  ndrect_two.set_range(0, range_two[0], range_two[1]);
  new_current_domain.set_ndrectangle(ndrect_two);

  // Schema evolution
  tiledb::ArraySchemaEvolution se(ctx_);
  se.expand_current_domain(new_current_domain);
  se.array_evolve(array_name);

  // Open array to check the ranges
  tiledb::Array array(ctx_, array_name, TILEDB_READ);
  auto s = array.schema();
  auto cd = tiledb::ArraySchemaExperimental::current_domain(ctx_, s);
  auto n = cd.ndrectangle();
  CHECK(n.range<int>(0)[0] == ndrect_two.range<int>(0)[0]);
  CHECK(n.range<int>(0)[1] == ndrect_two.range<int>(0)[1]);

  // Clean up.
  array.close();
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE_METHOD(
    CurrentDomainFx,
    "C++ API: CurrentDomain - Write outside of current domain",
    "[cppapi][ArraySchema][currentDomain]") {
  const std::string array_name = "test_current_domain_write";

  tiledb::VFS vfs(ctx_);
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create domain
  tiledb::Domain domain(ctx_);
  auto d1 = tiledb::Dimension::create<int32_t>(ctx_, "dim1", {{0, 100}}, 10);
  auto d2 = tiledb::Dimension::create(
      ctx_, "dim2", TILEDB_STRING_ASCII, nullptr, nullptr);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create an NDRectangle and set ranges
  tiledb::NDRectangle ndrect(ctx_, domain);
  int range_one[] = {10, 20};
  ndrect.set_range(0, range_one[0], range_one[1]);
  ndrect.set_range(1, std::string("b"), std::string("db"));

  // Create a currentDomain and set the NDRectangle
  tiledb::CurrentDomain current_domain(ctx_);
  current_domain.set_ndrectangle(ndrect);

  // Create array schema.
  tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(tiledb::Attribute::create<int>(ctx_, "a"));

  tiledb::ArraySchemaExperimental::set_current_domain(
      ctx_, schema, current_domain);

  // Create array
  tiledb::Array::create(array_name, schema);

  tiledb::Array array(ctx_, array_name, TILEDB_WRITE);

  // Some of the data here is outside of the current domain.
  std::vector<int32_t> dim1 = {12, 14};
  std::vector<int32_t> dim1_out = {12, 22};
  std::vector<char> dim2 = {'b', 'c'};
  std::vector<uint64_t> dim2_offsets = {0, 1};
  std::vector<char> dim2_out = {'a', 'c'};
  std::vector<int> a1 = {1, 2};

  // All data in current domain.
  tiledb::Query query1(ctx_, array, TILEDB_WRITE);
  query1.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", a1)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim2", dim2_offsets)
      .set_data_buffer("dim2", dim2);
  query1.submit();

  // Dimension 1 data out of current domain.
  tiledb::Query query2(ctx_, array, TILEDB_WRITE);
  query2.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", a1)
      .set_data_buffer("dim1", dim1_out)
      .set_offsets_buffer("dim2", dim2_offsets)
      .set_data_buffer("dim2", dim2);
  CHECK_THROWS_WITH(
      query2.submit(),
      Catch::Matchers::ContainsSubstring(
          "Cells are written outside of the defined current domain."));

  // Dimension 2 data out of current domain.
  tiledb::Query query3(ctx_, array, TILEDB_WRITE);
  query3.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", a1)
      .set_data_buffer("dim1", dim1)
      .set_offsets_buffer("dim2", dim2_offsets)
      .set_data_buffer("dim2", dim2_out);
  CHECK_THROWS_WITH(
      query3.submit(),
      Catch::Matchers::ContainsSubstring(
          "Cells are written outside of the defined current domain."));
  array.close();

  // Create new currentDomain to allow all data
  tiledb::CurrentDomain new_current_domain(ctx_);
  int range_two[] = {10, 22};
  tiledb::NDRectangle ndrect_two(ctx_, domain);
  ndrect_two.set_range(0, range_two[0], range_two[1]);
  ndrect_two.set_range(1, std::string("a"), std::string("db"));
  new_current_domain.set_ndrectangle(ndrect_two);

  // Schema evolution
  tiledb::ArraySchemaEvolution se(ctx_);
  se.expand_current_domain(new_current_domain);
  se.array_evolve(array_name);

  // Now try data that didn't succeed before.
  tiledb::Array array2(ctx_, array_name, TILEDB_WRITE);
  tiledb::Query query4(ctx_, array2, TILEDB_WRITE);
  query4.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", a1)
      .set_data_buffer("dim1", dim1_out)
      .set_offsets_buffer("dim2", dim2_offsets)
      .set_data_buffer("dim2", dim2_out);
  query4.submit();
  array2.close();

  // Clean up.
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}
