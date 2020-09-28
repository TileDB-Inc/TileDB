/**
 * @file   unit-cppapi-fill_values.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the attribute fill values with the C++ API.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/constants.h"

#include <iostream>

using namespace tiledb;

void check_dump(const Attribute& attr, const std::string& gold_out) {
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  fwrite(gold_out.c_str(), sizeof(char), gold_out.size(), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  attr.dump(fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif

  // Clean up
  Context ctx;
  VFS vfs(ctx);
  CHECK_NOTHROW(vfs.remove_file("gold_fout.txt"));
  CHECK_NOTHROW(vfs.remove_file("fout.txt"));
}

void create_array_1d(
    const std::string& array_name,
    int32_t fill_int32 = tiledb::sm::constants::empty_int32,
    std::string fill_char = std::string() + tiledb::sm::constants::empty_char,
    std::array<double, 2> fill_double = {
        {tiledb::sm::constants::empty_float64,
         tiledb::sm::constants::empty_float64}}) {
  Context ctx;
  VFS vfs(ctx);

  Domain domain(ctx);
  auto d = Dimension::create<int32_t>(ctx, "d", {{1, 10}}, 5);
  domain.add_dimension(d);

  auto a1 = Attribute::create<int32_t>(ctx, "a1");
  a1.set_fill_value(&fill_int32, sizeof(fill_int32));
  auto a2 = Attribute::create<std::string>(ctx, "a2");
  a2.set_fill_value(fill_char.c_str(), fill_char.size());
  auto a3 = Attribute::create<double>(ctx, "a3");
  a3.set_cell_val_num(2);
  a3.set_fill_value(fill_double.data(), 2 * sizeof(double));

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a1, a2, a3);

  CHECK_NOTHROW(Array::create(array_name, schema));
}

void write_array_1d_partial(const std::string& array_name) {
  Context ctx;

  std::vector<int32_t> a1 = {3, 4};
  std::vector<char> a2_val = {'3', '3', '4', '4', '4'};
  std::vector<uint64_t> a2_off = {0, 2};
  std::vector<double> a3 = {3.1, 3.2, 4.1, 4.2};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  CHECK_NOTHROW(query.set_buffer("a1", a1));
  CHECK_NOTHROW(query.set_buffer("a2", a2_off, a2_val));
  CHECK_NOTHROW(query.set_buffer("a3", a3));
  CHECK_NOTHROW(query.set_subarray<int32_t>({3, 4}));
  CHECK_NOTHROW(query.set_layout(TILEDB_ROW_MAJOR));
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  array.close();
}

void read_array_1d_partial(
    const std::string& array_name,
    int32_t fill_int32 = tiledb::sm::constants::empty_int32,
    std::string fill_char = std::string() + tiledb::sm::constants::empty_char,
    std::array<double, 2> fill_double = {
        {tiledb::sm::constants::empty_float64,
         tiledb::sm::constants::empty_float64}}) {
  Context ctx;

  std::vector<int32_t> a1(10);
  std::vector<char> a2_val(100);
  std::vector<uint64_t> a2_off(20);
  std::vector<double> a3(20);

  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  CHECK_NOTHROW(query.set_buffer("a1", a1));
  CHECK_NOTHROW(query.set_buffer("a2", a2_off, a2_val));
  CHECK_NOTHROW(query.set_buffer("a3", a3));
  CHECK_NOTHROW(query.set_subarray<int32_t>({1, 10}));

  REQUIRE(query.submit() == Query::Status::COMPLETE);

  auto res = query.result_buffer_elements();
  REQUIRE(res["a1"].second == 10);
  REQUIRE(res["a2"].first == 10);
  REQUIRE(res["a2"].second == 5 + 8 * fill_char.size());
  REQUIRE(res["a3"].second == 20);

  uint64_t off = 0;
  for (size_t i = 0; i < 2; ++i) {
    CHECK(a1[i] == fill_int32);
    CHECK(a2_off[i] == off);
    for (size_t c = 0; c < fill_char.size(); ++c) {
      CHECK(a2_val[off] == fill_char[c]);
      ++off;
    }
    CHECK(!std::memcmp(&a3[2 * i], &fill_double[0], sizeof(double)));
    CHECK(!std::memcmp(&a3[2 * i + 1], &fill_double[1], sizeof(double)));
  }
  CHECK(a1[2] == 3);
  CHECK(a1[3] == 4);
  CHECK(a2_off[2] == off);
  CHECK(a2_val[off] == '3');
  CHECK(a2_val[off + 1] == '3');
  off += 2;
  CHECK(a2_off[3] == off);
  CHECK(a2_val[off] == '4');
  CHECK(a2_val[off + 1] == '4');
  CHECK(a2_val[off + 2] == '4');
  off += 3;
  CHECK(a3[4] == 3.1);
  CHECK(a3[5] == 3.2);
  CHECK(a3[6] == 4.1);
  CHECK(a3[7] == 4.2);
  for (size_t i = 4; i < 10; ++i) {
    CHECK(a1[i] == fill_int32);
    CHECK(a2_off[i] == off);
    for (size_t c = 0; c < fill_char.size(); ++c) {
      CHECK(a2_val[off] == fill_char[c]);
      ++off;
    }
    CHECK(!std::memcmp(&a3[2 * i], &fill_double[0], sizeof(double)));
    CHECK(!std::memcmp(&a3[2 * i + 1], &fill_double[1], sizeof(double)));
  }

  array.close();
}

void read_array_1d_empty(
    const std::string& array_name,
    int32_t fill_int32 = tiledb::sm::constants::empty_int32,
    std::string fill_char = std::string() + tiledb::sm::constants::empty_char,
    std::array<double, 2> fill_double = {
        {tiledb::sm::constants::empty_float64,
         tiledb::sm::constants::empty_float64}}) {
  Context ctx;

  std::vector<int32_t> a1(10);
  std::vector<char> a2_val(100);
  std::vector<uint64_t> a2_off(20);
  std::vector<double> a3(20);

  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  CHECK_NOTHROW(query.set_buffer("a1", a1));
  CHECK_NOTHROW(query.set_buffer("a2", a2_off, a2_val));
  CHECK_NOTHROW(query.set_buffer("a3", a3));
  CHECK_NOTHROW(query.set_subarray<int32_t>({1, 10}));

  REQUIRE(query.submit() == Query::Status::COMPLETE);

  auto res = query.result_buffer_elements();
  REQUIRE(res["a1"].second == 10);
  REQUIRE(res["a2"].first == 10);
  REQUIRE(res["a2"].second == 10 * fill_char.size());
  REQUIRE(res["a3"].second == 20);

  uint64_t off = 0;
  for (size_t i = 0; i < 10; ++i) {
    CHECK(a1[i] == fill_int32);
    CHECK(a2_off[i] == off);
    for (size_t c = 0; c < fill_char.size(); ++c) {
      CHECK(a2_val[off] == fill_char[c]);
      ++off;
    }
    CHECK(!std::memcmp(&a3[2 * i], &fill_double[0], sizeof(double)));
    CHECK(!std::memcmp(&a3[2 * i + 1], &fill_double[1], sizeof(double)));
  }

  array.close();
}

TEST_CASE(
    "C++ API: Test fill values, basic errors", "[cppapi][fill-values][basic]") {
  int32_t value = 5;
  uint64_t value_size = sizeof(int32_t);

  Context ctx;

  // Fixed-sized
  auto a = tiledb::Attribute::create<int32_t>(ctx, "a");

  // Null value
  CHECK_THROWS(a.set_fill_value(nullptr, value_size));

  // Zero size
  CHECK_THROWS(a.set_fill_value(&value, 0));

  // Wrong size
  CHECK_THROWS(a.set_fill_value(&value, 100));

  // Get default
  const void* value_ptr;
  CHECK_NOTHROW(a.get_fill_value(&value_ptr, &value_size));
  CHECK(*(const int32_t*)value_ptr == -2147483648);
  CHECK(value_size == sizeof(int32_t));

  // Check dump
  std::string dump = std::string("### Attribute ###\n") + "- Name: a\n" +
                     "- Type: INT32\n" + "- Nullable: false\n" +
                     "- Cell val num: 1\n" + "- Filters: 0\n" +
                     "- Fill value: -2147483648\n";
  check_dump(a, dump);

  // Correct setter
  CHECK_NOTHROW(a.set_fill_value(&value, value_size));

  // Get the set value
  CHECK_NOTHROW(a.get_fill_value(&value_ptr, &value_size));
  CHECK(*(const int32_t*)value_ptr == 5);
  CHECK(value_size == sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Nullable: false\n" + "- Cell val num: 1\n" +
         "- Filters: 0\n" + "- Fill value: 5\n";
  check_dump(a, dump);

  // Setting the cell val num, also sets the fill value to a new default
  CHECK_NOTHROW(a.set_cell_val_num(2));
  CHECK_NOTHROW(a.get_fill_value(&value_ptr, &value_size));
  CHECK(((const int32_t*)value_ptr)[0] == -2147483648);
  CHECK(((const int32_t*)value_ptr)[1] == -2147483648);
  CHECK(value_size == 2 * sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Nullable: false\n" + "- Cell val num: 2\n" +
         "- Filters: 0\n" + "- Fill value: -2147483648, -2147483648\n";
  check_dump(a, dump);

  // Set a fill value that is comprised of two integers
  int32_t value_2[2] = {1, 2};
  CHECK_NOTHROW(a.set_fill_value(value_2, sizeof(value_2)));

  // Get the new value back
  CHECK_NOTHROW(a.get_fill_value(&value_ptr, &value_size));
  CHECK(((const int32_t*)value_ptr)[0] == 1);
  CHECK(((const int32_t*)value_ptr)[1] == 2);
  CHECK(value_size == 2 * sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Nullable: false\n" + "- Cell val num: 2\n" +
         "- Filters: 0\n" + "- Fill value: 1, 2\n";
  check_dump(a, dump);

  // Make the attribute var-sized
  CHECK_NOTHROW(a.set_cell_val_num(TILEDB_VAR_NUM));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Nullable: false\n" + "- Cell val num: var\n" +
         "- Filters: 0\n" + "- Fill value: -2147483648\n";
  check_dump(a, dump);

  // Get the default var-sized fill value
  CHECK_NOTHROW(a.get_fill_value(&value_ptr, &value_size));
  CHECK(*(const int32_t*)value_ptr == -2147483648);
  CHECK(value_size == sizeof(int32_t));

  // Set a new fill value for the var-sized attribute
  int32_t value_3[3] = {1, 2, 3};
  CHECK_NOTHROW(a.set_fill_value(value_3, sizeof(value_3)));

  // Get the new fill value
  CHECK_NOTHROW(a.get_fill_value(&value_ptr, &value_size));
  CHECK(((const int32_t*)value_ptr)[0] == 1);
  CHECK(((const int32_t*)value_ptr)[1] == 2);
  CHECK(((const int32_t*)value_ptr)[2] == 3);
  CHECK(value_size == 3 * sizeof(int32_t));

  // Check dump
  dump = std::string("### Attribute ###\n") + "- Name: a\n" +
         "- Type: INT32\n" + "- Nullable: false\n" + "- Cell val num: var\n" +
         "- Filters: 0\n" + "- Fill value: 1, 2, 3\n";
  check_dump(a, dump);
}

TEST_CASE(
    "C++ API: Test fill values, partial array",
    "[cppapi][fill-values][partial]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "fill_values_partial";

  // First test with default fill values
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  create_array_1d(array_name);
  write_array_1d_partial(array_name);
  read_array_1d_partial(array_name);

  CHECK_NOTHROW(vfs.remove_dir(array_name));

  std::string s("abc");
  create_array_1d(array_name, 0, s, {1.0, 2.0});
  write_array_1d_partial(array_name);
  read_array_1d_partial(array_name, 0, s, {1.0, 2.0});

  CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test fill values, empty array", "[cppapi][fill-values][empty]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "fill_values_empty";

  // First test with default fill values
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  create_array_1d(array_name);
  read_array_1d_empty(array_name);

  CHECK_NOTHROW(vfs.remove_dir(array_name));

  std::string s("abc");
  create_array_1d(array_name, 0, s, {1.0, 2.0});
  read_array_1d_empty(array_name, 0, s, {1.0, 2.0});

  CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test result estimation, empty dense arrays",
    "[cppapi][fill-values][empty][est-result]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "fill_values_est_result_empty";

  // First test with default fill values
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  SECTION("- Default fill values") {
    create_array_1d(array_name);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    auto est_a1 = query.est_result_size("a1");
    auto est_a2 = query.est_result_size_var("a2");
    auto est_a3 = query.est_result_size("a3");
    auto est_d = query.est_result_size("d");
    CHECK(est_d == 10 * sizeof(int32_t));
    CHECK(est_a1 == 10 * sizeof(int32_t));
    CHECK(est_a2.first == 10);
    CHECK(est_a2.second == 10 * sizeof(char));
    CHECK(est_a3 == 10 * 2 * sizeof(double));
  }

  SECTION("- Custom fill values") {
    std::string s("abc");
    create_array_1d(array_name, 0, s, {1.0, 2.0});

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    auto est_a1 = query.est_result_size("a1");
    auto est_a2 = query.est_result_size_var("a2");
    auto est_a3 = query.est_result_size("a3");
    auto est_d = query.est_result_size("d");
    CHECK(est_d == 10 * sizeof(int32_t));
    CHECK(est_a1 == 10 * sizeof(int32_t));
    CHECK(est_a2.first == 10);
    CHECK(est_a2.second == 10 * 3 * sizeof(char));
    CHECK(est_a3 == 10 * 2 * sizeof(double));
  }

  SECTION("- Default fill values, multi-range") {
    create_array_1d(array_name);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    query.add_range<int32_t>(0, 2, 3);
    query.add_range<int32_t>(0, 9, 10);
    auto est_a1 = query.est_result_size("a1");
    auto est_a2 = query.est_result_size_var("a2");
    auto est_a3 = query.est_result_size("a3");
    auto est_d = query.est_result_size("d");
    CHECK(est_d == 4 * sizeof(int32_t));
    CHECK(est_a1 == 4 * sizeof(int32_t));
    CHECK(est_a2.first == 4);
    CHECK(est_a2.second == 4 * sizeof(char));
    CHECK(est_a3 == 4 * 2 * sizeof(double));
  }

  CHECK_NOTHROW(vfs.remove_dir(array_name));
}

TEST_CASE(
    "C++ API: Test result estimation, partial dense arrays",
    "[cppapi][fill-values][partial][est-result]") {
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "fill_values_est_result_partial";

  // First test with default fill values
  if (vfs.is_dir(array_name))
    CHECK_NOTHROW(vfs.remove_dir(array_name));

  SECTION("- Default fill values") {
    create_array_1d(array_name);
    write_array_1d_partial(array_name);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    auto est_a1 = query.est_result_size("a1");
    auto est_a2 = query.est_result_size_var("a2");
    auto est_a3 = query.est_result_size("a3");
    auto est_d = query.est_result_size("d");
    CHECK(est_d == 10 * sizeof(int32_t));
    CHECK(est_a1 == 10 * sizeof(int32_t));
    CHECK(est_a2.first == 10);
    CHECK(est_a2.second == 10 * sizeof(char));
    CHECK(est_a3 == 10 * 2 * sizeof(double));
  }

  SECTION("- Custom fill values") {
    std::string s("abc");
    create_array_1d(array_name, 0, s, {1.0, 2.0});
    write_array_1d_partial(array_name);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    auto est_a1 = query.est_result_size("a1");
    auto est_a2 = query.est_result_size_var("a2");
    auto est_a3 = query.est_result_size("a3");
    auto est_d = query.est_result_size("d");
    CHECK(est_d == 10 * sizeof(int32_t));
    CHECK(est_a1 == 10 * sizeof(int32_t));
    CHECK(est_a2.first == 10);
    CHECK(est_a2.second == 10 * 3 * sizeof(char));
    CHECK(est_a3 == 10 * 2 * sizeof(double));
  }

  SECTION("- Default fill values, multi-range") {
    create_array_1d(array_name);
    write_array_1d_partial(array_name);

    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    query.add_range<int32_t>(0, 2, 3);
    query.add_range<int32_t>(0, 9, 10);
    auto est_a1 = query.est_result_size("a1");
    auto est_a2 = query.est_result_size_var("a2");
    auto est_a3 = query.est_result_size("a3");
    auto est_d = query.est_result_size("d");
    CHECK(est_d == 4 * sizeof(int32_t));
    CHECK(est_a1 == 4 * sizeof(int32_t));
    CHECK(est_a2.first == 4);
    CHECK(est_a2.second == 4 * sizeof(char));
    CHECK(est_a3 == 4 * 2 * sizeof(double));
  }

  CHECK_NOTHROW(vfs.remove_dir(array_name));
}
