/**
 * @file   unit-cppapi-type.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C++ API for type related dispatch.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

struct MyData {
  int a;
  float b;
  double c[3];
};

TEST_CASE("C++ API: Types", "[cppapi][types]") {
  using namespace tiledb;
  Context ctx;

  CHECK(
      (std::is_same<typename impl::TypeHandler<int>::value_type, int>::value));
  CHECK((std::is_same<typename impl::TypeHandler<int[5]>::value_type, int>::
             value));
  CHECK((std::is_same<typename impl::TypeHandler<MyData>::value_type, MyData>::
             value));
  CHECK(
      (std::is_same<typename impl::TypeHandler<std::string>::value_type, char>::
           value));

  CHECK_THROWS(impl::type_check<MyData>(TILEDB_INT8));
  CHECK_NOTHROW(impl::type_check<MyData>(TILEDB_STRING_ASCII, sizeof(MyData)));

  // static char arrays are ok for tiledb string types as long as the static
  // lengths are equal
  CHECK_THROWS(impl::type_check<char[10]>(TILEDB_STRING_ASCII, 9));
  CHECK_NOTHROW(impl::type_check<char[10]>(TILEDB_STRING_ASCII, 10));
  CHECK_NOTHROW(
      impl::type_check<char[10]>(TILEDB_STRING_ASCII, TILEDB_VAR_NUM));

  // const string data pointer should succeed
  CHECK_NOTHROW(
      impl::type_check<const char*>(TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<const char*>(TILEDB_STRING_UTF8, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<const char*>(TILEDB_STRING_UTF16, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<const char*>(TILEDB_STRING_UCS2, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<const char*>(TILEDB_STRING_UTF32, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<const char*>(TILEDB_STRING_UCS4, TILEDB_VAR_NUM));

  // std::basic_string type typecheck should succeed for tiledb string types
  CHECK_NOTHROW(
      impl::type_check<std::string>(TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<std::string>(TILEDB_STRING_UTF8, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<std::u16string>(TILEDB_STRING_UTF16, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<std::u16string>(TILEDB_STRING_UCS2, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<std::u32string>(TILEDB_STRING_UTF32, TILEDB_VAR_NUM));
  CHECK_NOTHROW(
      impl::type_check<std::u32string>(TILEDB_STRING_UCS4, TILEDB_VAR_NUM));

  // std:: container types of char datatypes should succeed for tiledb string
  // types
  CHECK_THROWS(impl::type_check<std::vector<int8_t>>(
      TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::vector<uint8_t>>(
      TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::vector<uint32_t>>(
      TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::vector<int8_t>>(
      TILEDB_STRING_UTF8, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::vector<uint8_t>>(
      TILEDB_STRING_UTF8, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::vector<uint32_t>>(
      TILEDB_STRING_UTF8, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::array<int8_t, 1>>(
      TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::array<uint8_t, 1>>(
      TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::array<uint32_t, 1>>(
      TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::array<int8_t, 1>>(
      TILEDB_STRING_UTF8, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::array<uint8_t, 1>>(
      TILEDB_STRING_UTF8, TILEDB_VAR_NUM));
  CHECK_THROWS(impl::type_check<std::array<uint32_t, 1>>(
      TILEDB_STRING_UTF8, TILEDB_VAR_NUM));

  CHECK_NOTHROW(
      impl::type_check<std::vector<char>>(TILEDB_STRING_ASCII, TILEDB_VAR_NUM));
  CHECK_NOTHROW(impl::type_check<std::array<char, 1>>(
      TILEDB_STRING_ASCII, TILEDB_VAR_NUM));

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<float>(ctx, "a2");
  auto a3 = Attribute::create<float[5]>(ctx, "a3");
  auto a4 = Attribute::create<MyData>(ctx, "a4");
  auto a5 = Attribute::create<MyData[5]>(ctx, "a5");
  auto a6 = Attribute::create<std::vector<MyData>>(ctx, "a6");
  auto a7 = Attribute::create<std::string>(ctx, "a7");
  auto a8 = Attribute::create<double>(ctx, "a8");
  auto a9 = Attribute::create<std::array<double, 5>>(ctx, "a9");

  CHECK(a1.type() == TILEDB_INT32);
  CHECK(a2.type() == TILEDB_FLOAT32);
  CHECK(a3.type() == TILEDB_FLOAT32);
  CHECK(a4.type() == TILEDB_STRING_ASCII);
  CHECK(a5.type() == TILEDB_STRING_ASCII);
  CHECK(a6.type() == TILEDB_STRING_ASCII);
  CHECK(a7.type() == TILEDB_STRING_ASCII);
  CHECK(a8.type() == TILEDB_FLOAT64);
  CHECK(a9.type() == TILEDB_FLOAT64);

  CHECK(a1.cell_val_num() == 1);
  CHECK(a2.cell_val_num() == 1);
  CHECK(a3.cell_val_num() == 5);
  CHECK(a4.cell_val_num() == sizeof(int) + sizeof(float) + sizeof(double[3]));
  CHECK(
      a5.cell_val_num() ==
      5 * (sizeof(int) + sizeof(float) + sizeof(double[3])));
  CHECK(a6.cell_val_num() == TILEDB_VAR_NUM);
  CHECK(a7.cell_val_num() == TILEDB_VAR_NUM);
  CHECK(a8.cell_val_num() == 1);
  CHECK(a9.cell_val_num() == 5);
}
