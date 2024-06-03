/**
 * @file tiledb/sm/array_schema/test/unit_arribute.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file contains unit tests for the attribute class
 */

#include <tdb_catch.h>

#include "tiledb/sm/array_schema/attribute.h"

using namespace tiledb::common;
using namespace tiledb::sm;

// Datatype::ANY attribute is var-sized by default
TEST_CASE("ANY Attribute default cell val num", "[array_schema][attribute]") {
  Attribute a("a", Datatype::ANY, false);
  CHECK(a.cell_val_num() == constants::var_num);
}

// Pass non-default cell val num to constructor
TEST_CASE("ANY Attribute cell val num validity", "[array_schema][attribute]") {
  SECTION(
      "Constructor: default cell val num is `constants::var_num` for "
      "`Datatype::ANY`") {
    CHECK(
        Attribute("a", Datatype::ANY, false).cell_val_num() ==
        constants::var_num);
  }

  FilterPipeline empty_pipeline;
  ByteVecValue empty_fill;

  SECTION("Constructor: `constants::var_num` is allowed for `Datatype::ANY`") {
    CHECK_NOTHROW(Attribute(
        "a", Datatype::ANY, constants::var_num, DataOrder::UNORDERED_DATA));
    CHECK_NOTHROW(Attribute(
        "a",
        Datatype::ANY,
        false,
        constants::var_num,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::UNORDERED_DATA));
  }

  SECTION(
      "set_cell_val_num: no error when redundantly setting "
      "`constants::var_num`") {
    Attribute a("a", Datatype::ANY, false);
    REQUIRE(a.cell_val_num() == constants::var_num);
    REQUIRE_NOTHROW(a.set_cell_val_num(constants::var_num));
    CHECK(a.cell_val_num() == constants::var_num);
  }

  const unsigned int invalid_cell_val_num = GENERATE(0, 1, 2, 5, 10, 100, 1000);

  SECTION(
      "Constructor: only `constants::var_num` is allowed for `Datatype::ANY`") {
    CHECK_THROWS(Attribute(
        "a", Datatype::ANY, invalid_cell_val_num, DataOrder::UNORDERED_DATA));
    CHECK_THROWS(Attribute(
        "a",
        Datatype::ANY,
        false,
        invalid_cell_val_num,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::UNORDERED_DATA));
  }

  SECTION(
      "set_cell_val_num: only `constants::var_num` is allowed for "
      "`Datatype::ANY`") {
    Attribute a("a", Datatype::ANY, false);
    const auto cell_val_num = a.cell_val_num();
    REQUIRE_THROWS(a.set_cell_val_num(invalid_cell_val_num));
    CHECK(a.cell_val_num() == cell_val_num);
  }
}

TEST_CASE(
    "Non-ANY unordered Attribute cell val num validity",
    "[array_schema][attribute]") {
  // Includes all datatypes except Datatype::ANY which is covered above
  const auto attribute_datatype = GENERATE(
      Datatype::FLOAT32,
      Datatype::FLOAT64,
      Datatype::INT8,
      Datatype::UINT8,
      Datatype::INT16,
      Datatype::UINT16,
      Datatype::INT32,
      Datatype::UINT32,
      Datatype::INT64,
      Datatype::UINT64,
      Datatype::STRING_ASCII,
      Datatype::STRING_UTF8,
      Datatype::STRING_UTF16,
      Datatype::STRING_UTF32,
      Datatype::STRING_UCS2,
      Datatype::STRING_UCS4,
      Datatype::DATETIME_YEAR,
      Datatype::DATETIME_WEEK,
      Datatype::DATETIME_DAY,
      Datatype::DATETIME_HR,
      Datatype::DATETIME_MIN,
      Datatype::DATETIME_SEC,
      Datatype::DATETIME_MS,
      Datatype::DATETIME_US,
      Datatype::DATETIME_NS,
      Datatype::DATETIME_PS,
      Datatype::DATETIME_FS,
      Datatype::DATETIME_AS,
      Datatype::TIME_HR,
      Datatype::TIME_MIN,
      Datatype::TIME_SEC,
      Datatype::TIME_MS,
      Datatype::TIME_US,
      Datatype::TIME_NS,
      Datatype::TIME_PS,
      Datatype::TIME_FS,
      Datatype::TIME_AS,
      Datatype::BOOL,
      Datatype::BLOB,
      Datatype::GEOM_WKT,
      Datatype::GEOM_WKB);

  DYNAMIC_SECTION(
      "Constructor: zero cell val num is invalid for datatype " +
      datatype_str(attribute_datatype)) {
    CHECK_THROWS(
        Attribute("a", attribute_datatype, 0, DataOrder::UNORDERED_DATA));
  }

  const unsigned int valid_cell_val_num =
      GENERATE(1, 2, 5, 10, 100, 1000, constants::var_num);

  DYNAMIC_SECTION(
      "Constructor: default cell val num is 1 for datatype " +
      datatype_str(attribute_datatype)) {
    CHECK(Attribute("a", attribute_datatype, false).cell_val_num() == 1);
  }

  DYNAMIC_SECTION(
      "Constructor: all nonzero cell val nums are valid for unordered "
      "attribute with "
      "datatype " +
      datatype_str(attribute_datatype)) {
    CHECK(
        Attribute(
            "a",
            attribute_datatype,
            valid_cell_val_num,
            DataOrder::UNORDERED_DATA)
            .cell_val_num() == valid_cell_val_num);
  }

  DYNAMIC_SECTION(
      "set_cell_val_num: all nonzero cell val nums are valid for unordered "
      "attribute "
      "with datatype " +
      datatype_str(attribute_datatype)) {
    Attribute a("a", attribute_datatype, 1, DataOrder::UNORDERED_DATA);
    REQUIRE(a.cell_val_num() == 1);
    REQUIRE_NOTHROW(a.set_cell_val_num(valid_cell_val_num));
    CHECK(a.cell_val_num() == valid_cell_val_num);
  }

  DYNAMIC_SECTION(
      "set_cell_val_num: zero cell val num is invalid for datatype " +
      datatype_str(attribute_datatype)) {
    Attribute a(
        "a", attribute_datatype, valid_cell_val_num, DataOrder::UNORDERED_DATA);
    REQUIRE(a.cell_val_num() == valid_cell_val_num);
    REQUIRE_THROWS(a.set_cell_val_num(0));
    CHECK(a.cell_val_num() == valid_cell_val_num);
  }
}

TEST_CASE(
    "STRING_ASCII ordered Attribute cell val num validity"
    "[array_schema][attribute]") {
  const unsigned int invalid_cell_val_num = GENERATE(0, 1, 2, 5, 10, 100, 1000);

  FilterPipeline empty_pipeline;
  ByteVecValue empty_fill;

  SECTION(
      "Constructor: anything other than `constants::var_num` is invalid for "
      "increasing "
      "STRING_ASCII attribute") {
    CHECK_THROWS(Attribute(
        "a",
        Datatype::STRING_ASCII,
        invalid_cell_val_num,
        DataOrder::INCREASING_DATA));

    CHECK_THROWS(Attribute(
        "a",
        Datatype::STRING_ASCII,
        true,
        invalid_cell_val_num,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::INCREASING_DATA,
        std::optional<std::string>()));
  }

  SECTION(
      "set_cell_val_num: anything other than `constants::var_num` is invalid "
      "for increasing STRING_ASCII attribute") {
    Attribute a(
        "a",
        Datatype::STRING_ASCII,
        constants::var_num,
        DataOrder::INCREASING_DATA);
    REQUIRE(a.cell_val_num() == constants::var_num);
    REQUIRE_THROWS(a.set_cell_val_num(invalid_cell_val_num));
    CHECK(a.cell_val_num() == constants::var_num);
  }
}

TEST_CASE(
    "Non-ANY, Non-STRING_ASCII ordered Attribute cell val num validity",
    "[array_schema][attribute]") {
  // Datatype::ANY and string datatypes intentionally omitted
  const auto attribute_datatype = GENERATE(
      Datatype::FLOAT32,
      Datatype::FLOAT64,
      Datatype::INT8,
      Datatype::UINT8,
      Datatype::INT16,
      Datatype::UINT16,
      Datatype::INT32,
      Datatype::UINT32,
      Datatype::INT64,
      Datatype::UINT64,
      // STRING_ASCII omitted due to different semantics, see other TEST_CASEs
      // Other string datatypes omitted as they are not valid for ordered
      // attributes.  See `ensure_ordered_attribute_datatype_is_valid`.
      Datatype::DATETIME_YEAR,
      Datatype::DATETIME_WEEK,
      Datatype::DATETIME_DAY,
      Datatype::DATETIME_HR,
      Datatype::DATETIME_MIN,
      Datatype::DATETIME_SEC,
      Datatype::DATETIME_MS,
      Datatype::DATETIME_US,
      Datatype::DATETIME_NS,
      Datatype::DATETIME_PS,
      Datatype::DATETIME_FS,
      Datatype::DATETIME_AS,
      Datatype::TIME_HR,
      Datatype::TIME_MIN,
      Datatype::TIME_SEC,
      Datatype::TIME_MS,
      Datatype::TIME_US,
      Datatype::TIME_NS,
      Datatype::TIME_PS,
      Datatype::TIME_FS,
      Datatype::TIME_AS);

  FilterPipeline empty_pipeline;
  ByteVecValue empty_fill;

  DYNAMIC_SECTION(
      "Constructor: cell val num 1 is valid for increasing attribute with "
      "datatype " +
      datatype_str(attribute_datatype)) {
    CHECK_NOTHROW(
        Attribute("a", attribute_datatype, 1, DataOrder::INCREASING_DATA));
    CHECK_NOTHROW(Attribute(
        "a",
        attribute_datatype,
        true,
        1,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::INCREASING_DATA,
        std::optional<std::string>()));
  }

  DYNAMIC_SECTION(
      "set_cell_val_num: cell val num 1 is valid for increasing attribute with "
      "datatype " +
      datatype_str(attribute_datatype)) {
    Attribute a("a", attribute_datatype, 1, DataOrder::INCREASING_DATA);
    REQUIRE(a.cell_val_num() == 1);
    CHECK_NOTHROW(a.set_cell_val_num(1));
    CHECK(a.cell_val_num() == 1);
  }

  const unsigned int invalid_cell_val_num =
      GENERATE(0, 2, 5, 10, 100, 1000, constants::var_num);

  DYNAMIC_SECTION(
      "Constructor: cell val num " + std::to_string(invalid_cell_val_num) +
      " is invalid for increasing attribute with datatype " +
      datatype_str(attribute_datatype)) {
    CHECK_THROWS(Attribute(
        "a",
        attribute_datatype,
        invalid_cell_val_num,
        DataOrder::INCREASING_DATA));
    CHECK_THROWS(Attribute(
        "a",
        attribute_datatype,
        true,
        invalid_cell_val_num,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::INCREASING_DATA,
        std::optional<std::string>()));
  }

  DYNAMIC_SECTION(
      "set_cell_val_num: cell val num " + std::to_string(invalid_cell_val_num) +
      " is invalid for increasing attribute with datatype " +
      datatype_str(attribute_datatype)) {
    Attribute a("a", attribute_datatype, 1, DataOrder::INCREASING_DATA);
    REQUIRE(a.cell_val_num() == 1);
    REQUIRE_THROWS(a.set_cell_val_num(invalid_cell_val_num));
    CHECK(a.cell_val_num() == 1);
  }
}
