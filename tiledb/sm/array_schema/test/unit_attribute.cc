/**
 * @file tiledb/sm/array_schema/test/unit_arribute.cc
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
 * This file contains unit tests for the attribute class
 */

#include <tdb_catch.h>

#include "tiledb/sm/array_schema/attribute.h"

using namespace tiledb::common;
using namespace tiledb::sm;

/**
 * Datatype::ANY attributes are always var-sized.
 */
TEST_CASE(
    "Attribute - Datatype::ANY set_cell_val_num", "[array_schema][attribute]") {
  // Any type default cell val num
  {
    Attribute a("a", Datatype::ANY, false);
    CHECK(a.cell_val_num() == constants::var_num);
  }

  // Set cell val num at construction
  {
    CHECK_THROWS(Attribute("a", Datatype::ANY, 1, DataOrder::UNORDERED_DATA));
    CHECK_THROWS(Attribute("a", Datatype::ANY, 2, DataOrder::UNORDERED_DATA));

    REQUIRE_NOTHROW(Attribute(
        "a", Datatype::ANY, constants::var_num, DataOrder::UNORDERED_DATA));
    Attribute a(
        "a", Datatype::ANY, constants::var_num, DataOrder::UNORDERED_DATA);
    CHECK(a.cell_val_num() == constants::var_num);
  }

  // Set cell val num after construction
  {
    Attribute a("a", Datatype::ANY, false);
    CHECK(a.cell_val_num() == constants::var_num);

    CHECK_THROWS(a.set_cell_val_num(1));
    CHECK_THROWS(a.set_cell_val_num(2));
    CHECK_NOTHROW(a.set_cell_val_num(constants::var_num));
    CHECK(a.cell_val_num() == constants::var_num);
  }

  // long form constructor spot-check
  {
    FilterPipeline empty_pipeline;
    ByteVecValue empty_fill;
    CHECK_THROWS(Attribute(
        "a",
        Datatype::ANY,
        true,
        1,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::INCREASING_DATA,
        std::optional<std::string>()));
  }
}

/**
 * Datatype::STRING_ASCII has special rules for cell val num
 */
TEST_CASE(
    "Attribute - Datatype::STRING_ASCII set_cell_val_num",
    "[array_schema][attribute]") {
  // default is 1, and that's fine when un-ordered
  {
    Attribute a("a", Datatype::STRING_ASCII, false);
    CHECK(a.cell_val_num() == 1);
  }

  // set at construction, non-var is not fine when ordered
  {
    CHECK_THROWS(
        Attribute("a", Datatype::STRING_ASCII, 1, DataOrder::INCREASING_DATA));
    CHECK_THROWS(Attribute(
        "a", Datatype::STRING_ASCII, 100, DataOrder::INCREASING_DATA));
  }

  // set later, 1 is not fine when un-ordered
  {
    Attribute a(
        "a",
        Datatype::STRING_ASCII,
        constants::var_num,
        DataOrder::INCREASING_DATA);
    CHECK_THROWS(a.set_cell_val_num(1));
    CHECK_THROWS(a.set_cell_val_num(100));
    CHECK(a.cell_val_num() == constants::var_num);
  }

  // long form constructor spot-check
  {
    FilterPipeline empty_pipeline;
    ByteVecValue empty_fill;
    CHECK_THROWS(Attribute(
        "a",
        Datatype::STRING_ASCII,
        true,
        1,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::INCREASING_DATA,
        std::optional<std::string>()));
  }
}

/**
 * Other datatypes can be any size fixed or var unless ordered
 */
TEST_CASE(
    "Attribute - Datatype::INT32 set_cell_val_num",
    "[array_schema][attribute]") {
  // anything goes when unordered, default is 1
  {
    CHECK(Attribute("a", Datatype::INT32, false).cell_val_num() == 1);
    CHECK(
        Attribute("a", Datatype::INT32, 1, DataOrder::UNORDERED_DATA)
            .cell_val_num() == 1);
    CHECK(
        Attribute("a", Datatype::INT32, 100, DataOrder::UNORDERED_DATA)
            .cell_val_num() == 100);
    CHECK(
        Attribute(
            "a", Datatype::INT32, constants::var_num, DataOrder::UNORDERED_DATA)
            .cell_val_num() == constants::var_num);

    Attribute a("a", Datatype::INT32, false);
    a.set_cell_val_num(1);
    CHECK(a.cell_val_num() == 1);
    a.set_cell_val_num(10);
    CHECK(a.cell_val_num() == 10);
    a.set_cell_val_num(constants::var_num);
    CHECK(a.cell_val_num() == constants::var_num);
  }

  // set at construction, only 1 is allowed when ordered
  {
    CHECK_NOTHROW(
        Attribute("a", Datatype::INT32, 1, DataOrder::INCREASING_DATA));
    CHECK_THROWS(
        Attribute("a", Datatype::INT32, 100, DataOrder::INCREASING_DATA));
    CHECK_THROWS(Attribute(
        "a", Datatype::INT32, constants::var_num, DataOrder::INCREASING_DATA));
  }

  // set later, only 1 is allowed when ordered
  {
    Attribute a("a", Datatype::INT32, 1, DataOrder::INCREASING_DATA);
    CHECK_THROWS(a.set_cell_val_num(10));
    CHECK_THROWS(a.set_cell_val_num(constants::var_num));
    CHECK(a.cell_val_num() == 1);
  }

  // long form constructor spot-check
  {
    FilterPipeline empty_pipeline;
    ByteVecValue empty_fill;
    CHECK_THROWS(Attribute(
        "a",
        Datatype::INT32,
        true,
        100,
        empty_pipeline,
        empty_fill,
        0,
        DataOrder::INCREASING_DATA,
        std::optional<std::string>()));
  }
}
