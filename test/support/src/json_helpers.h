/**
 * @file   json_helpers.h
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
 * This file declares some test suite helper functionsv to parse a JSON files to
 * create arrays, write fragments, etc.
 */

#ifndef TILEDB_TEST_JSON_HELPERS_H
#define TILEDB_TEST_JSON_HELPERS_H

#include <test/support/tdb_catch.h>
#include "external/include/nlohmann/json.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/datatype.h"

using namespace nlohmann;

namespace tiledb::test {

class JsonTestParser {
 public:
  JsonTestParser() = delete;

  JsonTestParser(Context& ctx)
      : ctx_(ctx) {
  }

  template <class T>
  Dimension create_dim(std::string name) {
    return Dimension::create<T>(ctx_, name, {{1, 4}}, 2);
  }

  Dimension create_dim(std::string name, std::string type) {
    sm::Datatype t;
    REQUIRE(sm::datatype_enum(type, &t).ok());
    switch (t) {
      case sm::Datatype::INT32:
        return create_dim<int32_t>(name);
      case sm::Datatype::INT64:
        return create_dim<int64_t>(name);
      case sm::Datatype::INT8:
        return create_dim<int8_t>(name);
      case sm::Datatype::UINT8:
        return create_dim<uint8_t>(name);
      case sm::Datatype::INT16:
        return create_dim<int16_t>(name);
      case sm::Datatype::UINT16:
        return create_dim<uint16_t>(name);
      case sm::Datatype::UINT32:
        return create_dim<uint32_t>(name);
      case sm::Datatype::UINT64:
        return create_dim<uint64_t>(name);
      case sm::Datatype::FLOAT32:
        return create_dim<float>(name);
      case sm::Datatype::FLOAT64:
        return create_dim<double>(name);
      case sm::Datatype::DATETIME_YEAR:
      case sm::Datatype::DATETIME_MONTH:
      case sm::Datatype::DATETIME_WEEK:
      case sm::Datatype::DATETIME_DAY:
      case sm::Datatype::DATETIME_HR:
      case sm::Datatype::DATETIME_MIN:
      case sm::Datatype::DATETIME_SEC:
      case sm::Datatype::DATETIME_MS:
      case sm::Datatype::DATETIME_US:
      case sm::Datatype::DATETIME_NS:
      case sm::Datatype::DATETIME_PS:
      case sm::Datatype::DATETIME_FS:
      case sm::Datatype::DATETIME_AS:
      case sm::Datatype::TIME_HR:
      case sm::Datatype::TIME_MIN:
      case sm::Datatype::TIME_SEC:
      case sm::Datatype::TIME_MS:
      case sm::Datatype::TIME_US:
      case sm::Datatype::TIME_NS:
      case sm::Datatype::TIME_PS:
      case sm::Datatype::TIME_FS:
      case sm::Datatype::TIME_AS:
        return create_dim<int64_t>(name);
      default:
        REQUIRE(false);
        return create_dim<int32_t>(name);
    }
  }

  template <class T>
  Attribute create_attr(std::string name) {
    return Attribute::create<int32_t>(ctx_, name);
    ;
  }

  Attribute create_attr(std::string name, std::string type) {
    sm::Datatype t;
    REQUIRE(sm::datatype_enum(type, &t).ok());
    switch (t) {
      case sm::Datatype::INT32:
        return create_attr<int32_t>(name);
      case sm::Datatype::INT64:
        return create_attr<int64_t>(name);
      case sm::Datatype::INT8:
        return create_attr<int8_t>(name);
      case sm::Datatype::UINT8:
        return create_attr<uint8_t>(name);
      case sm::Datatype::INT16:
        return create_attr<int16_t>(name);
      case sm::Datatype::UINT16:
        return create_attr<uint16_t>(name);
      case sm::Datatype::UINT32:
        return create_attr<uint32_t>(name);
      case sm::Datatype::UINT64:
        return create_attr<uint64_t>(name);
      case sm::Datatype::FLOAT32:
        return create_attr<float>(name);
      case sm::Datatype::FLOAT64:
        return create_attr<double>(name);
      case sm::Datatype::DATETIME_YEAR:
      case sm::Datatype::DATETIME_MONTH:
      case sm::Datatype::DATETIME_WEEK:
      case sm::Datatype::DATETIME_DAY:
      case sm::Datatype::DATETIME_HR:
      case sm::Datatype::DATETIME_MIN:
      case sm::Datatype::DATETIME_SEC:
      case sm::Datatype::DATETIME_MS:
      case sm::Datatype::DATETIME_US:
      case sm::Datatype::DATETIME_NS:
      case sm::Datatype::DATETIME_PS:
      case sm::Datatype::DATETIME_FS:
      case sm::Datatype::DATETIME_AS:
      case sm::Datatype::TIME_HR:
      case sm::Datatype::TIME_MIN:
      case sm::Datatype::TIME_SEC:
      case sm::Datatype::TIME_MS:
      case sm::Datatype::TIME_US:
      case sm::Datatype::TIME_NS:
      case sm::Datatype::TIME_PS:
      case sm::Datatype::TIME_FS:
      case sm::Datatype::TIME_AS:
        return create_attr<int64_t>(name);
      default:
        REQUIRE(false);
        return create_attr<int32_t>(name);
    }
  }

  void parse_json(std::string json_blob) {
    json parsed = json::parse(json_blob);

    // Create array schema.
    ArraySchema schema(ctx_, TILEDB_SPARSE);

    // Create domain.
    Domain domain(ctx_);

    // Create dimensions.
    auto dimensions = parsed["dimensions"];
    for (auto& d : dimensions) {
      auto dim = create_dim(d["name"], d["type"]);
      domain.add_dimension(dim);
    }

    // Set the domain on the schema.
    schema.set_domain(domain);

    // Create attributes.
    auto attributes = parsed["attributes"];
    for (auto& a : attributes) {
      auto attr = create_attr(a["name"], a["type"]);
      schema.add_attributes(attr);
    }

    Array::create(parsed["array_name"], schema);
  }

 private:
  Context& ctx_;
};
}  // End of namespace tiledb::test

#endif
