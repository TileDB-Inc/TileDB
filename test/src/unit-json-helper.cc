/**
 * @file   unit-json-helper.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests the Json helper.
 */

#include "test/support/src/json_helpers.h"

TEST_CASE("JSON", "[jsonc]") {
  // clang-format off
  std::string v =
    R"({
      "array_name": "temp",
      "dimensions":[
        {
          "name": "d1",
          "type": "INT32",
          "domain": "[1, 4]",
          "tile extent": 2
        },
        {
          "name": "d2",
          "type": "INT64",
          "domain": "[1, 4]",
          "tile extent": 4
        }
      ],
      "attributes":[
        {
          "name": "a1",
          "type": "INT32"
        },
        {
          "name": "a2",
          "type": "INT64"
        }
      ],
      "fragments":[
        {
          "timestamps": 1
          "d1" : [1, 1]
          "d2" : [1, 2]
          "a1" : [1, 4]
          "a2" : [2, 3]
        }
      ]
    })";
  // clang-format on

  tiledb::Context ctx;
  tiledb::test::JsonTestParser parser(ctx);
  parser.parse_json(v);
}