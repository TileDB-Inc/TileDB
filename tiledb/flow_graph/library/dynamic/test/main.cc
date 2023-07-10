/**
 * @file flow_graph/library/dynamic/test/main.cc
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
 */

#define CATCH_CONFIG_MAIN
#include "tdb_catch.h"

#include "../../platform/minimal_execution_platform.h"
#include "../../static/static_zero_graph.h"
#include "../dynamic_zero_graph.h"
#include "../to_dynamic_reference.h"

using namespace tiledb::flow_graph;
using namespace tiledb::flow_graph::dynamic_specification;

TEST_CASE("Dynamic ZeroGraph, valid specification") {
  STATIC_CHECK(graph<ZeroGraph>);
}

TEST_CASE("ToDynamic, valid specification") {
  using ZG2 = ToDynamicReference<
      static_specification::ZeroGraph,
      MinimalExecutionPlatform>;
  STATIC_CHECK(graph<ZG2>);
}