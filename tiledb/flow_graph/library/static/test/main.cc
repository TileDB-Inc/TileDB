/**
 * @file flow_graph/library/static/test/main.cc
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

//---------------------
// Nodes not yet tested elsewhere
//---------------------
/*
 * The bit bucket and the single element generator are test nodes. They're not
 * really used in any tests yet. Their headers are included here to ensure they
 * continue to compile as the code evolves. The tests only check construction.
 */
#include "../../../system/node_services.h"  // for MinimalNodeServices
#include "../bit_bucket.h"
#include "../single_element_generator.h"

using namespace tiledb::flow_graph::library;
using MNS = tiledb::flow_graph::execution::MinimalNodeServices;

TEST_CASE("library bit_bucket, instances") {
  [[maybe_unused]] BitBucketSpecification<void> bbs{};
  [[maybe_unused]] BitBucketInputPortSpecification<void> bbips{};
  [[maybe_unused]] BitBucket<void, MNS> bb{};
}

TEST_CASE("library single monostate generator, instances") {
  [[maybe_unused]] SingleMonostateGeneratorSpecification smgs{};
  [[maybe_unused]] MonostateOutputPortSpecification mops{};
  [[maybe_unused]] SingleMonostateGenerator<MNS> smg{};
}

//----------------------------------
//-------------------------------------------------------
