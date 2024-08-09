/**
 * @file   tdb_catch_prng.h
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
 * This file declares a Catch2 hook to seed a global random number generator.
 */

#ifndef TILEDB_TDB_CATCH_PRNG_H
#define TILEDB_TDB_CATCH_PRNG_H

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_get_random_seed.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>

#include "tiledb/common/random/prng.h"
#include "tiledb/common/random/seeder.h"

using namespace tiledb::common;

class PRNGSeederFromCatch : public Catch::EventListenerBase {
 public:
  /**
   * Make visible the base class constructor to default construct class
   * testPRNG using base class initialization.
   */
  using Catch::EventListenerBase::EventListenerBase;

  void testRunStarting(Catch::TestRunInfo const&) override {
    Seeder& seeder_ = Seeder::get();
    seeder_.set_seed(Catch::getSeed());
  }
};

#endif  //  TILEDB_TEST_SUPPORT_PRNG_H
