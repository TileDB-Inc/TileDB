/**
 * @file helpers3.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Some helper macros and variables for testing and debugging.
 *
 */

#ifndef TILEDB_DAG_TEST_HELPERS3_H
#define TILEDB_DAG_TEST_HELPERS3_H

#include <random>
#include <thread>
#include "experimental/tiledb/common/dag/edge/fsm3.h"

using namespace tiledb::common;

/**
 * Some constants for testing / debugging transferring data from source to
 * sink..
 */
const int EMPTY_SOURCE = 1234567;
const int EMPTY_SINK = 7654321;

/**
 * A function to generate a random number between 0 and the specified max
 */
[[maybe_unused]] static size_t random_us(size_t max = 7500) {
  thread_local static uint64_t generator_seed =
      std::hash<std::thread::id>()(std::this_thread::get_id());
  thread_local static std::mt19937_64 generator(generator_seed);
  std::uniform_int_distribution<size_t> distribution(0, max);
  return distribution(generator);
}

/**
 * A series of helper functions for testing the state
 * of the finite-state machine. We work with strings instead
 * of the enum values in order to make the printed output
 * from failed tests more interpretable.
 *
 * The functions are used as
 *
 * CHECK(is_source_empty(state) == "");
 *
 * If the condition passes, an empty string is returned,
 * otherwise, the string that is passed in is returned and
 * the CHECK will print its value in the diagnostic message.
 */
[[maybe_unused]] static std::string is_source_empty(PortState st) {
  if (str(st) == "st_000" || str(st) == "st_001" || str(st) == "st_010" ||
      str(st) == "st_011") {
    return {};
  }
  return str(st);
}

[[maybe_unused]] static std::string is_source_full(PortState st) {
  if (str(st) == "st_100" || str(st) == "st_101" || str(st) == "st_110" ||
      str(st) == "st_111") {
    return {};
  }
  return str(st);
}

[[maybe_unused]] static std::string is_source_post_move(PortState st) {
  if (str(st) != "st_111") {
    return {};
  }
  return str(st);
}

[[maybe_unused]] static std::string is_sink_empty(PortState st) {
  if (str(st) == "st_000" || str(st) == "st_010" || str(st) == "st_100" ||
      str(st) == "st_110") {
    return {};
  }
  return str(st);
}

[[maybe_unused]] static std::string is_sink_full(PortState st) {
  if (str(st) == "st_001" || str(st) == "st_011" || str(st) == "st_101" ||
      str(st) == "st_111") {
  }
  return str(st);
}

[[maybe_unused]] static std::string is_sink_post_move(PortState st) {
  if (str(st) != "st_000") {
    return {};
  }
  return str(st);
}

#endif  // TILEDB_DAG_TEST_HELPERS3_H
