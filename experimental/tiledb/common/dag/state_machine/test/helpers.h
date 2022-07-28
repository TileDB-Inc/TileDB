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
#include "experimental/tiledb/common/dag/state_machine/fsm.h"

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
template <class State>
[[maybe_unused]] std::string is_source_empty(State st);

template <>
[[maybe_unused]] std::string is_source_empty(
    decltype(PortState<three_stage>::st_000) st) {
  std::string state = str(st);
  if (state == "st_000" || state == "st_001" || state == "st_010" ||
      state == "st_011") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_source_full(
    decltype(PortState<three_stage>::st_000) st) {
  std::string state = str(st);
  if (state == "st_100" || state == "st_101" || state == "st_110" ||
      state == "st_111") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_source_post_move(
    decltype(PortState<three_stage>::st_000) st) {
  std::string state = str(st);
  if (state != "st_111") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_sink_empty(
    decltype(PortState<three_stage>::st_000) st) {
  std::string state = str(st);
  if (state == "st_000" || state == "st_010" || state == "st_100" ||
      state == "st_110") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_sink_full(
    decltype(PortState<three_stage>::st_000) st) {
  std::string state = str(st);
  if (state == "st_001" || state == "st_011" || state == "st_101" ||
      state == "st_111") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_sink_post_move(
    decltype(PortState<three_stage>::st_000) st) {
  std::string state = str(st);
  if (state != "st_000") {
    return {};
  }
  return state;
}

/**
 *
 */

template <>
[[maybe_unused]] std::string is_source_empty(
    decltype(PortState<two_stage>::st_00) st) {
  std::string state = str(st);
  if (state == "st_00" || state == "st_01") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_source_full(
    decltype(PortState<two_stage>::st_00) st) {
  std::string state = str(st);
  if (state == "st_10" || state == "st_11") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_source_post_move(
    decltype(PortState<two_stage>::st_00) st) {
  std::string state = str(st);
  if (state != "st_11") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_sink_empty(
    decltype(PortState<two_stage>::st_00) st) {
  std::string state = str(st);
  if (state == "st_00" || state == "st_10") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_sink_full(
    decltype(PortState<two_stage>::st_00) st) {
  std::string state = str(st);
  if (state == "st_01" || state == "st_11") {
    return {};
  }
  return state;
}

[[maybe_unused]] static std::string is_sink_post_move(
    decltype(PortState<two_stage>::st_00) st) {
  std::string state = str(st);
  if (state != "st_00") {
    return {};
  }
  return state;
}

#endif  // TILEDB_DAG_TEST_HELPERS3_H
