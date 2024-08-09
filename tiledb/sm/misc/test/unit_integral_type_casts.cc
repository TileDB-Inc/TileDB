/**
 * @file unit_integral_type_casts.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB Inc.
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
 * Tests the C++ API for enumeration related functions.
 */

#include <iostream>
#include <random>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "tiledb/sm/misc/integral_type_casts.h"

// These tests are for the safe_integral_cast and safe_itegral_cast_to_datatype
// functions found in `tiledb/sm/misc/integral_type_casts.h`.
//
// There are two main tests in this file, one for safe_integral_cast and one
// for safe_integral_cast_to_datatype. These tests work by generating a
// comprehensive sample of test data to exhaustively check that casts between
// all supported integral datatypes are covered. Generating tests in this
// manner allows us to assert whether a cast should succeed vs one that should
// fail.
//
// Both tests have the same general outline. First, pass a callback to the
// run_test function which is responsible for generating a TestCase value
// which is nothing more than a type width (i.e., sizeof(int32_t)) and a
// set of random bytes in length 1 to byte_width. This ensures that we're
// able to test casts that should succeed (i.e., 5 stored in an int32_t is
// castable to a uint8_t) and casts that should fail (i.e., 300 stored in an
// int32_t is not castable to either int8_t or uint8_t).
//
// The callback passed to `run_test` is then responsible for enumerating all
// of the test types for cast checks. Both tests have the same naming pattern
// of:
//
//  1. check_$function_name - Based on TestCase.byte_width, pick the source
//       datatype for the test
//  2. dispatch_$function_name - Attempt to convert the source test data to all
//       target types.
//  3. run_$functon_name - Attempt the cast for the given TestData, source, and
//       target types. These use the should_cast_succeed helper to know whether
//       a given cast should succeed or fail.
//
// Beyond the main test layouts, there are two important helper functions
// that will be useful for understanding.
//
// First, `should_cast_succeed` which takes the source type and value, and a
// target type and returns a boolean on whether a cast should succeed. This
// is the crux of these tests as this logic is the concise list of rules that
// dictate when a cast is safe or not.
//
// The second function is `calculate_width` which is called by
// `should_cast_succeed`. This calculates the number of bits and bytes required
// to hold a given value and informs `should_cast_succeed` when a cast is
// representable in a target type. If the code in this function looks a little
// weird, you'll want to review 2's complement representation on why it counts
// things as it does.

using namespace tiledb::sm;

/* ********************************* */
/*            CONSTANTS              */
/* ********************************* */

// Widths of integral types int8_t to int64_t
constexpr int TYPE_WIDTHS[] = {1, 2, 4, 8};
constexpr int NUM_TYPE_WIDTHS = 4;

// Width of widest type (i.e., sizeof(int64_t))
constexpr int MAX_WIDTH = 8;

// An aribtrary number of iterations to run per test
constexpr int RANDOM_ITERATIONS = 500;

// A POD struct representing a test case
struct TestCase {
  // The width of the type, i.e., sizeof(int8_t), sizeof(uint32_t), etc, this
  // is obviously restricted to the values 1, 2, 4, 8.
  uint8_t type_width;

  // Storage for the generated value's data. LSB is data[0]
  uint8_t data[MAX_WIDTH];
};

/* ********************************* */
/*          TEST CALLBACKS           */
/* ********************************* */

// Test implementation for util::safe_integral_cast
void check_safe_integral_cast(const TestCase& tc);

// Test implementation for util::safe_integral_cast_to_datatype
void check_safe_integral_cast_to_datatype(const TestCase& tc);

/* ********************************* */
/*       TEST HELPER FUNCTIONS       */
/* ********************************* */

// Test driver function
template <typename TestFunc>
void run_test(TestFunc func);

// Apply the test function against each source type based on tc.type_width
template <typename TestFunc>
void dispatch(TestFunc func, const TestCase& tc);

// Return a boolean indicating whether the given cast should succeed or fail
template <typename Source, typename Target>
bool should_cast_succeed(Source src);

template <typename Source>
bool should_cast_succeed(Source src, Datatype dtype);

// Calculate the width of a value in both bits and bytes.
template <typename Type>
std::tuple<uint8_t, uint8_t> calculate_width(Type val);

// Fills in a TestCase's data, byte_width, and msb_set members
void generate_case(TestCase& tc, uint8_t gen_byte_width, bool set_msb);

// Convert a uint8_t[] to integral type T
template <typename T>
T buf_to_value(const uint8_t* buffer);

/* ********************************* */
/*        safe_integral_cast         */
/* ********************************* */

TEST_CASE("util::safe_integral_cast", "[safe-integral-casts]") {
  run_test(check_safe_integral_cast);
}

template <typename Source>
void dispatch_safe_integral_cast(const TestCase& tc);

template <typename Source, typename Target>
void run_safe_integral_cast(const TestCase&, Source src);

void check_safe_integral_cast(const TestCase& tc) {
  switch (tc.type_width) {
    case 1:
      dispatch_safe_integral_cast<int8_t>(tc);
      dispatch_safe_integral_cast<uint8_t>(tc);
      return;
    case 2:
      dispatch_safe_integral_cast<int16_t>(tc);
      dispatch_safe_integral_cast<uint16_t>(tc);
      return;
    case 4:
      dispatch_safe_integral_cast<int32_t>(tc);
      dispatch_safe_integral_cast<uint32_t>(tc);
      return;
    case 8:
      dispatch_safe_integral_cast<int64_t>(tc);
      dispatch_safe_integral_cast<uint64_t>(tc);
      return;
    default:
      throw std::logic_error("Invalid type_width");
  }
}

template <typename Source>
void dispatch_safe_integral_cast(const TestCase& tc) {
  Source val = buf_to_value<Source>(tc.data);

  run_safe_integral_cast<Source, int8_t>(tc, val);
  run_safe_integral_cast<Source, uint8_t>(tc, val);

  run_safe_integral_cast<Source, int16_t>(tc, val);
  run_safe_integral_cast<Source, uint16_t>(tc, val);

  run_safe_integral_cast<Source, int32_t>(tc, val);
  run_safe_integral_cast<Source, uint32_t>(tc, val);

  run_safe_integral_cast<Source, int64_t>(tc, val);
  run_safe_integral_cast<Source, uint64_t>(tc, val);
}

template <typename Source, typename Target>
void run_safe_integral_cast(const TestCase&, Source src) {
  if (should_cast_succeed<Source, Target>(src)) {
    auto tgt = utils::safe_integral_cast<Source, Target>(src);
    REQUIRE(src == utils::safe_integral_cast<Target, Source>(tgt));
  } else {
    REQUIRE_THROWS(utils::safe_integral_cast<Source, Target>(src));
  }
}

/* ********************************* */
/*  safe_integral_cast_to_datatype   */
/* ********************************* */

TEST_CASE("util::safe_integral_cast_to_datatype", "[safe-integral-casts]") {
  run_test(check_safe_integral_cast_to_datatype);
}

TEST_CASE(
    "util::safe_integral_cast_to_datatype bad type",
    "[safe-integral-casts][error]") {
  auto datatype = GENERATE(
      Datatype::BLOB,
      Datatype::GEOM_WKB,
      Datatype::GEOM_WKT,
      Datatype::STRING_ASCII);
  ByteVecValue bvv;
  REQUIRE_THROWS(utils::safe_integral_cast_to_datatype(5, datatype, bvv));
}

template <typename Source>
void dispatch_safe_integral_cast_to_datatype(const TestCase& tc);

template <typename Source>
void run_safe_integral_cast_to_datatype(
    const TestCase&, Source src, Datatype dt);

void check_safe_integral_cast_to_datatype(const TestCase& tc) {
  switch (tc.type_width) {
    case 1:
      dispatch_safe_integral_cast_to_datatype<int8_t>(tc);
      dispatch_safe_integral_cast_to_datatype<uint8_t>(tc);
      return;
    case 2:
      dispatch_safe_integral_cast_to_datatype<int16_t>(tc);
      dispatch_safe_integral_cast_to_datatype<uint16_t>(tc);
      return;
    case 4:
      dispatch_safe_integral_cast_to_datatype<int32_t>(tc);
      dispatch_safe_integral_cast_to_datatype<uint32_t>(tc);
      return;
    case 8:
      dispatch_safe_integral_cast_to_datatype<int64_t>(tc);
      dispatch_safe_integral_cast_to_datatype<uint64_t>(tc);
      return;
    default:
      throw std::logic_error("Invalid type_width");
  }
}

template <typename Source>
void dispatch_safe_integral_cast_to_datatype(const TestCase& tc) {
  Source val = buf_to_value<Source>(tc.data);

  std::vector<Datatype> datatypes = {
      Datatype::BOOL,
      Datatype::INT8,
      Datatype::UINT8,
      Datatype::INT16,
      Datatype::UINT16,
      Datatype::INT32,
      Datatype::UINT32,
      Datatype::INT64,
      Datatype::UINT64};

  for (auto dt : datatypes) {
    run_safe_integral_cast_to_datatype<Source>(tc, val, dt);
  }
}

template <typename Source>
void run_safe_integral_cast_to_datatype(
    const TestCase&, Source src, Datatype dtype) {
  ByteVecValue dest;
  if (should_cast_succeed<Source>(src, dtype)) {
    utils::safe_integral_cast_to_datatype<Source>(src, dtype, dest);
    switch (dtype) {
      case Datatype::BOOL:
        REQUIRE(dest.rvalue_as<uint8_t>() == (uint8_t)src);
        return;
      case Datatype::INT8:
        REQUIRE(dest.rvalue_as<int8_t>() == (int8_t)src);
        return;
      case Datatype::UINT8:
        REQUIRE(dest.rvalue_as<uint8_t>() == (uint8_t)src);
        return;
      case Datatype::INT16:
        REQUIRE(dest.rvalue_as<int16_t>() == (int16_t)src);
        return;
      case Datatype::UINT16:
        REQUIRE(dest.rvalue_as<uint16_t>() == (uint16_t)src);
        return;
      case Datatype::INT32:
        REQUIRE(dest.rvalue_as<int32_t>() == (int32_t)src);
        return;
      case Datatype::UINT32:
        REQUIRE(dest.rvalue_as<uint32_t>() == (uint32_t)src);
        return;
      case Datatype::INT64:
        REQUIRE(dest.rvalue_as<int64_t>() == (int64_t)src);
        return;
      case Datatype::UINT64:
        REQUIRE(dest.rvalue_as<uint64_t>() == (uint64_t)src);
        return;
      default:
        throw std::logic_error("Invalid datatype for test");
    }
  } else {
    REQUIRE_THROWS(
        utils::safe_integral_cast_to_datatype<Source>(src, dtype, dest));
  }
}

/* ********************************* */
/*    TEST HELPER IMPLEMENTATIONS    */
/* ********************************* */

template <typename TestFunc>
void run_test(TestFunc tfunc) {
  TestCase tc;
  for (uint8_t i = 0; i < NUM_TYPE_WIDTHS; i++) {
    tc.type_width = TYPE_WIDTHS[i];

    // Always check no bits set edge case
    memset(tc.data, 0, MAX_WIDTH);
    tfunc(tc);

    for (uint8_t gen_width = 1; gen_width <= tc.type_width; gen_width++) {
      for (int i = 0; i < RANDOM_ITERATIONS; i++) {
        generate_case(tc, gen_width, false);
        tfunc(tc);

        generate_case(tc, gen_width, true);
        tfunc(tc);
      }
    }

    // Always check all bits set edge case
    memset(tc.data, 255, MAX_WIDTH);
    tfunc(tc);
  }
}

template <typename Source, typename Target>
bool should_cast_succeed(Source src) {
  auto [bit_width, byte_width] = calculate_width(src);
  bool tgt_signed = std::numeric_limits<Target>::is_signed;
  uint8_t tgt_size = sizeof(Target);

  // Obviously, if we have more bytes in src than the Target type can hold,
  // the cast will fail.
  if (byte_width > tgt_size) {
    return false;
  }

  // Unsigned types can't hold negative values, so if src is negative
  // and Target is not signed, we must fail.
  if (src < 0 && !tgt_signed) {
    return false;
  }

  // When converting from a positive value to a signed type we have to make
  // sure that the signed type has more than bit_width + 1 bits available so
  // that the resulting value doesn't become negative.
  if ((src > 0 && tgt_signed) && (bit_width >= (tgt_size * 8))) {
    return false;
  }

  // When converting to a negative value to a signed type, we have to make
  // sure the target has enough bits to represent the value.
  if ((src < 0 && tgt_signed) && (bit_width >= (tgt_size * 8))) {
    return false;
  }

  // Otherwise, the cast should succeed
  return true;
}

template <typename Source>
bool should_cast_succeed(Source src, Datatype dtype) {
  switch (dtype) {
    case Datatype::BOOL:
      return should_cast_succeed<Source, uint8_t>(src);
    case Datatype::INT8:
      return should_cast_succeed<Source, int8_t>(src);
    case Datatype::UINT8:
      return should_cast_succeed<Source, uint8_t>(src);
    case Datatype::INT16:
      return should_cast_succeed<Source, int16_t>(src);
    case Datatype::UINT16:
      return should_cast_succeed<Source, uint16_t>(src);
    case Datatype::INT32:
      return should_cast_succeed<Source, int32_t>(src);
    case Datatype::UINT32:
      return should_cast_succeed<Source, uint32_t>(src);
    case Datatype::INT64:
      return should_cast_succeed<Source, int64_t>(src);
    case Datatype::UINT64:
      return should_cast_succeed<Source, uint64_t>(src);
    default:
      throw std::logic_error("Invalid datatype for test");
  }
}

template <typename Type>
std::tuple<uint8_t, uint8_t> calculate_width(Type val) {
  // Calculate the number of bits required to hold src. For positive values,
  // this is the largest numbered bit set to 1, for negative values its the
  // largest numbered bit set to 0.

  uint8_t bit_width = 0;
  if (val > 0) {
    // For positive values, we find the highest set bit by counting the number
    // of right shifts required before the value is zero.
    uint8_t i = 0;
    while (val > 0) {
      val >>= 1;
      i += 1;
    }
    bit_width = i;
  } else if (val < 0) {
    // For negative values, we find the highest unset bit by counting the
    // number of left shifts before the value becomes greater than or equal
    // to zero.
    uint8_t i = 0;
    while (val < 0) {
      val <<= 1;
      i += 1;
    }
    bit_width = sizeof(Type) * 8 - i;
  }

  uint8_t byte_width = bit_width / 8;
  if (bit_width % 8 != 0) {
    byte_width += 1;
  }

  return {bit_width, byte_width};
}

// A simple function for generating random uint64_t values
uint64_t generate_value() {
  static std::random_device rand_dev;
  static std::mt19937_64 generator(rand_dev());
  static std::uniform_int_distribution<unsigned long long> dist;
  return dist(generator);
}

// Generate a TestCase with the given gen_width number of random bytes. The
// only curisoity is the set_msb value which forces the most significant bit
// of the most significant byte to 0 or 1. Forcing the msb to 1 allows us to
// assert that we're testing two important cases. First, casting negative
// values between different types widths, and second, checking that large
// unsigned values of a given type that aren't representable in the signed
// type of the same byte width.
void generate_case(TestCase& tc, uint8_t gen_width, bool set_msb) {
  // Reset TestCase state
  memset(tc.data, 0, MAX_WIDTH);

  // Convince GCC that we'll not write beyond MAX_WIDTH bytes into tc.data
  if (gen_width > MAX_WIDTH) {
    throw std::logic_error("Invalid gen_width in test case");
  }

  // A standard bit shift approach for converting integral values into
  // an array of uint8_t values. Its important to note that the << and >>
  // operators are endian-ness agnostic. So this just masks everything but
  // the least-significant-byte, converts that to a uint8_t and stores it
  // in the array. Then drop the least-significant byte of the random value
  // by shifting 8 bits to the right.
  //
  // TestCase.byte_width is calculated here since its perfectly valid for
  // a generated value to not have any bits set in any given byte which can
  // cause expected failures to unexpected succeed if its the MSB that is
  // all zeroes.
  uint64_t rand_value = generate_value();
  for (int i = 0; i < gen_width; i++) {
    tc.data[i] = static_cast<uint8_t>(rand_value & 0xFF);
    rand_value >>= 8;
  }

  // If set_msb is false, set the gen_width's byte msb to 0, else set to 1
  if (!set_msb) {
    tc.data[gen_width - 1] &= 0x7F;
  } else {
    tc.data[gen_width - 1] |= 0x80;
  }
}

// Convert a uint8_t buffer to the given type T. This works using the standard
// bit twiddling approach to progressive bitwise-or one byte at a time in a
// most-to-least significant byte order.
template <typename T>
T buf_to_value(const uint8_t* buffer) {
  T ret = 0;
  for (int8_t i = sizeof(T) - 1; i >= 0; i--) {
    T new_byte = (T)buffer[i];
    ret = (ret << 8) | new_byte;
  }
  return ret;
}
