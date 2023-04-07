// clang-format off
/**
 * @file   absl_lilnk_test.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests for linkage to libraries of the targets found to be referenced by
 * gcssdk v1.22 at the time of creation of absl_link_test.cc.
 *
 * Those targets were crudely identified by extracting from the following action:
 * ep_gcssdk\google>findstr /s absl:: *cmake*
cloud\bigquery\CMakeLists.txt:    PUBLIC absl::memory absl::strings google_cloud_cpp_grpc_utils
cloud\bigtable\CMakeLists.txt:    PUBLIC absl::memory
cloud\bigtable\CMakeLists.txt:        PUBLIC absl::memory
cloud\bigtable\CMakeLists.txt:            PRIVATE absl::memory
cloud\bigtable\examples\CMakeLists.txt:                    absl::time
cloud\bigtable\examples\CMakeLists.txt:                    absl::time
cloud\CMakeLists.txt:    google_cloud_cpp_common PUBLIC absl::flat_hash_map absl::memory
cloud\CMakeLists.txt:                                   absl::optional absl::time Threads::Threads)
cloud\CMakeLists.txt:                    absl::variant GTest::gmock_main GTest::gmock GTest::gtest)
cloud\CMakeLists.txt:# runtime deps of the absl::optional target (http://github.com/abseil/abseil-cpp
cloud\CMakeLists.txt:        PUBLIC absl::function_ref
cloud\CMakeLists.txt:               absl::memory
cloud\CMakeLists.txt:               absl::time
cloud\CMakeLists.txt:                        absl::variant
cloud\pubsub\benchmarks\CMakeLists.txt:                    absl::str_format
cloud\pubsub\CMakeLists.txt:                         googleapis-c++::pubsub_protos absl::flat_hash_map)
cloud\pubsub\CMakeLists.txt:                    absl::str_format
cloud\spanner\CMakeLists.txt:    PUBLIC absl::fixed_array
cloud\spanner\CMakeLists.txt:           absl::memory
cloud\spanner\CMakeLists.txt:           absl::numeric
cloud\spanner\CMakeLists.txt:           absl::strings
cloud\spanner\CMakeLists.txt:           absl::time
cloud\spanner\CMakeLists.txt:                    absl::memory
cloud\spanner\CMakeLists.txt:                    absl::numeric
cloud\storage\benchmarks\CMakeLists.txt:                    absl::strings
cloud\storage\benchmarks\CMakeLists.txt:                        absl::strings
cloud\storage\CMakeLists.txt:    PUBLIC absl::memory
cloud\storage\CMakeLists.txt:           absl::strings
cloud\storage\CMakeLists.txt:           absl::str_format
cloud\storage\CMakeLists.txt:           absl::time
cloud\storage\CMakeLists.txt:           absl::variant
cloud\storage\CMakeLists.txt:               absl::strings
cloud\storage\CMakeLists.txt:        PUBLIC absl::memory
cloud\storage\CMakeLists.txt:            PRIVATE absl::memory
cloud\storage\tests\CMakeLists.txt:                absl::strings
cloud\testing_util\CMakeLists.txt:        PUBLIC absl::symbolize absl::failure_signal_handler
 */
// clang-format on

#include <stdio.h>
#include <cstdint>
#include <cstdlib>
#include <sstream>

#include <absl/base/casts.h>             // absl::function_ref
#include <absl/container/fixed_array.h>  // absl::fixed_array
#include <absl/hash/hash.h>              // absl::flat_hash_map
#include <absl/memory/memory.h>
#include <absl/meta/type_traits.h>  // absl::function_ref
#include <absl/numeric/int128.h>    // absl::numeric
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>  // absl::str_format
#include <absl/strings/string_view.h>
#include <absl/time/clock.h>
#include <absl/types/optional.h>  // absl::optional
#include <absl/types/variant.h>   // absl::variant
#if 0
// absl::symbolize, absl::failure_signal_handler both in testing_util, not
// verifying these as we do not build gcs tests.
#include <absl/symbolize/
#include <absl/failure_signal_handler/
#endif

int main() {
  {
      // absl::memory target/library seem header only, nothing to attempt to
      // link to.
  } {  // strings
    absl::string_view view_float("5.927");
    float f;
    (void)absl::SimpleAtof(view_float, &f);
    printf("%f\n", f);
  }
  {  // time
    absl::Time n, b, a;
    b = absl::FromUnixNanos(absl::GetCurrentTimeNanos());
    n = absl::Now();
    a = absl::FromUnixNanos(absl::GetCurrentTimeNanos());
    if (b > a) {
      printf(
          "Unexpected b < a... (%s) < a (%s)\n",
          absl::FormatTime(b).c_str(),
          absl::FormatTime(a).c_str());
    }
  }
  {  // hash.h
    bool is_hashable = std::is_default_constructible<absl::Hash<int>>();
    printf("is_hashable<int>(), %d\n", is_hashable);
  }
  {  // optional
    absl::optional<int> empty;
    if (!empty) {
      printf("empty !empty?\n");
    }
    if (empty.has_value()) {
      printf("empty unexpectedly has value!\n");
    }
  }
  {  // variant
    absl::variant<uint64_t> x;
    if (x.index()) {
      printf("x.index() unexpectedly != zero\n");
    }
  }
  {  // str_format
    auto fmtd = absl::StrFormat("%d", 123);
    if (fmtd != std::string("123")) {
      printf("must not work like I guessed, \"%s\" != \"123\"\n", fmtd.c_str());
    }
  }
  {  // fixed_array
    absl::FixedArray<int, 10> fa(5);
    *fa.begin() = atoi("99753");
    printf("fa.begin() %d, fa[0] %d\n", *fa.begin(), fa[0]);
  }
  {  // numeric
    absl::uint128 u128{1299.3};
    std::stringstream sstr;
    sstr << "u128 " << u128;
    printf("%s\n", sstr.str().c_str());
  }

  return 0;
}
