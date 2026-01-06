/**
 * @file   unit_intercept.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file contains unit tests and simple examples for the `INTERCEPT`
 * capability.
 */

#include "tiledb/common/util/intercept.h"

#include <test/support/tdb_catch.h>

#include <barrier>
#include <thread>

// global variable for demonstrating intercept perfect forwarding
int global = 0;

namespace intercept {

DECLARE_INTERCEPT(my_library_function_entry);
DEFINE_INTERCEPT(my_library_function_entry);

// NB: DECLARE_INTERCEPT is not strictly necessary if everything is in the same
// file. The DECLARE also need not be in a header, the linker will resolve it
// correctly if the DECLARE is written in the unit test file and the DEFINE
// in the source file.
DEFINE_INTERCEPT(my_library_function_exit, int, std::string_view, int);

}  // namespace intercept

/**
 * Silly library function for demonstrating intercepts.
 */
int my_library_function(std::string_view arg) {
  INTERCEPT(intercept::my_library_function_entry);

  const int local = global++;
  INTERCEPT(intercept::my_library_function_exit, global, arg, local);

  return local;
}

/**
 * Demonstrates using intercepts to log aspects of an execution
 * which we might want to make assertions about in a test.
 */
TEST_CASE("Intercept log", "[intercept]") {
  std::map<std::string, std::vector<std::pair<int, int>>> values;

  {
    auto cb = intercept::my_library_function_exit().and_also(
        [&values](int snapshot_global, std::string_view arg, int local) {
          values[std::string(arg)].emplace_back(
              std::make_pair(snapshot_global, local));
        });

    global = 0;
    my_library_function("foo");
    my_library_function("bar");
    my_library_function("foo");

    CHECK(values.size() == 2);
    CHECK(values["foo"] == std::vector<std::pair<int, int>>{{1, 0}, {3, 2}});
    CHECK(values["bar"] == std::vector<std::pair<int, int>>{{2, 1}});
  }

  // now that the callback is de-registered we shouldn't see anything
  const decltype(values) snapshot = values;
  my_library_function("bar");
  CHECK(values == snapshot);
}

/**
 * Demonstrates using intercepts to simulate errors occurring
 * within a library function. This can be very useful if it is known
 * that the throwing of the error is causing problems, but the error
 * itself is difficult to reproduce.
 */
TEST_CASE("Intercept simulate error", "[intercept]") {
  // nothing happens
  my_library_function("foo");

  // we can register a callback to make it throw
  {
    auto cb = intercept::my_library_function_entry().and_also(
        []() { throw std::logic_error("intercept"); });
    CHECK_THROWS(
        my_library_function("foo"),
        Catch::Matchers::ContainsSubstring("intercept"));
  }

  // now the callback is de-registered, it should not throw again
  my_library_function("foo");
}

/**
 * Demonstrates using intercepts to synchronize multiple threads,
 * producing a deterministic behavior.
 */
TEST_CASE("Intercept synchronize", "[intercept]") {
  global = 0;

  std::barrier sync(2);

  auto cb = intercept::my_library_function_exit().and_also(
      [&sync](int snapshot_global, std::string_view, int) {
        if (snapshot_global == 2) {
          // waits for the main thread
          sync.arrive_and_wait();
          // the main thread has arrived; wait for its signal to resume
          sync.arrive_and_wait();
        }
      });

  std::vector<int> tt_values;

  std::thread tt([&tt_values]() {
    tt_values.push_back(my_library_function("foo"));
    tt_values.push_back(my_library_function("bar"));
    tt_values.push_back(my_library_function("baz"));
    tt_values.push_back(my_library_function("gub"));
  });

  sync.arrive_and_wait();

  // the thread is waiting for a signal to continue,
  // we can run arbitrary code while it does so
  global = 100;

  sync.arrive_and_wait();

  tt.join();

  // because we synchronized the two threads we should always
  // see exactly the same values
  CHECK(tt_values == std::vector<int>{0, 1, 100, 101});
}
