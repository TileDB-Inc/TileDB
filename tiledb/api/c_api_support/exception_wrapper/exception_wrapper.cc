/**
 * @file tiledb/api/c_api_support/exception_wrapper/exception_wrapper.cc
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
 * This file defines support functions for the exception wrappers.
 */

#include "exception_wrapper.h"

namespace tiledb::api::detail {

//----------------------------------
// ETVisitorStdException
//----------------------------------
static_assert(ErrorTreeVisitor<ETVisitorStdException>);
/**
 * Quote a string appropriate for nested exceptions.
 *
 * Quoted characters:
 * - ":" origin / message separator
 * - "()" nesting indicator
 * - "+" separator for multiples throw from parallel code
 * - "\\" quoting character
 *
 * @section Maturity Notes
 *
 * Not implemented yet. We are not yet guaranteeing that error messages are
 * recognizable by a formal grammar. The test coverage required for such
 * a guarantee is not trivial; it has been deferred.
 */
std::string quoted(std::string s) {
  return s;
}

void ETVisitorStdException::start_level() noexcept {
  try {
    s_ += " (";
  } catch (...) {
  }
}
void ETVisitorStdException::end_level() noexcept {
  try {
    s_ += ")";
  } catch (...) {
  }
}
void ETVisitorStdException::separator() noexcept {
  try {
    s_ += ", ";
  } catch (...) {
  }
}
void ETVisitorStdException::item(const std::exception& e) noexcept {
  try {
    try {
      auto e2{dynamic_cast<const common::StatusException&>(e)};
      // StatusException is already formatted with ": " inside
      s_ += quoted(e.what());
      return;
    } catch (...) {
      // If the cast fails, we'll revert to the base case.
    }
    s_ += "TileDB internal: ";
    s_ += quoted(e.what());
  } catch (...) {
  }
}

}  // namespace tiledb::api::detail
