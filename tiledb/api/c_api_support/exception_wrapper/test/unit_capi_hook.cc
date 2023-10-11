/**
 * @file c_api_support/exception_wrapper/test/unit_capi_hook.cc
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

#include <test/support/tdb_catch.h>

#include "../exception_wrapper.h"

using namespace tiledb::api;
using namespace tiledb::api::detail;

//-------------------------------------------------------
// Hook
//-------------------------------------------------------

class LABase {
 protected:
  static std::string msg;

 public:
  static void reset() {
    msg = "";
  }
  static std::string message() {
    return msg;
  }
};

std::string LABase::msg;

template <auto f>
class LoggingAspect : public LABase {
 public:
  template <typename... Args>
  static void apply(Args...) {
    msg += "something: " + std::string(typeid(f).name());
  }
};
capi_return_t tf_null() {
  return TILEDB_OK;
}
using null_wrapped_for_logging = tiledb::api::
    CAPIFunction<tf_null, tiledb::api::ExceptionAction, LoggingAspect<tf_null>>;

TEST_CASE("Hook 0", "[hook]") {
  LABase::reset();
  CHECK(LABase::message() == "");
  tiledb::api::ExceptionAction h;
  null_wrapped_for_logging().function(h);
  // CHECK(LABase::message()== "something");
}

//------------------------------------------------------
// C API definition functions
//-------------------------------------------------------
