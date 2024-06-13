/**
 * @file tiledb/api/c_api_support/exception_wrapper/test/unit_capi_hook.cc
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

#include "../../c_api_support.h"
#include "hook_common.h"
#include "logging_aspect.h"

using namespace tiledb::api;
using namespace tiledb::api::detail;

// from `logging_aspect.h`
std::string LABase::msg_{};
bool LABase::touched_{false};

template <auto f>
using selector_type = CAPIFunctionSelector<f, void>;

/**
 * API function that does nothing.
 */
capi_return_t tf_null() {
  return TILEDB_OK;
}
/**
 * Metadata for null function.
 */
template <>
struct CAPIFunctionNameTrait<tf_null> {
  static constexpr std::string_view name{"tf_null"};
};

namespace tiledb::api {
/**
 * API implementation function that does nothing.
 */
capi_return_t tiledb_capi_nil(int) {
  return 0;
}
}  // namespace tiledb::api
/*
 * API definitions with macros.
 */
CAPI_INTERFACE(capi_nil, int x) {
  return tiledb::api::api_entry_plain<tiledb::api::tiledb_capi_nil>(x);
}

/**
 * The wrapped function with an unconditional invocation of the logging
 * aspect as an explicit template argument.
 */
using null_always_wrapped_for_logging = tiledb::api::
    CAPIFunction<tf_null, tiledb::api::ExceptionAction, LoggingAspect<tf_null>>;
/**
 * The wrapped function conditional upon an override as to whether the
 * logging aspect is compiled in or not. The aspect argument is omitted, the
 * default applies, and overriding is possible.
 */
using null_maybe_wrapped_for_logging =
    tiledb::api::CAPIFunction<tf_null, tiledb::api::ExceptionAction>;

TEST_CASE("Compile consistency") {
  /*
   * In all cases verify that the default aspect is being used if and only if
   * the hook is not enabled.
   */
  CHECK(
      compiled_with_hook != std::same_as<
                                selector_type<tf_null>::aspect_type,
                                CAPIFunctionNullAspect<tf_null>>);
  if constexpr (compiled_with_hook) {
    /*
     * In the case "with hook", check that the aspect is what the test defines.
     */
    CHECK(std::same_as<
          selector_type<tf_null>::aspect_type,
          LoggingAspect<tf_null>>);
  }
}

TEST_CASE("Hook unconditional") {
  LABase::reset();
  CHECK(LABase::touched() == false);
  CHECK(LABase::message() == "");
  tiledb::api::ExceptionAction h;
  null_always_wrapped_for_logging().function(h);
  CHECK(LABase::touched() == true);
  CHECK(LABase::message() == "tf_null");
}

/*
 * Test that the hook is invoked if and only if it's compiled in. This is the
 * same as the previous test but for the last line.
 */
TEST_CASE("Hook conditional for touch") {
  LABase::reset();
  CHECK(LABase::touched() == false);
  tiledb::api::ExceptionAction h;
  null_maybe_wrapped_for_logging().function(h);
  CHECK(LABase::touched() == compiled_with_hook);
}

TEST_CASE("Hook conditional with text 1") {
  LABase::reset();
  CHECK(LABase::message() == "");
  tiledb::api::ExceptionAction h;
  null_maybe_wrapped_for_logging().function(h);
  if constexpr (compiled_with_hook) {
    CHECK(LABase::message() == "tf_null");
  } else {
    CHECK(LABase::message() == "");
  }
}

TEST_CASE("Hook conditional with text 2") {
  LABase::reset();
  CHECK(LABase::message() == "");
  tiledb::api::ExceptionAction h;
  ::tiledb_capi_nil(0);
  if constexpr (compiled_with_hook) {
    CHECK(LABase::message() == "tiledb_capi_nil");
  } else {
    CHECK(LABase::message() == "");
  }
}
