/**
 * @file c_api_support/exception_wrapper/test/unit_capi_exception_wrapper.cc
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
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>

using namespace tiledb::api;

/*
 * Ensure we're using the stub
 */
#include "tiledb/sm/storage_manager/storage_manager.h"
static_assert(tiledb::sm::StorageManager::is_overriding_class);

#include "../exception_wrapper.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_context.h"

using tiledb::api::test_support::ordinary_context;

capi_return_t tf_always_good() {
  return TILEDB_OK;
}
using always_good_wrapped = tiledb::api::
    CAPIFunction<tf_always_good, tiledb::api::ExceptionActionCtxErr>;

capi_return_t tf_always_throw() {
  throw StatusException("Test", "error");
}
using always_throw_wrapped = tiledb::api::
    CAPIFunction<tf_always_throw, tiledb::api::ExceptionActionCtxErr>;

TEST_CASE("ExceptionAction - construct") {
  tiledb::api::ExceptionAction h{};
  CHECK_NOTHROW(h.validate());
}

TEST_CASE("ExceptionAction - action") {
  tiledb::api::ExceptionAction h{};
  /*
   * The action sends a message to the log. We're not going to check that, just
   * that it returns.
   */
  CHECK_NOTHROW(h.action(std::runtime_error("an error message")));
}

TEST_CASE("ExceptionActionCtx - construct") {
  auto ctx{make_handle<tiledb_ctx_handle_t>(tiledb::sm::Config{})};
  tiledb::api::ExceptionActionCtx h{ctx};
  CHECK_NOTHROW(h.validate());
}

TEST_CASE("ExceptionActionCtx - construct invalid") {
  tiledb::api::ExceptionActionCtx h{nullptr};
  CHECK_THROWS(h.validate());
}

TEST_CASE("ExceptionActionCtx - action") {
  auto ctx{make_handle<tiledb_ctx_handle_t>(tiledb::sm::Config{})};
  tiledb::api::ExceptionActionCtx h{ctx};
  auto x{ctx->context().last_error()};
  CHECK(!x.has_value());
  h.action(std::runtime_error("an error message"));
  auto y{ctx->context().last_error()};
  REQUIRE(y.has_value());
  CHECK(y.value() == "TileDB internal: an error message");
}

TEST_CASE("ExceptionActionErr - construct") {
  tiledb_error_handle_t* error{nullptr};
  tiledb::api::ExceptionActionErr h{&error};
  CHECK_NOTHROW(h.validate());
}

TEST_CASE("ExceptionActionErr - construct invalid") {
  tiledb::api::ExceptionActionErr h{nullptr};
  CHECK_THROWS(h.validate());
}

/*
 * Check that the error handle nulls out the pointer on success.
 */
TEST_CASE("ExceptionActionErr - action on success") {
  auto errorx{make_handle<tiledb_error_handle_t>("")};
  tiledb_error_handle_t* error{errorx};
  tiledb::api::ExceptionActionErr h{&error};
  REQUIRE(error != nullptr);
  h.action_on_success();
  CHECK(error == nullptr);
  break_handle(errorx);
}

TEST_CASE("ExceptionActionErr - action") {
  tiledb_error_handle_t* error{nullptr};
  tiledb::api::ExceptionActionErr h{&error};
  h.action(std::runtime_error("an error message"));
  REQUIRE(error != nullptr);
  REQUIRE_NOTHROW(tiledb::api::ensure_error_is_valid(error));
  CHECK(error->message() == "TileDB internal: an error message");
  break_handle(error);
}

TEST_CASE("ExceptionActionCtxErr - action") {
  auto ctx{make_handle<tiledb_ctx_handle_t>(tiledb::sm::Config{})};
  tiledb_error_handle_t* error;
  tiledb::api::ExceptionActionCtxErr h{ctx, &error};
  auto x{ctx->context().last_error()};
  CHECK(!x.has_value());
  h.action(std::runtime_error("an error message"));
  auto y{ctx->context().last_error()};
  REQUIRE(y.has_value());
  CHECK(y.value() == "TileDB internal: an error message");
  REQUIRE(error != nullptr);
  REQUIRE_NOTHROW(tiledb::api::ensure_error_is_valid(error));
  CHECK(error->message() == "TileDB internal: an error message");
  break_handle(error);
}

TEST_CASE("CAPIFunction - return") {
  tiledb_ctx_handle_t* ctx{
      make_handle<tiledb_ctx_handle_t>(tiledb::sm::Config{})};
  tiledb_error_handle_t* error{nullptr};
  always_good_wrapped::handler_type handler{ctx, &error};
  always_good_wrapped::function(handler);
  auto x{ctx->context().last_error()};
  CHECK(!x.has_value());
  CHECK(error == nullptr);
  break_handle(ctx);
}

TEST_CASE("CAPIFunction - Invalid context") {
  tiledb_error_handle_t* error{nullptr};
  always_good_wrapped::handler_type handler{nullptr, &error};
  CHECK(always_good_wrapped::function(handler) == TILEDB_INVALID_CONTEXT);
  CHECK(error != nullptr);
}

TEST_CASE("CAPIFunction - Invalid error") {
  tiledb_ctx_handle_t* ctx{
      make_handle<tiledb_ctx_handle_t>(tiledb::sm::Config{})};
  always_good_wrapped::handler_type handler{ctx, nullptr};
  CHECK(always_good_wrapped::function(handler) == TILEDB_INVALID_ERROR);
  auto x{ctx->context().last_error()};
  CHECK(x.has_value());
  break_handle(ctx);
}

TEST_CASE("CAPIFunction - throw") {
  tiledb_ctx_handle_t* ctx{
      make_handle<tiledb_ctx_handle_t>(tiledb::sm::Config{})};
  tiledb_error_handle_t* error{nullptr};
  always_throw_wrapped::handler_type handler{ctx, &error};
  always_throw_wrapped::function(handler);
  auto x{ctx->context().last_error()};
  REQUIRE(x.has_value());
  CHECK(x.value() == "Test: error");
  REQUIRE(error != nullptr);
  REQUIRE_NOTHROW(tiledb::api::ensure_error_is_valid(error));
  CHECK(error->message() == "Test: error");
  break_handle(error);
}

capi_return_t tf_assign(int input, int* output) {
  *output = input;
  return TILEDB_OK;
}

TEST_CASE("api_entry_plain - return") {
  int k{1};
  auto rc{tiledb::api::api_entry_plain<tf_assign>(2, &k)};
  CHECK(tiledb_status(rc) == TILEDB_OK);
  CHECK(k == 2);
}

TEST_CASE("api_entry_plain - throw") {
  auto rc{tiledb::api::api_entry_plain<tf_always_throw>()};
  CHECK(tiledb_status(rc) == TILEDB_ERR);
}

void tf_void_assign(int input, int* output) {
  *output = input;
}

void tf_void_throw() {
  (void)tf_always_throw();
}

TEST_CASE("api_entry_void - return") {
  int k{3};
  tiledb::api::api_entry_void<tf_void_assign>(4, &k);
  CHECK(k == 4);
}

TEST_CASE("api_entry_void - throw") {
  CHECK_NOTHROW(tiledb::api::api_entry_void<tf_void_throw>());
}

capi_return_t tf_context_assign(tiledb_ctx_handle_t*, int input, int* output) {
  return tf_assign(input, output);
}

capi_return_t tf_context_throw(tiledb_ctx_handle_t*) {
  return tf_always_throw();
}

TEST_CASE("api_entry_with_context - return") {
  ordinary_context x{};
  auto y1{x.context->last_error()};
  REQUIRE(!y1.has_value());
  int k{6};
  auto rc{
      tiledb::api::api_entry_with_context<tf_context_assign>(x.context, 7, &k)};
  CHECK(tiledb_status(rc) == TILEDB_OK);
  CHECK(k == 7);
  auto y2{x.context->last_error()};
  CHECK(!y2.has_value());
}

TEST_CASE("api_entry_with_context - throw") {
  ordinary_context x{};
  auto y1{x.context->last_error()};
  REQUIRE(!y1.has_value());
  auto rc{tiledb::api::api_entry_with_context<tf_context_throw>(x.context)};
  CHECK(tiledb_status(rc) == TILEDB_ERR);
  auto y2{x.context->last_error()};
  CHECK(y2.has_value());
}

TEST_CASE("api_entry_context - return") {
  ordinary_context x{};
  int k{7};
  auto rc{tiledb::api::api_entry_context<tf_assign>(x.context, 8, &k)};
  CHECK(tiledb_status(rc) == TILEDB_OK);
  CHECK(k == 8);
}

TEST_CASE("api_entry_context - throw") {
  ordinary_context x{};
  auto rc{tiledb::api::api_entry_context<tf_always_throw>(x.context)};
  CHECK(tiledb_status(rc) == TILEDB_ERR);
  auto e{x.context->last_error()};
  CHECK(e.has_value());
  CHECK(e.value() == "Test: error");
}

TEST_CASE("api_entry_error - return") {
  /*
   * `bogus_error` is not valid as an error handle, but it'll never be checked
   * as such. It's there so that `error` can be initialized to non-null.
   */
  tiledb_error_handle_t bogus_error{""};
  tiledb_error_handle_t* error{&bogus_error};
  int k{9};
  auto rc{tiledb::api::api_entry_error<tf_assign>(&error, 10, &k)};
  CHECK(tiledb_status(rc) == TILEDB_OK);
  CHECK(k == 10);
  CHECK(error == nullptr);
}

TEST_CASE("api_entry_error - throw") {
  tiledb_error_handle_t* error{};
  auto rc{tiledb::api::api_entry_error<tf_always_throw>(&error)};
  CHECK(tiledb_status(rc) == TILEDB_ERR);
  REQUIRE(error != nullptr);
  CHECK(error->message() == "Test: error");
  tiledb_error_free(&error);
}

capi_return_t tf_budget_never_available() {
  throw BudgetUnavailable("Test", "budget unavailable");
}
TEST_CASE("BudgetUnavailable - special return value from CAPIFunction") {
  auto rc{tiledb::api::api_entry_plain<tf_budget_never_available>()};
  CHECK(tiledb_status(rc) == TILEDB_BUDGET_UNAVAILABLE);
}

capi_return_t tf_budget_exceeded() {
  throw BudgetExceeded("Test", "budget exceeded");
}
TEST_CASE("BudgetExceeded - ordinary return value from CAPIFunction") {
  auto rc{tiledb::api::api_entry_plain<tf_budget_exceeded>()};
  CHECK(tiledb_status(rc) == TILEDB_ERR);
}
