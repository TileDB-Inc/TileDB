/**
 * @file tiledb/common/assert.h
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
 * This file provides assertion routines which should be used instead
 * of the C standard library `assert`. It is only used with the
 * `TILEDB_ASSERTIONS` feature.
 *
 * There are several reasons we should avoid vanilla `assert`:
 * 1) It is fully compiled out of `NDEBUG` builds in a non-configurable way.
 *    But we want to enable our `assert`s to run in CI even if `NDEBUG`
 *    is on.
 * 2) Its only failure mode is to crash the whole process with SIGABRT.
 *    This is not always the right way to handle an internal error
 *    for an interactive, possibly multi-tenant process.
 *    A logic error within a single query executor shouldn't need to
 *    crash the whole process - just the query.  Whereas it is appropriate
 *    to crash the whole process if there is a data integrity problem
 *    or evidence of undefined behavior.
 * 3) `assert` doesn't provide any capability to enable additional details
 *    about the context leading up to the failure. This is often the first
 *    step when investigating a failing `assert` - well, what were those
 *    values anyway, if not what I expected?
 *
 * To address each of these issues, this file defines `iassert` and `passert`.
 * - `iassert` ("internal" assert) throws a `AssertFailure` exception if it
 *   fails.  This must not be caught within our query engine code so that it
 *   propagates up to the user-level request, where they may decide what to
 *   do with it (in principle - as of this writing it is not part of any
 *   exposed API).
 * - `passert` ("panic" assert) mimics the traditional `assert` behavior,
 *   panicking and calling `std::abort` in the event of a failure,
 *   printing details to `stderr`.
 *
 * Both routines accept additional variadic arguments which constitute
 * a C++ format string and the arguments to that format string.
 * If the assertion fails, the format string is applied to the additional
 * arguments and the result is appended to the failure payload.
 *
 * Both forms of assert are defined to do nothing if the
 * `TILEDB_ASSERTIONS` build configuration option is not used.
 *
 * When should you use `iassert` versus `passert`?
 *
 * Use `iassert` if your claim is about the logic of a single request.
 * If the condition is false, do the consequences extend beyond the
 * single query? If not then `iassert` is a good choice.
 *
 * Use `passert` if your claim is about process-level data or data
 * which spans multiple requests, possibly representing a corrupt
 * state. If the claim is false, is there any path whatsoever to
 * recovery (including "don't do that again")? If not, then `passert`
 * is a good choice.
 */

#ifndef TILEDB_ASSERT_H
#define TILEDB_ASSERT_H

#ifdef TILEDB_ASSERTIONS

#include <spdlog/fmt/fmt.h>
#include <cstdlib>
#include <iostream>

#define __SOURCE__ (&__FILE__[__SOURCE_DIR_PATH_SIZE__])

namespace tiledb::common {

/**
 * Exception thrown when a recoverable assertion fails.
 *
 * This indicates a bug in TileDB and ideally the receipient
 * of this exception would report it as such.
 */
class [[nodiscard]] AssertFailure : public std::exception {
  AssertFailure(const std::string& what);

 public:
  AssertFailure(const char* file, uint64_t line, const char* expr)
      : AssertFailure(fmt::format(
            "TileDB core library internal error: {}:{}: {}",
            file,
            line,
            expr)) {
  }

  template <typename... Args>
  AssertFailure(
      const char* file,
      uint64_t line,
      const char* expr,
      fmt::format_string<Args...> fmt,
      Args&&... fmt_args)
      : AssertFailure(fmt::format(
            "TileDB core library internal error: {}:{}: {}\nDetails: {}",
            file,
            line,
            expr,
            fmt::format(fmt, std::forward<Args>(fmt_args)...))) {
  }

  const char* what() const noexcept override {
    return what_.c_str();
  }

 private:
  std::string what_;
};

/**
 * Assertion failure which results in an internal error.
 * An exception is thrown.
 *
 * This should be used when the consequence of the assertion being
 * incorrect has consequences which would be scoped to a single request, e.g. a
 * query cannot continue.
 *
 * Called when the argument to `iassert` is false.
 */
[[noreturn]] void iassert_failure(
    const char* file, uint64_t line, const char* expr);

/**
 * Assertion failure which results in an internal error.
 * An exception is thrown, with additional context about what caused the error.
 *
 * This should be used when the consequence of the assertion being
 * incorrect has consequences which would be scoped to a single request, e.g. a
 * query cannot continue.
 *
 * Called when the argument to `iassert` is false.
 */
template <typename... Args>
[[noreturn]] [[maybe_unused]] static void iassert_failure(
    const char* file,
    uint64_t line,
    const char* expr,
    fmt::format_string<Args...> fmt,
    Args&&... fmt_args) {
  throw AssertFailure(file, line, expr, fmt, std::forward<Args>(fmt_args)...);
}

/**
 * Assertion failure which results in a process panic.
 * SIGABRT is raised.
 *
 * This should be used when the consequence of the assertion being
 * incorrect indicates that the internal process state is no longer safe to
 * use. This could mean undefined behavior or corrupt data, for example.
 *
 * Called when the argument to `passert` is false.
 */
[[noreturn]] void passert_failure(
    const char* file, uint64_t line, const char* expr);

/**
 * Assertion failure which results in a process panic.
 * SIGABRT is raised. Provides additional context about what caused the error.
 *
 * This should be used when the consequence of the assertion being
 * incorrect indicates that the internal process state is no longer safe to
 * use. This could mean undefined behavior or corrupt data, for example.
 *
 * Called when the argument to `passert` is false.
 */
template <typename... Args>
[[noreturn]] [[maybe_unused]] static void passert_failure(
    const char* file,
    uint64_t line,
    const char* expr,
    fmt::format_string<Args...> fmt,
    Args&&... fmt_args) {
  std::cerr << "FATAL TileDB core library internal error: " << expr
            << std::endl;
  std::cerr << "  " << file << ":" << line << std::endl;
  std::cerr << "  Details: "
            << fmt::format(fmt, std::forward<Args>(fmt_args)...) << std::endl;
  std::abort();
}

}  // namespace tiledb::common

#define iassert(condition, ...)                             \
  do {                                                      \
    const bool __iassert_value(condition);                  \
    if (!__iassert_value) [[unlikely]] {                    \
      tiledb::common::iassert_failure(                      \
          __SOURCE__, __LINE__, #condition, ##__VA_ARGS__); \
    }                                                       \
  } while (0)

#define passert(condition, ...)                             \
  do {                                                      \
    const bool __passert_value(condition);                  \
    if (!__passert_value) [[unlikely]] {                    \
      tiledb::common::passert_failure(                      \
          __SOURCE__, __LINE__, #condition, ##__VA_ARGS__); \
    }                                                       \
  } while (0)

#else

#define iassert(condition, ...) \
  do {                          \
  } while (0)

#define passert(condition, ...) \
  do {                          \
  } while (0)

#endif

#endif
