/**
 * @file   throw_catch_types.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * @section DESCRIPTION
 */
#ifndef TILEDB_THROW_CATCH_TYPES_H
#define TILEDB_THROW_CATCH_TYPES_H

namespace tiledb::common {
namespace detail {

enum class throw_catch_target { self, source, sink, last };

class throw_catch_exception : public std::exception {
  throw_catch_target target_{throw_catch_target::self};

 public:
  explicit throw_catch_exception(
      throw_catch_target target = throw_catch_target::self)
      : target_{target} {
  }

  [[nodiscard]] throw_catch_target target() const {
    return target_;
  }
};

class throw_catch_exit : public throw_catch_exception {
  using throw_catch_exception::throw_catch_exception;
};

class throw_catch_wait : public throw_catch_exception {
  using throw_catch_exception::throw_catch_exception;
};

class throw_catch_notify : public throw_catch_exception {
  using throw_catch_exception::throw_catch_exception;
};

}  // namespace detail

const detail::throw_catch_exit throw_catch_exit;
const detail::throw_catch_exit throw_catch_sink_exit{
    detail::throw_catch_target::sink};
const detail::throw_catch_exit throw_catch_source_exit{
    detail::throw_catch_target::source};
const detail::throw_catch_wait throw_catch_sink_wait{
    detail::throw_catch_target::sink};
const detail::throw_catch_wait throw_catch_source_wait{
    detail::throw_catch_target::source};
const detail::throw_catch_notify throw_catch_notify_sink{
    detail::throw_catch_target::sink};
const detail::throw_catch_notify throw_catch_notify_source{
    detail::throw_catch_target::source};

}  // namespace tiledb::common
#endif  // TILEDB_THROW_CATCH_TYPES_H
