/**
 * @file tdb_catch.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2023 TileDB, Inc.
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
 * This file is a transitional wrapper for catch.hpp to accommodate known coming
 * changes catch2 => catch3 header organization and providing the possibility of
 * handling both until all build environments have moved to the catch3 versions.
 */

#ifndef TILEDB_MISC_TDB_CATCH_H
#define TILEDB_MISC_TDB_CATCH_H

/*
 * The header-only version of Catch includes Windows system headers that bleed
 * out preprocessor definitions. This has been reported as
 * https://github.com/catchorg/Catch2/issues/2432. It may not be a problem for
 * version 3 of Catch, which isn't header-only.
 *
 * We need to detect and remove definitions that are a problem. It shouldn't be
 * strictly necessary, but out of a superabundance of caution, we detect
 * previous definitions and leave them unchanged.
 *   - DELETE
 */
#if defined(DELETE)
#define TILEDB_CATCH_DELETE_PREDEFINED DELETE
#endif

/*
 * The actual payload of this file
 */
#include <catch2/catch_all.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

namespace Catch {
template <typename T>
struct StringMaker<std::optional<T>> {
  static std::string convert(std::optional<T> const& value) {
    if (value.has_value()) {
      return "Some(" + StringMaker<T>::convert(value.value()) + ")";
    } else {
      return "None";
    }
  }
};

template <>
struct StringMaker<std::pair<std::string, size_t>> {
  static std::string convert(const std::pair<std::string, size_t>& value) {
    std::ostringstream oss;
    oss << "(" << value.first << ", " << value.second << ")";
    return oss.str();
  }
};
}  // namespace Catch

/*
 * Clean up preprocessor definitions
 */
#if defined(TILEDB_CATCH_DELETE_PREDEFINED)
#define DELETE TILEDB_CATCH_DELETE_PREDEFINED
#undef TILEDB_CATCH_DELETE_PREDEFINED
#else
#if defined(DELETE)
#undef DELETE
#endif
#endif

#endif  // TILEDB_MISC_TDB_CATCH_H
