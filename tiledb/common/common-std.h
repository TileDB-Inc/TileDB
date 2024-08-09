/**
 * @file   tiledb/common/common-std.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Common facilities of the TileDB library. This file is for use by common
 * facilities that will themselves be included in "common.h", and thus can't use
 * that file to avoid self- reference. This file contains all the declarations
 * from `std`.
 */

#ifndef TILEDB_COMMON_COMMON_STD_H
#define TILEDB_COMMON_COMMON_STD_H

#include <cstdint>

/**
 * Size type for anything in external storage.
 *
 * Note that on some platforms `storage_size_t` may be larger than `size_t`. It
 * should not be assumed that anything in external storage will fit in memory
 * (even virtual memory).
 */
using storage_size_t = uint64_t;

/**
 * Type for anything format version related.
 */
using format_version_t = uint32_t;

/**
 * Type for anything offsets related.
 */
using offsets_t = uint64_t;

/*
 * Value manipulation
 */
#include <utility>
using std::forward;
using std::move;
using std::swap;

/*
 * Structured binding.
 *
 * Structured binding allows multiple return values from functions to be
 * returned simultaneously. A common pattern this library uses to return
 * a success boolean and the return value when successful. In order to do this,
 * the return value must be optional.
 */
#include <tuple>
using std::get;
using std::ignore;
using std::tie;
using std::tuple;

#include <optional>
using std::nullopt;
using std::optional;

#include <span>
using std::span;

using std::size_t;

/**
 * @section Reference
 * Documentation on the preprocessor operators
 * https://en.cppreference.com/w/cpp/preprocessor/replace
 */
/*
 * The '#' stringize operator applies when interpreting macro arguments,
 * so there's a need for two macro invocations. The first prepends the
 * operator and the second interprets it.
 */
#if defined(TILEDB_TOKEN_TO_STRING)
#error TO_STRINGTOKEN is already defined
#endif
#if defined(TILEDB_TO_STRING)
#error TO_STRING is already defined
#endif
#define TILEDB_TOKEN_TO_STRING(x) #x
#define TILEDB_TO_STRING(x) TILEDB_TOKEN_TO_STRING(x)

/*
 * The '##' concatenation operator applies when interpreting macro arguments,
 * so there's a need for two macro invocations. The first constructions a token
 * with an internal operator and the second interprets it.
 */
#if defined(TILEDB_TOKEN_JOIN)
#error JOINTOKEN is already defined
#endif
#if defined(TILEDB_JOIN)
#error JOIN is already defined
#endif
#define TILEDB_TOKEN_JOIN(x, y) x##y
#define TILEDB_JOIN(x, y) TILEDB_TOKEN_JOIN(x, y)

#endif  // TILEDB_COMMON_COMMON_STD_H
