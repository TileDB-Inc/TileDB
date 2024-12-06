/**
 * @file tdb_rapidcheck.h
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
 * This file wraps <rapidcheck.h> and <rapidcheck/catch.h> for convenience
 * (and compatibility due to a snag with <rapidcheck/catch.h>).
 */

#ifndef TILEDB_MISC_TDB_RAPIDCHECK_H
#define TILEDB_MISC_TDB_RAPIDCHECK_H

/*
 * `catch` must be included first to define
 * `CATCH_TEST_MACROS_HPP_INCLUDED`
 * which is the bridge between v2 and v3 compatibility for <rapidcheck/catch.h>
 */
#include <test/support/tdb_catch.h>

#include <rapidcheck.h>
#include <rapidcheck/catch.h>

#endif  // TILEDB_MISC_TDB_RAPIDCHECK_H
