/**
 * @file   meet-capnproto-win32-include-expectations.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file defines items needed to successfully compile tiledb capnproto
 * dependent modules with MSVC on windows, after first checking that they are
 * not already defined.
 */

#ifndef TILEDB_SERIALIZATION_CAPNPROTO_WIN32_ADJUSTMENT_H
#define TILEDB_SERIALIZATION_CAPNPROTO_WIN32_ADJUSTMENT_H

// vs2017, capnp_utils.h several layers down includes kj/windows-sanity.h, which
// *assumes* 'ERROR' will have been #define'd somewhere previously, which in
// this compilation stream, (with capnp v0.8.0), is *not* true!!! so compilation
// fails... (appears to also assume  similarly for 'VOID'...)  See if can safely
// #include <windows.h> here so that the appropriate 'ERROR'/'VOID' entities
// will be defined with the correct values...
#ifdef _WIN32

#ifndef _INC_WINDOWS
#include <windows.h>  //seems to still not find 'ERROR'
#else

// apparently windows.h has (generally) been included in files where we include
// this file, and  these definitions are not (seen?)/processed by the compiler,
// so define these locally here as needed to enable capnproto (capnp_utils.h) to
// compile successfully.

#ifndef ERROR
#define ERROR \
  0  // seems to be #define'd in wingdi.h, somehow we're avoiding that in
     // include stream?
#endif
#ifndef VOID
#define VOID void
#endif

#endif  // ifndef _INC_WINDOWS

#endif  // def _WIN32

#endif  // TILEDB_SERIALIZATION_CAPNPROTO_WIN32_ADJUSTMENT_H
