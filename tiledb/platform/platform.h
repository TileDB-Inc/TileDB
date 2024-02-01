/**
 * @file   tiledb/platform/platform.h
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
 *
 * Platform/machine config of the TileDB library.
 */

#ifndef TILEDB_PLATFORM_H
#define TILEDB_PLATFORM_H

namespace tiledb::platform {
/** Operating System */
#if defined(_WIN32)
constexpr bool is_os_windows = true;
constexpr bool is_os_macosx = false;
constexpr bool is_os_linux = false;
#elif defined(__APPLE__) && defined(__MACH__)
constexpr bool is_os_windows = false;
constexpr bool is_os_macosx = true;
constexpr bool is_os_linux = false;
#elif defined(__linux)
constexpr bool is_os_windows = false;
constexpr bool is_os_macosx = false;
constexpr bool is_os_linux = true;
#endif  // _WIN32

}  // namespace tiledb::platform
#endif  // TILEDB_PLATFORM_H
