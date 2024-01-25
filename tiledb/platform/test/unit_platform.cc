/**
 * @file tiledb/platform/test/unit_platform.cc
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
 * Tests the platform header.
 */

#include <iostream>

#include "tiledb/platform/platform.h"
#include "unit_platform.h"

using namespace tiledb;

TEST_CASE("Platform: Test OS constexpr flags", "[platform][os_flags]") {
  std::string os_name(PLATFORM_OS_NAME);

  if (os_name == "Windows") {
    REQUIRE(platform::is_os_windows);
    REQUIRE(!platform::is_os_macosx);
    REQUIRE(!platform::is_os_linux);
  } else if (os_name == "Darwin") {
    REQUIRE(!platform::is_os_windows);
    REQUIRE(platform::is_os_macosx);
    REQUIRE(!platform::is_os_linux);
  } else if (os_name == "Linux") {
    REQUIRE(!platform::is_os_windows);
    REQUIRE(!platform::is_os_macosx);
    REQUIRE(platform::is_os_linux);
  } else {
    std::cerr << "Warning: Unknown OS name reported by CMake: "
              << PLATFORM_OS_NAME << std::endl;
  }
}
