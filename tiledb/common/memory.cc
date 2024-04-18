/**
 * @file   memory.cc
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
 * This file contains implementation common memory functions
 */

#ifdef __linux__
#include <features.h>
#ifdef __GLIBC__
#include <malloc.h>
#endif
#endif

#include <sstream>
#include "tiledb/common/logger.h"

namespace tiledb {
namespace common {
void tdb_malloc_trim() {
  std::stringstream ss;
#if defined(__linux__) && defined(__GLIBC__)
  int ret = malloc_trim(0);
  ss << "malloc_trim ";
  if (ret == 0)
    ss << "did not unmap memory";
  else
    ss << "did unmap memory";
  LOG_TRACE(ss.str());
#endif
}
}  // namespace common
}  // namespace tiledb
