/**
 * @file common/governor/governor.h
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
 */

#ifndef TILEDB_COMMON_GOVERNOR_H
#define TILEDB_COMMON_GOVERNOR_H

namespace tiledb::common {

/**
 * The governor grants top-level permissions for resource use.
 *
 * 1. Execution resources. The governor may revoke permission to execute, which
 *    allows orderly shutdown of threads. This allows for recovery from
 *    otherwise-unrecoverable conditions such as memory exhaustion.
 * 2. Memory resources.
 *
 * Currently the governor is a stub with minimal functionality. It only has the
 * ability to receive a signal about memory exhaustion.
 */
class Governor {
 public:
  /**
   * Signal to the governor that the system is out of memory.
   */
  static void memory_panic();
};

}  // namespace tiledb::common

#endif  // TILEDB_COMMON_GOVERNOR_H
