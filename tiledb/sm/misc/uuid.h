/**
 * @file   uuid.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
 * This file declares a platform-independent UUID generator.
 */

#ifndef TILEDB_UUID_H
#define TILEDB_UUID_H

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {
namespace uuid {

/**
 * Generates a 128-bit UUID. The string is formatted with hyphens like:
 * 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx' where 'x' is a hexadecimal digit.
 * Note: this function internally acquires a lock.
 *
 * @param uuid Output parameter which will store the UUID in string format.
 * @param hyphenate If false, the UUID string will not be hyphenated.
 * @return Status
 */
Status generate_uuid(std::string* uuid, bool hyphenate = true);

}  // namespace uuid
}  // namespace sm
}  // namespace tiledb

#endif