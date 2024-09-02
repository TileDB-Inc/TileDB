/**
 * @file object_mutex.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares an object mutex.
 */

#ifndef TILEDB_OBJECT_MUTEX_H
#define TILEDB_OBJECT_MUTEX_H

#include <mutex>

namespace tiledb::sm {

/**
 * There was previously a mutex member variable in `StorageManagerCanonical`
 * for providing thread-safety upon the creation of TileDB objects. This header
 * defines a standalone mutex to preserve this legacy behavior. The mutex is
 * intended to act as an interim solution as APIs are migrated out of
 * `StorageManagerCanonical`, but will likely eventually be adopted to protect
 * the atomicity of all object operations.
 *
 * @section Maturity
 * Once the migration of APIs out of `StorageManagerCanonical` is complete,
 * an audit should be done to determine the true merit of this header.
 */
extern std::mutex object_mtx;

}  // namespace tiledb::sm

#endif  // TILEDB_OBJECT_MUTEX_H
