/**
 * @file ls_callback.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This defines the type definition used for ls callback functions in VFS.
 */

#ifndef TILEDB_LS_CALLBACK_H
#define TILEDB_LS_CALLBACK_H

#include <cstdint>
#include <functional>

namespace tiledb::sm {
/**
 * Typedef for the callback function invoked on each object collected by ls.
 *
 * @param path The path of a visited object for the relative filesystem.
 * @param path_len The length of the path string.
 * @param object_size The size of the object at the path.
 * @param data Cast to user defined struct to store paths and offsets. See
 *      VFSExperimental::LsObjects as an example.
 * @return `1` if the walk should continue to the next object, `0` if the walk
 *    should stop, and `-1` on error.
 */
typedef std::function<int32_t(const char*, size_t, uint64_t, void*)> LsCallback;
}  // namespace tiledb::sm

#endif  // TILEDB_LS_CALLBACK_H
