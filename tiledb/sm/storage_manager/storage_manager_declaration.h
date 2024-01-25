/**
 * @file   storage_manager_declaration.h
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
 * This file defines the alias `StorageManager`.
 */

#ifndef TILEDB_STORAGE_MANAGER_DECLARATION_H
#define TILEDB_STORAGE_MANAGER_DECLARATION_H

namespace tiledb::sm {

/*
 * Forward declaration of the default alias class for `StorageManager`, for when
 * this file is included separately.
 */
class StorageManagerCanonical;

/**
 * Selection struct defines the ordinary class used by the name
 * `StorageManager`. Specializing this class to `void` may override the
 * default.
 */
template <class T>
struct StorageManagerSelector {
  using type = StorageManagerCanonical;
};

}  // namespace tiledb::sm

#if __has_include("storage_manager_declaration_override.h")
#include "storage_manager_declaration_override.h"
#endif

namespace tiledb::sm {

/**
 * Definition of `StorageManager` is an alias to another class.
 */
using StorageManager = typename StorageManagerSelector<void>::type;

}  // namespace tiledb::sm

#endif  // TILEDB_STORAGE_MANAGER_DECLARATION_H
