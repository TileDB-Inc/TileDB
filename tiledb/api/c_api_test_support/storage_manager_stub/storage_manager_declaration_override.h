/**
 * @file storage_manager_declaration_override.h
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
 * This file defines an override for the alias `StorageManager`.
 */

#ifndef TILEDB_C_API_TEST_SUPPORT_STORAGE_MANAGER_DECLARATION_OVERRIDE_H
#define TILEDB_C_API_TEST_SUPPORT_STORAGE_MANAGER_DECLARATION_OVERRIDE_H

namespace tiledb::sm {

/*
 * Forward declarations
 */
class StorageManagerStub;
template <class>
struct StorageManagerSelector;

/**
 * Selection struct defines the ordinary class used by the name
 * `StorageManager`. Specializing this class to `void` may override the
 * default.
 */
template <>
struct StorageManagerSelector<void> {
  using type = StorageManagerStub;
};

}  // namespace tiledb::sm

#endif  // TILEDB_C_API_TEST_SUPPORT_STORAGE_MANAGER_DECLARATION_OVERRIDE_H
