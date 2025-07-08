/**
 * @file filesystem_base.h
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
 * This file implements the FilesystemBase class.
 */

#include "filesystem_base.h"

namespace tiledb::sm {

/* ********************************* */
/*                API                */
/* ********************************* */

void FilesystemBase::sync(const URI&) const {
  throw UnsupportedOperation("sync");
}

bool FilesystemBase::is_bucket(const URI&) const {
  throw UnsupportedOperation("is_bucket");
}

bool FilesystemBase::is_empty_bucket(const URI&) const {
  throw UnsupportedOperation("is_empty_bucket");
}

void FilesystemBase::create_bucket(const URI&) const {
  throw UnsupportedOperation("create_bucket");
}

void FilesystemBase::remove_bucket(const URI&) const {
  throw UnsupportedOperation("remove_bucket");
}

void FilesystemBase::empty_bucket(const URI&) const {
  throw UnsupportedOperation("empty_bucket");
}

}  // namespace tiledb::sm
