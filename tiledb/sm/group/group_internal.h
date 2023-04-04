/**
 * @file   group_internal.h
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
 * This file defines common internal types for the TileDB Groups module.
 */

#ifndef TILEDB_GROUP_INTERNAL_H
#define TILEDB_GROUP_INTERNAL_H

#include <atomic>

#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group_details.h"
#include "tiledb/sm/group/group_directory.h"
#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class GroupDirectoryException : public StatusException {
 public:
  explicit GroupDirectoryException(const std::string& message)
      : StatusException("GroupDirectory", message) {
  }
};
class GroupException : public StatusException {
 public:
  explicit GroupException(const std::string& message)
      : StatusException("Group", message) {
  }
};
class GroupMemberException : public StatusException {
 public:
  explicit GroupMemberException(const std::string& message)
      : StatusException("GroupMember", message) {
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GROUP_INTERNAL_H
