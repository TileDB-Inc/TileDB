/**
 * @file   group_details_v1.h
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
 * This file defines TileDB Group Details V1
 */

#ifndef TILEDB_GROUP_DETAILS_V1_H
#define TILEDB_GROUP_DETAILS_V1_H

#include <atomic>

#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group_details.h"
#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class GroupDetails;

class GroupDetailsV1 : public GroupDetails {
 public:
  GroupDetailsV1(const URI& group_uri);

  /** Destructor. */
  ~GroupDetailsV1() override = default;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @return Status
   */
  void serialize(Serializer& serializer) override;

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The format spec version.
   * @return Status and Attribute
   */
  static shared_ptr<GroupDetails> deserialize(
      Deserializer& deserializer, const URI& group_uri);

 protected:
  /**
   * Apply any pending member additions or removals
   *
   * mutates members_ and clears members_to_modify_;
   */
  void apply_pending_changes() override;

 private:
  /* Format version for class. */
  inline static const format_version_t format_version_ = 1;
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GROUP_DETAILS_V1_H
