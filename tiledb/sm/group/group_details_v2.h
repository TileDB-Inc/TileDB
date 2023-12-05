/**
 * @file   group_details_v2.h
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
 * This file defines TileDB Group Details V2
 */

#ifndef TILEDB_GROUP_DETAILS_V2_H
#define TILEDB_GROUP_DETAILS_V2_H

#include <atomic>

#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group_details.h"
#include "tiledb/sm/group/group_details_v1.h"
#include "tiledb/sm/group/group_member.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class GroupDetails;

class GroupDetailsV2 : public GroupDetails {
 public:
  GroupDetailsV2(const URI& group_uri);

  /** Destructor. */
  ~GroupDetailsV2() override = default;

  void serialize(
      const std::vector<std::shared_ptr<GroupMember>>& members,
      Serializer& serializer) const override;

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The format spec version.
   * @return Group detail
   */
  static shared_ptr<GroupDetails> deserialize(
      Deserializer& deserializer, const URI& group_uri);

  /**
   * Returns a Group object from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The format spec version.
   * @return Group detail
   */
  static shared_ptr<GroupDetails> deserialize(
      const std::vector<shared_ptr<Deserializer>>& deserializer,
      const URI& group_uri);

  std::vector<std::shared_ptr<GroupMember>> members_to_serialize()
      const override;

 private:
  /* Format version for class. */
  inline static const format_version_t format_version_ = 2;
};
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GROUP_DETAILS_V2_H
