/**
 * @file   query_channel.h
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
 * This file defines class QueryChannel.
 */

#ifndef TILEDB_QUERY_CHANNEL_H
#define TILEDB_QUERY_CHANNEL_H

#include "tiledb/common/common.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/query/readers/aggregators/iaggregator.h"

namespace tiledb::sm {

class QueryChannel {
 public:
  using ChannelAggregates =
      std::unordered_map<std::string, shared_ptr<IAggregator>>;

  QueryChannel(bool is_default, const ChannelAggregates& aggregates)
      : default_(is_default)
      , aggregates_{aggregates} {
  }

  bool is_default() {
    return default_;
  }

  ChannelAggregates& aggregates() {
    return aggregates_;
  }

 private:
  bool default_;
  ChannelAggregates aggregates_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_CHANNEL_H
