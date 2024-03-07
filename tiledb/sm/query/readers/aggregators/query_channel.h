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

/**
 * Original class is only used for capnp (de)serialization. `class Query` uses
 * its own container to hold aggregates.
 */
class LegacyQueryAggregatesOverDefault {
 public:
  using ChannelAggregates =
      std::unordered_map<std::string, shared_ptr<IAggregator>>;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Construct a QueryChannel
   *
   * @param is_default If true, this is the default query channel
   * @param aggregates A map of aggregators by output field name
   */
  LegacyQueryAggregatesOverDefault(
      bool is_default, const ChannelAggregates& aggregates)
      : default_(is_default)
      , aggregates_{aggregates} {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * @return true if this is the default query channel
   */
  bool is_default() const {
    return default_;
  }

  /**
   * @return the map of aggregator pointers
   */
  const ChannelAggregates& aggregates() const {
    return aggregates_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
  bool default_;
  ChannelAggregates aggregates_;
};

/* forward declaration */
class Query;

/**
 * Replacement for the current QueryChannel, which does not work for more than
 * the initial case with a default channel and only simple aggregates.
 *
 * Responsibility for choosing channel identifiers is the responsibility of
 * `class Query`; this class merely carries the resulting identifier.
 */
class QueryChannel {
  std::reference_wrapper<Query> query_;
  size_t id_;

 public:
  /**
   * Default constructor is deleted. A channel makes no sense without a query.
   */
  QueryChannel() = delete;
  /*
   * Ordinary constructor.
   */
  QueryChannel(Query& q, size_t id)
      : query_(q)
      , id_(id) {
    (void)id_;
  }
  /**
   * Copy constructor is the default.
   */
  QueryChannel(const QueryChannel&) = default;
  /**
   * Move constructor is the default.
   */
  QueryChannel(QueryChannel&&) = default;
  /**
   * Copy assignment is the default.
   */
  QueryChannel& operator=(const QueryChannel&) = default;
  /**
   * Move assignment is the default.
   */
  QueryChannel& operator=(QueryChannel&&) = default;

  /**
   * Accessor for query member
   */
  inline Query& query() {
    return query_;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_CHANNEL_H
