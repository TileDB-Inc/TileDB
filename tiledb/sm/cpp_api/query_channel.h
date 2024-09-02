/**
 * @file query_channel.h
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
 * This file declares the C++ API for the TileDB QueryChannel class.
 */

#ifndef TILEDB_CPP_API_QUERY_CHANNEL_H
#define TILEDB_CPP_API_QUERY_CHANNEL_H

#include "channel_operation.h"
#include "context.h"
#include "query.h"
#include "tiledb.h"
#include "tiledb_experimental.h"

namespace tiledb {

class QueryChannel {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Create an QueryChannel by wrapping a pointer allocated by the C API
   *
   * @param ctx The Context to use.
   * @param ch The tiledb_query_channel_t allocated by the C API.
   */
  QueryChannel(const Context& ctx, tiledb_query_channel_t* ch)
      : ctx_(ctx)
      , deleter_(&ctx) {
    channel_ = std::shared_ptr<tiledb_query_channel_t>(ch, deleter_);
  }

  QueryChannel() = delete;
  QueryChannel(const QueryChannel&) = default;
  QueryChannel(QueryChannel&&) = default;
  QueryChannel& operator=(const QueryChannel&) = default;
  QueryChannel& operator=(QueryChannel&&) = default;

  /** Destructor. */
  ~QueryChannel() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Apply an aggregate operation on this channel which will
   * produce the results on the output field passed as argument
   *
   * **Example:**
   * @code{.cpp}
   * tiledb::Context ctx;
   * auto array = Array(ctx, uri, TILEDB_READ);
   * Query query(ctx, array);
   * QueryChannel default_channel =
   *    QueryExperimental::get_default_channel(query);
   * default_channel.apply_aggregate("Count", CountOperation{});
   *
   * uint64_t count = 0;
   * uint64_t size = sizeof(uint64_t);
   * query.set_data_buffer("Count", &count, &size)
   * query.submit();
   * @endcode
   *
   * @param output_field the output field where results will be available.
   * @param operation the aggregate operation to be applied on the channel
   */
  void apply_aggregate(
      const std::string& output_field, const ChannelOperation& operation) {
    ctx_.get().handle_error(tiledb_channel_apply_aggregate(
        ctx_.get().ptr().get(),
        channel_.get(),
        output_field.c_str(),
        operation.ptr()));
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

 private:
  /**
   * Create a QueryChannel
   *
   * @param query The TileDB query to use.
   */
  static QueryChannel create_default_channel(const Query& query) {
    const Context& ctx = query.ctx();
    tiledb_query_channel_t* default_channel = nullptr;
    ctx.handle_error(tiledb_query_get_default_channel(
        ctx.ptr().get(), query.ptr().get(), &default_channel));
    return QueryChannel(ctx, default_channel);
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB query. */
  std::reference_wrapper<const Context> ctx_;

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C QueryChannel object. */
  std::shared_ptr<tiledb_query_channel_t> channel_;

  friend class QueryExperimental;
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_QUERY_CHANNEL_H
