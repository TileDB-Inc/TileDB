/**
 * @file channel_operation.h
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
 * This file declares the C++ API for the TileDB ChannelOperation class.
 */

#ifndef TILEDB_CPP_API_CHANNEL_OPERATION_H
#define TILEDB_CPP_API_CHANNEL_OPERATION_H

#include "channel_operator.h"
#include "context.h"
#include "query.h"
#include "tiledb.h"
#include "tiledb_experimental.h"

namespace tiledb {

class ChannelOperation {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor required for COUNT */
  ChannelOperation() = default;

  /**
   * Create a ChannelOperation by wrapping a pointer allocated by the C API
   *
   * @param ctx The Context to use.
   * @param operation The tiledb_channel_operation_t allocated by the C API.
   */
  ChannelOperation(
      [[maybe_unused]] const Context& ctx,
      tiledb_channel_operation_t* operation)
      : deleter_() {
    operation_ =
        std::shared_ptr<tiledb_channel_operation_t>(operation, deleter_);
  }

  ChannelOperation(const ChannelOperation&) = default;
  ChannelOperation(ChannelOperation&&) = default;
  ChannelOperation& operator=(const ChannelOperation&) = default;
  ChannelOperation& operator=(ChannelOperation&&) = default;

  /** Destructor. */
  virtual ~ChannelOperation() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 private:
  virtual const tiledb_channel_operation_t* ptr() const {
    return operation_.get();
  };

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Create a ChannelOperation
   *
   * @param query The TileDB query
   * @param input_field The attribute name the aggregate operation will run on
   */
  template <
      class Op,
      std::enable_if_t<std::is_base_of_v<ChannelOperator, Op>, bool> = true>
  static ChannelOperation create(
      const Query& query, const std::string& input_field) {
    const Context& ctx = query.ctx();
    tiledb_channel_operation_t* operation = nullptr;
    ctx.handle_error(tiledb_create_unary_aggregate(
        ctx.ptr().get(),
        query.ptr().get(),
        Op::ptr(),
        input_field.c_str(),
        &operation));
    return ChannelOperation(ctx, operation);
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Deleter wrapper. */
  impl::Deleter deleter_;

  /** Pointer to the TileDB C channel operation object. */
  std::shared_ptr<tiledb_channel_operation_t> operation_;

  friend class QueryExperimental;
  friend class QueryChannel;
};

class CountOperation : public ChannelOperation {
 private:
  virtual const tiledb_channel_operation_t* ptr() const {
    return tiledb_aggregate_count;
  }
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_CHANNEL_OPERATION_H
