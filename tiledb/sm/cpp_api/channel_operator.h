/**
 * @file channel_operator.h
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
 * This file declares the C++ API for the TileDB ChannelOperator class.
 */

#ifndef TILEDB_CPP_API_CHANNEL_OPERATOR_H
#define TILEDB_CPP_API_CHANNEL_OPERATOR_H

#include "context.h"
#include "query.h"
#include "tiledb.h"
#include "tiledb_experimental.h"

namespace tiledb {

class ChannelOperator {};

class SumOperator : public ChannelOperator {
 public:
  /* ********************************* */
  /*                API                */
  /* ********************************* */
  static const tiledb_channel_operator_t* ptr() {
    return tiledb_channel_operator_sum;
  }
};

class MinOperator : public ChannelOperator {
 public:
  /* ********************************* */
  /*                API                */
  /* ********************************* */
  static const tiledb_channel_operator_t* ptr() {
    return tiledb_channel_operator_min;
  }
};

class MaxOperator : public ChannelOperator {
 public:
  /* ********************************* */
  /*                API                */
  /* ********************************* */
  static const tiledb_channel_operator_t* ptr() {
    return tiledb_channel_operator_max;
  }
};

class MeanOperator : public ChannelOperator {
 public:
  /* ********************************* */
  /*                API                */
  /* ********************************* */
  static const tiledb_channel_operator_t* ptr() {
    return tiledb_channel_operator_mean;
  }
};

class NullCountOperator : public ChannelOperator {
 public:
  /* ********************************* */
  /*                API                */
  /* ********************************* */
  static const tiledb_channel_operator_t* ptr() {
    return tiledb_channel_operator_null_count;
  }
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_CHANNEL_OPERATOR_H
