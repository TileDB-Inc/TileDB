/**
 * @file   range_manager_strategy.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines classes that implement different stratgegies for managing
 * ranges used by the RangeManager class.
 */

#ifndef TILEDB_RANGE_MANAGER_STRATEGY_H
#define TILEDB_RANGE_MANAGER_STRATEGY_H

#include "tiledb/common/interval/interval.h"
#include "tiledb/common/status.h"

/**
 * Example base calass for the range management strategy. Specific methods
 * and subclasses to be filled in as needed.
 */
class RangeStrategyBase {
  virtual ~RangeStrategyBase() = default;

  virtual bool is_default() = 0;

  virtual Status add_range(
      std::vector<std::vector<Range>>& ranges, Range&& new_range) = 0;

  virtual Status add_range_unsafe(
      std::vector<std::vector<Range>>& ranges, Range&& new_range) = 0;

  virtual Status sort_ranges(
      std::vector<std::vector<Range>>& range, ThreadPool* const compute_tp) = 0;
};

<template T> class DefaultRangeStrategy : public RangeStrategyBase {};

<template T> class BasicRangeStrategy : public RangeStrategyBase {};

<template T> class RangeStrategyWithCoalesce : public BasicRangeStrategy {};

#endif
