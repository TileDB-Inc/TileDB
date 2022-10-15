/**
 * @file   config_iter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements class ConfigIter.
 */

#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/config/config.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ConfigIter::ConfigIter(const Config& config, const std::string& prefix)
    : param_values_(config.param_values())
    , prefix_(prefix)
    , config_(config) {
  it_ = param_values_.get().begin();
  next_while_not_prefix();
}

ConfigIter::~ConfigIter() = default;

/* ****************************** */
/*                API             */
/* ****************************** */

bool ConfigIter::end() const {
  return it_ == param_values_.get().end();
}

void ConfigIter::next() {
  if (it_ != param_values_.get().end())
    it_++;
  next_while_not_prefix();
}

const std::string& ConfigIter::param() const {
  return param_;
}

void ConfigIter::reset(const Config& config, const std::string& prefix) {
  prefix_ = prefix;
  param_values_ = config.param_values();
  it_ = param_values_.get().begin();
  next_while_not_prefix();
}

const std::string& ConfigIter::value() const {
  return value_;
}

/* ********************************* */
/*         PRIVATE METHODS           */
/* ********************************* */

void ConfigIter::next_while_not_prefix() {
  if (!prefix_.empty()) {
    while (!end() && it_->first.find(prefix_) != 0)
      ++it_;
  }

  if (it_ != param_values_.get().end()) {
    param_ = it_->first.substr(prefix_.size());
    bool found;
    value_ = config_.get(param_, &found);
    // If for some reason the param is missing from the config, use the original
    // one from values map
    if (!found)
      value_ = it_->second;
  }
}

}  // namespace sm
}  // namespace tiledb
