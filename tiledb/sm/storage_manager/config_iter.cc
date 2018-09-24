/**
 * @file   config_iter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/sm/storage_manager/config_iter.h"

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ConfigIter::ConfigIter(Config* config, std::string prefix)
    : config_(config)
    , prefix_(prefix) {
  param_values_ = config_->param_values(prefix);
  it_ = param_values_.begin();
}

ConfigIter::~ConfigIter() = default;

/* ****************************** */
/*                API             */
/* ****************************** */

bool ConfigIter::end() const {
  return it_ == param_values_.end();
}

void ConfigIter::next() {
  if (it_ != param_values_.end())
    it_++;
}

const std::string& ConfigIter::param() const {
  return it_->first;
}

void ConfigIter::reset(Config* config, std::string prefix) {
  config_ = config;
  param_values_ = config_->param_values(prefix);
  it_ = param_values_.begin();
}

const std::string& ConfigIter::value() const {
  return it_->second;
}

}  // namespace sm
}  // namespace tiledb
