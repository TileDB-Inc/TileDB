/**
 * @file   config_iter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines class ConfigIter.
 */

#ifndef TILEDB_CONFIG_ITER_H
#define TILEDB_CONFIG_ITER_H

#include <functional>
#include <map>
#include <string>

namespace tiledb {
namespace sm {

class Config;

/**
 * Implements a config iterator.
 *
 * @note: The iterator is not designed to be multi-threaded.
 */
class ConfigIter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ConfigIter(const Config* config, const std::string& prefix = "");

  /** Destructor. */
  ~ConfigIter();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns `true` if the iterator has reached its end. */
  bool end() const;

  /** Advances the iterator. */
  void next();

  /** Returns the current parameter name pointed by the iterator. */
  const std::string& param() const;

  /** Resets the iterator. */
  void reset(const Config* config, const std::string& prefix);

  /** Returns the current parameter value pointed by the iterator. */
  const std::string& value() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The config parameter-value pairs. */
  std::reference_wrapper<const std::map<std::string, std::string>>
      param_values_;

  /** An iterator pointing to the current parameter-value pair. */
  std::map<std::string, std::string>::const_iterator it_;

  /** The prefix used to constrain the parameters to be iterated on. */
  std::string prefix_;

  /** Current parameter. */
  std::string param_;

  /** Current value. */
  std::string value_;

  /** Config */
  const Config* config_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Advance the iterator while the current paramerer does not
   * have the same prefix as `prefix_`. Do not advance if `prefix_ == ""`.
   */
  void next_while_not_prefix();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONFIG_ITER_H
