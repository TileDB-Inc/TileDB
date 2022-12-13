/**
 * @file domain.h
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
 * Implementation of the v3 API
 */

#ifndef TILEDB_API_V3_DOMAIN_H
#define TILEDB_API_V3_DOMAIN_H

#include <vector>

namespace tiledb::test::api::v3 {

template<typename T>
class Domain {
  public:
    Domain(std::initializer_list<T> domain) {
      if (domain.size() != 2) {
        throw std::logic_error(
            "Domain size must be 2, not " + std::to_string(domain.size()));
      }
      std::vector<T> vec(domain);
      domain_ = std::make_pair(vec[0], vec[1]);
    }

  private:
    std::pair<T, T> domain_;
};


} // namespace tiledb::test::api::v3

#endif // TILEDB_API_V3_DOMAIN_H

