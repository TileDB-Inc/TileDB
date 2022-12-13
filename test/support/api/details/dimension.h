/**
 * @file dimension.h
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
 * Implementation details for the v3 Dimension class
 */

#ifndef TILEDB_API_V3_DETAILS_DIMENSION_H
#define TILEDB_API_V3_DETAILS_DIMENSION_H

#include <memory>
#include <optional>

#include "../domain.h"

using namespace tiledb::test::api;

namespace tiledb::test::api::v3::details {

class DimensionBase {
  public:
    // Disable Copy/Move
    DimensionBase(const DimensionBase&) = delete;
    DimensionBase& operator=(const DimensionBase&) = delete;
    DimensionBase(DimensionBase&&) = delete;
    DimensionBase& operator=(DimensionBase&&) = delete;

    virtual ~DimensionBase() = default;

    virtual std::string name() = 0;

  protected:
    DimensionBase() = default;
};

template<typename T>
class DimensionImpl : public DimensionBase {
  public:
    DimensionImpl(
      std::string name,
      std::optional<v3::Domain<T>> domain,
      std::optional<T> extent)
      : name_(name)
      , domain_(domain)
      , extent_(extent) {
    }

    static std::shared_ptr<DimensionBase> create(
        std::string name,
        std::initializer_list<T> domain,
        std::optional<T> extent = std::nullopt) {
      return create(name, v3::Domain(domain), extent);
    }

    static std::shared_ptr<DimensionBase> create(
        std::string name,
        std::optional<v3::Domain<T>> domain = std::nullopt,
        std::optional<T> extent = std::nullopt) {
      return std::make_shared<DimensionImpl>(name, domain, extent);
    }

    std::string name() {
      return name_;
    }

    WriterBuffer buffer() {
      return WriterBuffer::create<T>();
    }

  private:
    std::string name_;
    std::optional<v3::Domain<T>> domain_;
    std::optional<T> extent_;
};

} // namespace tiledb::test::api::v3::details

#endif // TILEDB_API_V3_DETAILS_DIMENSION_H

