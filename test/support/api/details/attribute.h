/**
 * @file attribute.h
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
 * Implementation details for the v3 Attribute class
 */

#ifndef TILEDB_API_V3_DETAILS_ATTRIBUTE_H
#define TILEDB_API_V3_DETAILS_ATTRIBUTE_H

#include <memory>
#include <optional>

namespace tiledb::test::api::v3::details {

class AttributeBase {
  public:
    // Disable Copy/Move
    AttributeBase(const AttributeBase&) = delete;
    AttributeBase& operator=(const AttributeBase&) = delete;
    AttributeBase(AttributeBase&&) = delete;
    AttributeBase& operator=(AttributeBase&&) = delete;

    virtual ~AttributeBase() = default;

    virtual std::string name() = 0;

  protected:
    AttributeBase() = default;
};

template<typename T>
class AttributeImpl : public AttributeBase {
  public:
    AttributeImpl(std::string name, std::optional<T> fill_value)
    : name_(name)
    , fill_value_(fill_value) {}

    static std::shared_ptr<AttributeBase> create(
        std::string name,
        std::optional<T> fill_value = std::nullopt) {
      return std::make_shared<AttributeImpl>(name, fill_value);
    }

    std::string name() {
      return name_;
    }

    WriterBuffer buffer() {
      return WriterBuffer::create<T>();
    }

  private:
    std::string name_;
    std::optional<T> fill_value_;
};

} // namespace tiledb::test::api::v3::details

#endif // TILEDB_API_V3_DETAILS_ATTRIBUTE_H

