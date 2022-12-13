/**
 * @file buffer.h
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

#ifndef TILEDB_API_V3_DETAILS_BUFFER_H
#define TILEDB_API_V3_DETAILS_BUFFER_H

#include <vector>

namespace tiledb::test::api::v3::details {

class WriterBufferBase {
  public:
    virtual ~WriterBufferBase() = default;

  protected:
    WriterBufferBase() = default;
};

template<typename T>
class WriterBufferImpl : public WriterBufferBase {
  public:
    WriterBufferImpl() = default;
    WriterBufferImpl(size_t size)
      : WriterBufferBase()
      , buf_(size) {
    }

    template<typename T>
    static std::shared_ptr<WriterBufferBase> create() {
      return std::make_shared<WriterBufferImpl<T>>();
    }

    void emplace_back() {
      buf_.emblace_back();
    }

    void push_back(T val) {
      buf_.push_back(T);
    }

  private:
    std::vector<T> buf_;
};

class WriterBuffer : public WriterBufferImpl {
  public:
    WriterBuffer(std::shared_ptr<WriterBufferBase> buf) : buf_(buf) {}

    template<typename T>
    static std::shared_ptr<WriterBufferBase> create(size_t size = 0) {
      return WriterBuffer(std::make_shared<WriterBufferImpl<T>>(size));
    }

  private:
    std::shared_ptr<WriterBufferBase> buf_;
};

} // namespace tiledb::test::api::v3::details

#endif // TILEDB_API_V3_DETAILS_WRITER_H
