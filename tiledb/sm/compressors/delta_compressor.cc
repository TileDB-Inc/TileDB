/**
 * @file   delta_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file implements the delta compressor class.
 */

#include "tiledb/sm/compressors/delta_compressor.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define ABS(x) ((x) < 0) ? uint64_t(-(x)) : uint64_t(x)

using namespace tiledb::common;

namespace tiledb::sm {

class DeltaCompressorException : public StatusException {
 public:
  explicit DeltaCompressorException(const std::string& message)
      : StatusException("DeltaCompressor", message) {
  }
};

/* ****************************** */
/*               API              */
/* ****************************** */

void Delta::compress(
    Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer) {
  switch (type) {
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
      Delta::compress<std::byte>(input_buffer, output_buffer);
      break;
    case Datatype::INT8:
      Delta::compress<int8_t>(input_buffer, output_buffer);
      break;
    case Datatype::BOOL:
    case Datatype::UINT8:
      Delta::compress<uint8_t>(input_buffer, output_buffer);
      break;
    case Datatype::INT16:
      Delta::compress<int16_t>(input_buffer, output_buffer);
      break;
    case Datatype::UINT16:
      Delta::compress<uint16_t>(input_buffer, output_buffer);
      break;
    case Datatype::INT32:
      Delta::compress<int>(input_buffer, output_buffer);
      break;
    case Datatype::UINT32:
      Delta::compress<uint32_t>(input_buffer, output_buffer);
      break;
    case Datatype::INT64:
      Delta::compress<int64_t>(input_buffer, output_buffer);
      break;
    case Datatype::UINT64:
      Delta::compress<uint64_t>(input_buffer, output_buffer);
      break;
    case Datatype::CHAR:
      Delta::compress<char>(input_buffer, output_buffer);
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      Delta::compress<int64_t>(input_buffer, output_buffer);
      break;
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      Delta::compress<uint8_t>(input_buffer, output_buffer);
      break;
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      throw DeltaCompressorException(
          "Compression is not yet supported for float datatypes.");
    default:
      throw DeltaCompressorException(
          "Compression failed; Unsupported datatype");
  }
}

void Delta::decompress(
    Datatype type,
    ConstBuffer* input_buffer,
    PreallocatedBuffer* output_buffer) {
  switch (type) {
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
      Delta::decompress<std::byte>(input_buffer, output_buffer);
      break;
    case Datatype::INT8:
      Delta::decompress<int8_t>(input_buffer, output_buffer);
      break;
    case Datatype::BOOL:
    case Datatype::UINT8:
      Delta::decompress<uint8_t>(input_buffer, output_buffer);
      break;
    case Datatype::INT16:
      Delta::decompress<int16_t>(input_buffer, output_buffer);
      break;
    case Datatype::UINT16:
      Delta::decompress<uint16_t>(input_buffer, output_buffer);
      break;
    case Datatype::INT32:
      Delta::decompress<int>(input_buffer, output_buffer);
      break;
    case Datatype::UINT32:
      Delta::decompress<uint32_t>(input_buffer, output_buffer);
      break;
    case Datatype::INT64:
      Delta::decompress<int64_t>(input_buffer, output_buffer);
      break;
    case Datatype::UINT64:
      Delta::decompress<uint64_t>(input_buffer, output_buffer);
      break;
    case Datatype::CHAR:
      Delta::decompress<char>(input_buffer, output_buffer);
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      Delta::decompress<int64_t>(input_buffer, output_buffer);
      break;
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      Delta::decompress<uint8_t>(input_buffer, output_buffer);
      break;
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      throw DeltaCompressorException(
          "Decompression is not yet supported for float datatypes.");
    default:
      throw DeltaCompressorException(
          "Decompression failed; Unsupported datatype");
  }
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
void Delta::compress(ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Calculate number of values and handle trivial case
  uint64_t value_size = sizeof(T);
  uint64_t num = input_buffer->size() / value_size;
  assert(num > 0 && (input_buffer->size() % value_size == 0));

  auto in = (T*)input_buffer->data();

  // Write number of values
  throw_if_not_ok(output_buffer->write(&num, sizeof(uint64_t)));

  // Write first value
  throw_if_not_ok(output_buffer->write(&in[0], value_size));
  if (num > 1) {
    for (uint64_t i = 1; i < num; ++i) {
      int64_t cur_delta = int64_t(in[i]) - int64_t(in[i - 1]);
      throw_if_not_ok(output_buffer->write(&cur_delta, value_size));
    }
  }
}

template <class T>
void Delta::decompress(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  uint64_t num = 0;
  uint64_t value_size = sizeof(T);
  throw_if_not_ok(input_buffer->read(&num, sizeof(uint64_t)));

  // Read first value
  T last_value;
  T cur_value;
  throw_if_not_ok(input_buffer->read(&last_value, value_size));
  throw_if_not_ok(output_buffer->write(&last_value, value_size));
  if (num > 1) {
    // Decompress rest of the values
    for (uint64_t i = 1; i < num; ++i) {
      throw_if_not_ok(input_buffer->read(&cur_value, value_size));
      cur_value = static_cast<T>(int64_t(last_value) + int64_t(cur_value));
      throw_if_not_ok(output_buffer->write(&cur_value, value_size));
      last_value = cur_value;
    }
  }
}

// Explicit template instantiations

template void Delta::compress<std::byte>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<char>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<int8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<uint8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<int16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<uint16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<int>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<uint32_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<int64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template void Delta::compress<uint64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);

template void Delta::decompress<std::byte>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<char>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<int8_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<uint8_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<int16_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<uint16_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<int>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<uint32_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<int64_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template void Delta::decompress<uint64_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);

}  // namespace tiledb::sm
