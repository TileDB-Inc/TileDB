/**
 * @file   dd_compressor.cc
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
 * This file implements the double delta compressor class.
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

namespace tiledb {
namespace sm {

const uint64_t Delta::OVERHEAD = 17;

/* ****************************** */
/*               API              */
/* ****************************** */

Status Delta::compress(
    Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer) {
  switch (type) {
    case Datatype::BLOB:
      return Delta::compress<int8_t>(
          input_buffer, output_buffer);  // TODO FIXME
    case Datatype::INT8:
      return Delta::compress<int8_t>(input_buffer, output_buffer);
    case Datatype::BOOL:
    case Datatype::UINT8:
      return Delta::compress<uint8_t>(input_buffer, output_buffer);
    case Datatype::INT16:
      return Delta::compress<int16_t>(input_buffer, output_buffer);
    case Datatype::UINT16:
      return Delta::compress<uint16_t>(input_buffer, output_buffer);
    case Datatype::INT32:
      return Delta::compress<int>(input_buffer, output_buffer);
    case Datatype::UINT32:
      return Delta::compress<uint32_t>(input_buffer, output_buffer);
    case Datatype::INT64:
      return Delta::compress<int64_t>(input_buffer, output_buffer);
    case Datatype::UINT64:
      return Delta::compress<uint64_t>(input_buffer, output_buffer);
    case Datatype::CHAR:
      return Delta::compress<char>(input_buffer, output_buffer);
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
      return Delta::compress<int64_t>(input_buffer, output_buffer);
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      return Delta::compress<uint8_t>(input_buffer, output_buffer);
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      return Delta::compress<int32_t>(input_buffer, output_buffer);
      // return LOG_STATUS(Status_CompressionError(
      //     "Cannot compress tile with Delta; Float "
      //     "datatypes are not supported"));
  }

  assert(false);
  return LOG_STATUS(Status_CompressionError(
      "Cannot compress tile with Delta; Not supported datatype"));
}

Status Delta::decompress(
    Datatype type,
    ConstBuffer* input_buffer,
    PreallocatedBuffer* output_buffer) {
  switch (type) {
    case Datatype::BLOB:
      return Delta::decompress<uint8_t>(input_buffer, output_buffer);  // TODO
    case Datatype::INT8:
      return Delta::decompress<int8_t>(input_buffer, output_buffer);
    case Datatype::BOOL:
    case Datatype::UINT8:
      return Delta::decompress<uint8_t>(input_buffer, output_buffer);
    case Datatype::INT16:
      return Delta::decompress<int16_t>(input_buffer, output_buffer);
    case Datatype::UINT16:
      return Delta::decompress<uint16_t>(input_buffer, output_buffer);
    case Datatype::INT32:
      return Delta::decompress<int>(input_buffer, output_buffer);
    case Datatype::UINT32:
      return Delta::decompress<uint32_t>(input_buffer, output_buffer);
    case Datatype::INT64:
      return Delta::decompress<int64_t>(input_buffer, output_buffer);
    case Datatype::UINT64:
      return Delta::decompress<uint64_t>(input_buffer, output_buffer);
    case Datatype::CHAR:
      return Delta::decompress<char>(input_buffer, output_buffer);
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
      return Delta::decompress<int64_t>(input_buffer, output_buffer);
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      return Delta::decompress<uint8_t>(input_buffer, output_buffer);
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      return Delta::decompress<int32_t>(input_buffer, output_buffer);
      // return LOG_STATUS(Status_CompressionError(
      //     "Cannot decompress tile with Delta; Float "
      //     "datatypes are not supported"));
  }

  assert(false);
  return LOG_STATUS(Status_CompressionError(
      "Cannot decompress tile with Delta; Not supported datatype"));
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
Status Delta::compress(ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Calculate number of values and handle trivial case
  uint64_t value_size = sizeof(T);
  uint64_t num = input_buffer->size() / value_size;
  assert(num > 0 && (input_buffer->size() % value_size == 0));

  // TODO TBD Calculate bitsize (ignoring the sign bit)
  auto in = (T*)input_buffer->data();
  unsigned int bitsize = sizeof(T);

  // Write bitsize and number of values
  RETURN_NOT_OK(output_buffer->write(&bitsize, sizeof(uint8_t)));
  RETURN_NOT_OK(output_buffer->write(&num, sizeof(uint64_t)));

  // Trivial case - no compression
   if (bitsize >= sizeof(T) * 8 - 1) {
    RETURN_NOT_OK(output_buffer->write(in, input_buffer->size()));
    return Status::Ok();
  }

  // Write first value
  RETURN_NOT_OK(output_buffer->write(&in[0], value_size));
  if (num == 1)
    return Status::Ok();

  for (uint64_t i = 1; i < num + 1; ++i) {
    int64_t cur_delta = in[i] - in[i - 1];
    RETURN_NOT_OK(output_buffer->write(&cur_delta, value_size));
  }

  return Status::Ok();
}

template <class T>
Status Delta::decompress(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  // Read bitsize and number of values
  uint8_t bitsize_c = 0;
  uint64_t num = 0;
  uint64_t value_size = sizeof(T);
  RETURN_NOT_OK(input_buffer->read(&bitsize_c, sizeof(uint8_t)));
  RETURN_NOT_OK(input_buffer->read(&num, sizeof(uint64_t)));

//   auto in = (T*)input_buffer->data();

  // Practically speaking, this would never overflow, but it's weird to downcast
  // here..
   auto bitsize = static_cast<unsigned int>(bitsize_c);
   auto out = (T*)output_buffer->cur_data();

  // Read first value
  T last_value;
  T cur_value;
  RETURN_NOT_OK(input_buffer->read(&last_value, value_size));
  RETURN_NOT_OK(output_buffer->write(&last_value, value_size));
  if (num == 1)
    return Status::Ok();

  // Decompress rest of the values
  for (uint64_t i = 1; i < num; ++i) {
    RETURN_NOT_OK(input_buffer->read(&cur_value, value_size));
    cur_value = last_value + cur_value;
    RETURN_NOT_OK(output_buffer->write(&cur_value, value_size));
    last_value = cur_value;
  }

  return Status::Ok();
}

template <class T>
tuple<Status, std::optional<unsigned int>> Delta::compute_bitsize(T* in, uint64_t num) {
  // Find maximum absolute double delta
  unsigned bitsize = 0;
  int64_t max = 0;
  if (num <= 2) {
    bitsize = 0;
    return {Status::Ok(), bitsize};
  }
  char delta_out_of_bounds = 0;
  for (uint64_t i = 1; i < num; ++i) {
    int64_t cur_delta = in[i] - in[i - 1];
    max = std::max(std::abs(cur_delta), max);
  }
  // Calculate bitsize of the maximum absolute delta
  do {
    ++(bitsize);
    max >>= 1;
  } while (max);
  return {Status::Ok(), bitsize};
}

// Explicit template instantiations

template Status Delta::compress<char>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<int8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<uint8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<int16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<uint16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<int>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<uint32_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<int64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status Delta::compress<uint64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);

template Status Delta::decompress<char>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<int8_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<uint8_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<int16_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<uint16_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<int>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<uint32_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<int64_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status Delta::decompress<uint64_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);

};  // namespace sm
}  // namespace tiledb
