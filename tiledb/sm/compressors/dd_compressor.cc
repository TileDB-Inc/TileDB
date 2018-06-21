/**
 * @file   dd_compressor.cc
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
 * This file implements the double delta compressor class.
 */

#include "tiledb/sm/compressors/dd_compressor.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define ABS(x) ((x) < 0) ? uint64_t(-(x)) : uint64_t(x)
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {
namespace sm {

const uint64_t DoubleDelta::OVERHEAD = 17;

/* ****************************** */
/*               API              */
/* ****************************** */

Status DoubleDelta::compress(
    Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer) {
  STATS_FUNC_IN(compressor_dd_compress);

  switch (type) {
    case Datatype::INT8:
      return DoubleDelta::compress<int8_t>(input_buffer, output_buffer);
    case Datatype::UINT8:
      return DoubleDelta::compress<uint8_t>(input_buffer, output_buffer);
    case Datatype::INT16:
      return DoubleDelta::compress<int16_t>(input_buffer, output_buffer);
    case Datatype::UINT16:
      return DoubleDelta::compress<uint16_t>(input_buffer, output_buffer);
    case Datatype::INT32:
      return DoubleDelta::compress<int>(input_buffer, output_buffer);
    case Datatype::UINT32:
      return DoubleDelta::compress<uint32_t>(input_buffer, output_buffer);
    case Datatype::INT64:
      return DoubleDelta::compress<int64_t>(input_buffer, output_buffer);
    case Datatype::UINT64:
      return DoubleDelta::compress<uint64_t>(input_buffer, output_buffer);
    case Datatype::CHAR:
      return DoubleDelta::compress<char>(input_buffer, output_buffer);
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      return DoubleDelta::compress<uint8_t>(input_buffer, output_buffer);
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      return LOG_STATUS(
          Status::TileIOError("Cannot compress tile with DoubleDelta; Float "
                              "datatypes are not supported"));
  }

  assert(false);
  return LOG_STATUS(Status::TileIOError(
      "Cannot compress tile with DoubleDelta; Not supported datatype"));

  STATS_FUNC_OUT(compressor_dd_compress);
}

Status DoubleDelta::decompress(
    Datatype type,
    ConstBuffer* input_buffer,
    PreallocatedBuffer* output_buffer) {
  STATS_FUNC_IN(compressor_dd_decompress);

  switch (type) {
    case Datatype::INT8:
      return DoubleDelta::decompress<int8_t>(input_buffer, output_buffer);
    case Datatype::UINT8:
      return DoubleDelta::decompress<uint8_t>(input_buffer, output_buffer);
    case Datatype::INT16:
      return DoubleDelta::decompress<int16_t>(input_buffer, output_buffer);
    case Datatype::UINT16:
      return DoubleDelta::decompress<uint16_t>(input_buffer, output_buffer);
    case Datatype::INT32:
      return DoubleDelta::decompress<int>(input_buffer, output_buffer);
    case Datatype::UINT32:
      return DoubleDelta::decompress<uint32_t>(input_buffer, output_buffer);
    case Datatype::INT64:
      return DoubleDelta::decompress<int64_t>(input_buffer, output_buffer);
    case Datatype::UINT64:
      return DoubleDelta::decompress<uint64_t>(input_buffer, output_buffer);
    case Datatype::CHAR:
      return DoubleDelta::decompress<char>(input_buffer, output_buffer);
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      return DoubleDelta::decompress<uint8_t>(input_buffer, output_buffer);
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
      return LOG_STATUS(
          Status::TileIOError("Cannot decompress tile with DoubleDelta; Float "
                              "datatypes are not supported"));
  }

  assert(false);
  return LOG_STATUS(Status::TileIOError(
      "Cannot decompress tile with DoubleDelta; Not supported datatype"));

  STATS_FUNC_OUT(compressor_dd_decompress);
}

uint64_t DoubleDelta::overhead(uint64_t nbytes) {
  // DoubleDelta has a fixed size overhead
  (void)nbytes;
  return DoubleDelta::OVERHEAD;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
Status DoubleDelta::compress(ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Calculate number of values and handle trivial case
  uint64_t value_size = sizeof(T);
  uint64_t num = input_buffer->size() / value_size;
  assert(num > 0 && (input_buffer->size() % value_size == 0));

  // Calculate bitsize (ignoring the sign bit)
  auto in = (T*)input_buffer->data();
  unsigned int bitsize;
  RETURN_NOT_OK(compute_bitsize(in, num, &bitsize));
  assert(bitsize <= std::numeric_limits<uint8_t>::max());
  auto bitsize_c = static_cast<uint8_t>(bitsize);

  // Write bitsize and number of values
  RETURN_NOT_OK(output_buffer->write(&bitsize_c, sizeof(uint8_t)));
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

  // Write second value
  RETURN_NOT_OK(output_buffer->write(&in[1], value_size));
  if (num == 2)
    return Status::Ok();

  // Write double deltas
  int64_t prev_delta = int64_t(in[1]) - int64_t(in[0]);
  int bit_in_chunk = 63;  // Leftmost bit (MSB)
  uint64_t chunk = 0;
  for (uint64_t i = 2; i < num; ++i) {
    int64_t cur_delta = int64_t(in[i]) - int64_t(in[i - 1]);
    int64_t dd = cur_delta - prev_delta;
    RETURN_NOT_OK(
        write_double_delta(output_buffer, dd, bitsize, &chunk, &bit_in_chunk));
    prev_delta = cur_delta;
  }

  // Write whatever is left in the chunk
  if (bit_in_chunk < 63)
    RETURN_NOT_OK(output_buffer->write(&chunk, sizeof(uint64_t)));

  return Status::Ok();
}

template <class T>
Status DoubleDelta::compute_bitsize(
    T* in, uint64_t num, unsigned int* bitsize) {
  // Find maximum absolute double delta
  *bitsize = 0;
  int64_t max = 0;
  if (num <= 2) {
    *bitsize = 0;
    return Status::Ok();
  }
  int64_t prev_delta = int64_t(in[1]) - int64_t(in[0]);
  char delta_out_of_bounds = 0;
  for (uint64_t i = 2; i < num; ++i) {
    int64_t cur_delta = int64_t(in[i]) - int64_t(in[i - 1]);
    int64_t dd = cur_delta - prev_delta;
    delta_out_of_bounds |= (char)(cur_delta < 0 && prev_delta > 0 && dd > 0);
    delta_out_of_bounds |= (char)(cur_delta > 0 && prev_delta < 0 && dd < 0);
    max = MAX(std::abs(dd), max);
    prev_delta = cur_delta;
  }
  // Handle error
  if (delta_out_of_bounds) {
    return LOG_STATUS(
        Status::CompressionError("Cannot compress with DoubleDelta; Some "
                                 "negative double delta is out of bounds"));
  }
  // Calculate bitsize of the maximum absolute double delta
  do {
    ++(*bitsize);
    max >>= 1;
  } while (max);
  return Status::Ok();
}

template <class T>
Status DoubleDelta::decompress(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  // Read bitsize and number of values
  uint8_t bitsize_c = 0;
  uint64_t num = 0;
  uint64_t value_size = sizeof(T);
  RETURN_NOT_OK(input_buffer->read(&bitsize_c, sizeof(uint8_t)));
  RETURN_NOT_OK(input_buffer->read(&num, sizeof(uint64_t)));
  auto bitsize = static_cast<unsigned int>(bitsize_c);
  auto out = (T*)output_buffer->cur_data();

  // Trivial case - no compression
  if (bitsize >= sizeof(T) * 8 - 1) {
    RETURN_NOT_OK(output_buffer->write(
        input_buffer, input_buffer->nbytes_left_to_read()));
    return Status::Ok();
  }

  // Read first value
  T value;
  RETURN_NOT_OK(input_buffer->read(&value, value_size));
  RETURN_NOT_OK(output_buffer->write(&value, value_size));
  if (num == 1)
    return Status::Ok();

  // Read second value
  RETURN_NOT_OK(input_buffer->read(&value, value_size));
  RETURN_NOT_OK(output_buffer->write(&value, value_size));
  if (num == 2)
    return Status::Ok();

  // Read first chunk
  uint64_t chunk;
  RETURN_NOT_OK(input_buffer->read(&chunk, sizeof(uint64_t)));
  int bit_in_chunk = 63;  // Leftmost bit (MSB)

  // Decompress rest of the values
  int64_t dd;
  for (uint64_t i = 2; i < num; ++i) {
    RETURN_NOT_OK(
        read_double_delta(input_buffer, &dd, bitsize, &chunk, &bit_in_chunk));
    value = (T)(dd + 2 * (int64_t)out[i - 1] - (int64_t)out[i - 2]);
    RETURN_NOT_OK(output_buffer->write(&value, value_size));
  }

  return Status::Ok();
}

Status DoubleDelta::read_double_delta(
    ConstBuffer* buff,
    int64_t* double_delta,
    int bitsize,
    uint64_t* chunk,
    int* bit_in_chunk) {
  // Read sign
  uint64_t to_and = 1;
  to_and <<= (*bit_in_chunk);
  int sign = (((*chunk) & to_and) == 0) ? 1 : -1;
  --(*bit_in_chunk);

  // Read chunk and reset
  if (*bit_in_chunk < 0) {
    RETURN_NOT_OK(buff->read(chunk, sizeof(uint64_t)));
    *bit_in_chunk = 63;
  }

  // Read double delta
  int bits_left_to_read = bitsize;
  int bits_to_read_from_chunk = MIN(*bit_in_chunk + 1, bits_left_to_read);
  uint64_t tmp_chunk;
  int bit_in_dd = bitsize - 1;
  *double_delta = 0;
  while (bits_left_to_read > 0) {
    // Read from chunk
    if (bits_to_read_from_chunk > 0) {
      tmp_chunk = (((*chunk) << (63 - *bit_in_chunk)) >> (63 - bit_in_dd));
      (*double_delta) |= tmp_chunk;
      bit_in_dd -= bits_to_read_from_chunk;
      (*bit_in_chunk) -= bits_to_read_from_chunk;
      bits_left_to_read -= bits_to_read_from_chunk;
    }

    // Read chunk and reset
    if (*bit_in_chunk < 0 && buff->offset() != buff->size()) {
      RETURN_NOT_OK(buff->read(chunk, sizeof(uint64_t)));
      *bit_in_chunk = 63;
      bits_to_read_from_chunk = MIN(*bit_in_chunk + 1, bits_left_to_read);
    }
  }

  // Set sign
  (*double_delta) *= sign;

  return Status::Ok();
}

Status DoubleDelta::write_double_delta(
    Buffer* buff,
    int64_t double_delta,
    int bitsize,
    uint64_t* chunk,
    int* bit_in_chunk) {
  // Write sign
  uint64_t to_or = (double_delta < 0) ? 1 : 0;
  to_or <<= (*bit_in_chunk);
  (*chunk) |= to_or;
  --(*bit_in_chunk);

  // Write chunk and reset
  if (*bit_in_chunk < 0) {
    RETURN_NOT_OK(buff->write(chunk, sizeof(uint64_t)));
    *bit_in_chunk = 63;
    *chunk = 0;
  }

  // Write rest of bits
  int bits_left_to_write = bitsize;
  int bit_in_dd = bitsize - 1;
  int bits_to_fill_in_chunk = MIN((*bit_in_chunk) + 1, bits_left_to_write);
  uint64_t abs_dd = ABS(double_delta);
  uint64_t tmp_abs_dd;

  while (bits_left_to_write > 0) {
    if (bits_to_fill_in_chunk > 0) {
      tmp_abs_dd = (abs_dd << (63 - bit_in_dd));
      tmp_abs_dd >>= (63 - (*bit_in_chunk));
      (*chunk) |= tmp_abs_dd;
      (*bit_in_chunk) -= bits_to_fill_in_chunk;
      bit_in_dd -= bits_to_fill_in_chunk;
      bits_left_to_write -= bits_to_fill_in_chunk;
    }

    // Write chunk and reset
    if (*bit_in_chunk < 0) {
      RETURN_NOT_OK(buff->write(chunk, sizeof(uint64_t)));
      *bit_in_chunk = 63;
      *chunk = 0;
      bits_to_fill_in_chunk = MIN((*bit_in_chunk) + 1, bits_left_to_write);
    }
  }

  return Status::Ok();
}

// Explicit template instantiations

template Status DoubleDelta::compress<char>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<int8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<uint8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<int16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<uint16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<int>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<uint32_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<int64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::compress<uint64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);

template Status DoubleDelta::decompress<char>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<int8_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<uint8_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<int16_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<uint16_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<int>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<uint32_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<int64_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);
template Status DoubleDelta::decompress<uint64_t>(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer);

};  // namespace sm
}  // namespace tiledb
