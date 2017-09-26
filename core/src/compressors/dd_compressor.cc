/**
 * @file   dd_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "dd_compressor.h"
#include "logger.h"

#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define ABS(x) ((x) < 0) ? uint64_t(-(x)) : uint64_t(x)
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

const uint64_t DoubleDelta::OVERHEAD = 17;

/* ****************************** */
/*               API              */
/* ****************************** */

template <class T>
Status DoubleDelta::compress(ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Calculate number of values and handle trivial case
  uint64_t value_size = sizeof(T);
  uint64_t num = input_buffer->size() / value_size;
  assert(num > 0 && (input_buffer->size() % value_size == 0));

  // Calculate bitsize (ignoring the sign bit)
  auto in = (T*)input_buffer->data();
  int bitsize;
  RETURN_NOT_OK(calculate_bitsize(in, num, &bitsize));
  auto bitsize_c = (char)bitsize;

  // Write bitsize and number of values
  RETURN_NOT_OK(output_buffer->write(&bitsize_c, sizeof(char)));
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
  int64_t cur_delta, dd;
  for (uint64_t i = 2; i < num; ++i) {
    cur_delta = int64_t(in[i]) - int64_t(in[i - 1]);
    dd = cur_delta - prev_delta;
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
Status DoubleDelta::decompress(
    ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Read bitsize and number of values
  char bitsize_c;
  uint64_t num;
  uint64_t value_size = sizeof(T);
  RETURN_NOT_OK(input_buffer->read(&bitsize_c, sizeof(char)));
  RETURN_NOT_OK(input_buffer->read(&num, sizeof(uint64_t)));
  auto bitsize = (int)bitsize_c;
  uint64_t output_offset = output_buffer->offset();

  // Trivial case - no compression
  if (bitsize >= sizeof(T) * 8 - 1) {
    output_buffer->write(input_buffer);
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
  T* out;
  for (uint64_t i = 2; i < num; ++i) {
    RETURN_NOT_OK(
        read_double_delta(input_buffer, &dd, bitsize, &chunk, &bit_in_chunk));
    // The following is very important! Proper start of values for this batch.
    // Also the buffer may be realloced.
    out = (T*)((char*)output_buffer->data() + output_offset);
    value = (T)(dd + 2 * (int64_t)out[i - 1] - (int64_t)out[i - 2]);
    RETURN_NOT_OK(output_buffer->write(&value, value_size));
  }

  return Status::Ok();
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
Status DoubleDelta::calculate_bitsize(T* in, uint64_t num, int* bitsize) {
  // Find maximum absolute double delta
  int64_t max = 0;
  int64_t prev_delta = in[1] - in[0];
  int64_t cur_delta, dd;
  char delta_out_of_bounds = 0;
  for (uint64_t i = 2; i < num; ++i) {
    cur_delta = in[i] - in[i - 1];
    dd = cur_delta - prev_delta;
    delta_out_of_bounds |= (char)(cur_delta < 0 && prev_delta > 0 && dd > 0);
    delta_out_of_bounds |= (char)(cur_delta > 0 && prev_delta < 0 && dd < 0);
    max = MAX(ABS(dd), max);
    prev_delta = cur_delta;
  }

  // Handle error
  if (delta_out_of_bounds) {
    LOG_STATUS(
        Status::CompressionError("Cannot compress with DoubleDelta; Some "
                                 "negative double delta is out of bounds"));
  }

  // Calculate bitsize of the maximum absolute double delta
  *bitsize = 0;
  do {
    ++(*bitsize);
    max >>= 1;
  } while (max);

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
    if (*bit_in_chunk < 0) {
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
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<int8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<uint8_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<int16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<uint16_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<int>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<uint32_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<int64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);
template Status DoubleDelta::decompress<uint64_t>(
    ConstBuffer* input_buffer, Buffer* output_buffer);

};  // namespace tiledb
