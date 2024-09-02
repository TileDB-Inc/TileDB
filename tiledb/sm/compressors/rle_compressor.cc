/**
 * @file   rle_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements the rle compressor class.
 */

#include "rle_compressor.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"

#include <cassert>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class RLEException : public StatusException {
 public:
  explicit RLEException(const std::string& message)
      : StatusException("RLEException", message) {
  }
};

void RLE::compress(
    uint64_t value_size, ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr)
    throw RLEException("Failed compressing with RLE; null input buffer");
  unsigned int cur_run_len = 1;
  unsigned int max_run_len = 65535;
  auto input_cur = (const unsigned char*)input_buffer->data() + value_size;
  auto input_prev = (const unsigned char*)input_buffer->data();
  uint64_t value_num = input_buffer->size() / value_size;
  unsigned char byte;

  // Trivial case
  if (value_num == 0)
    return;

  // Sanity check on input buffer
  if (input_buffer->size() % value_size != 0) {
    throw RLEException(
        "Failed compressing with RLE; invalid input buffer format");
  }

  // Make runs
  for (uint64_t i = 1; i < value_num; ++i) {
    if (std::memcmp(input_cur, input_prev, value_size) == 0 &&
        cur_run_len < max_run_len) {  // Expand the run
      ++cur_run_len;
    } else {  // Save the run
      // Copy to output buffer
      throw_if_not_ok(output_buffer->write(input_prev, value_size));
      byte = (unsigned char)(cur_run_len >> 8);
      throw_if_not_ok(output_buffer->write(&byte, sizeof(char)));
      byte = (unsigned char)(cur_run_len % 256);
      throw_if_not_ok(output_buffer->write(&byte, sizeof(char)));

      // Reset current run length
      cur_run_len = 1;
    }

    // Update run info
    input_prev = input_cur;
    input_cur = input_prev + value_size;
  }

  // Save final run
  throw_if_not_ok(output_buffer->write(input_prev, value_size));
  byte = (unsigned char)(cur_run_len >> 8);
  throw_if_not_ok(output_buffer->write(&byte, sizeof(char)));
  byte = (unsigned char)(cur_run_len % 256);
  throw_if_not_ok(output_buffer->write(&byte, sizeof(char)));
}

void RLE::decompress(
    uint64_t value_size,
    ConstBuffer* input_buffer,
    PreallocatedBuffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr)
    throw RLEException("Failed decompressing with RLE; null input buffer");

  auto input_cur = static_cast<const unsigned char*>(input_buffer->data());
  uint64_t run_size = value_size + 2 * sizeof(char);
  uint64_t run_num = input_buffer->size() / run_size;
  unsigned char byte;

  // Trivial case
  if (run_num == 0)
    return;

  // Sanity check on input buffer format
  if (input_buffer->size() % run_size != 0) {
    throw RLEException(
        "Failed decompressing with RLE; invalid input buffer format");
  }

  // Decompress runs
  for (uint64_t i = 0; i < run_num; ++i) {
    // Retrieve the current run length
    std::memcpy(&byte, input_cur + value_size, sizeof(char));
    uint64_t run_len = (((uint64_t)byte) << 8);
    std::memcpy(&byte, input_cur + value_size + sizeof(char), sizeof(char));
    run_len += (uint64_t)byte;

    // Copy to output buffer
    for (uint64_t j = 0; j < run_len; ++j)
      throw_if_not_ok(output_buffer->write(input_cur, value_size));

    // Update input/output tracking info
    input_cur += run_size;
  }
}

uint64_t RLE::overhead(uint64_t nbytes, uint64_t value_size) {
  // In the worst case, RLE adds two bytes per every value in the buffer.
  uint64_t value_num = nbytes / value_size;
  return value_num * 2;
}

uint8_t RLE::compute_bytesize(uint64_t param_length) {
  if (param_length <= std::numeric_limits<uint8_t>::max()) {
    return 1;
  } else if (param_length <= std::numeric_limits<uint16_t>::max()) {
    return 2;
  } else if (param_length <= std::numeric_limits<uint32_t>::max()) {
    return 4;
  } else {
    return 8;
  }
}

tuple<uint64_t, uint64_t, uint64_t, uint64_t> RLE::calculate_compression_params(
    const span<std::string_view> input) {
  if (input.empty())
    return {0, 0, 0, 0};

  uint64_t max_identicals = 1;
  uint64_t identicals = 1;
  uint64_t output_strings_size = 0;
  uint64_t num_of_runs = 1;
  uint64_t max_string_size = input[0].size();
  auto previous = input[0];
  for (uint64_t i = 1; i < input.size(); i++) {
    if (input[i] == previous) {
      identicals++;
    } else {
      max_identicals = std::max(identicals, max_identicals);
      max_string_size =
          std::max(max_string_size, static_cast<uint64_t>(previous.size()));
      output_strings_size += previous.size();
      num_of_runs++;
      identicals = 1;
      previous = input[i];
    }
  }

  // take into account the last string
  output_strings_size += previous.size();
  max_identicals = std::max(identicals, max_identicals);

  return {
      compute_bytesize(max_identicals),
      compute_bytesize(max_string_size),
      num_of_runs,
      output_strings_size};
}

void RLE::compress(
    const span<std::string_view> input,
    uint64_t rle_len_size,
    uint64_t string_len_size,
    span<std::byte> output) {
  if (input.empty() || output.empty() || rle_len_size == 0 ||
      string_len_size == 0) {
    throw RLEException(
        "Failed compressing strings with RLE; empty input arguments");
  }

  if (rle_len_size <= 1) {
    if (string_len_size <= 1) {
      compress<uint8_t, uint8_t>(input, output);
    } else if (string_len_size <= 2) {
      compress<uint8_t, uint16_t>(input, output);
    } else if (string_len_size <= 4) {
      compress<uint8_t, uint32_t>(input, output);
    } else {
      compress<uint8_t, uint64_t>(input, output);
    }
  } else if (rle_len_size <= 2) {
    if (string_len_size <= 1) {
      compress<uint16_t, uint8_t>(input, output);
    } else if (string_len_size <= 2) {
      compress<uint16_t, uint16_t>(input, output);
    } else if (string_len_size <= 4) {
      compress<uint16_t, uint32_t>(input, output);
    } else {
      compress<uint16_t, uint64_t>(input, output);
    }
  } else if (rle_len_size <= 4) {
    if (string_len_size <= 1) {
      compress<uint32_t, uint8_t>(input, output);
    } else if (string_len_size <= 2) {
      compress<uint32_t, uint16_t>(input, output);
    } else if (string_len_size <= 4) {
      compress<uint32_t, uint32_t>(input, output);
    } else {
      compress<uint32_t, uint64_t>(input, output);
    }
  } else {
    if (string_len_size <= 1) {
      compress<uint64_t, uint8_t>(input, output);
    } else if (string_len_size <= 2) {
      compress<uint64_t, uint16_t>(input, output);
    } else if (string_len_size <= 4) {
      compress<uint64_t, uint32_t>(input, output);
    } else {
      compress<uint64_t, uint64_t>(input, output);
    }
  }
}

void RLE::decompress(
    const span<const std::byte> input,
    const uint8_t rle_len_size,
    const uint8_t string_len_size,
    span<std::byte> output,
    span<uint64_t> output_offsets) {
  if (input.empty() || rle_len_size == 0 || string_len_size == 0) {
    throw RLEException(
        "Failed decompressing strings with RLE; empty input arguments");
  }

  if (rle_len_size <= 1) {
    if (string_len_size <= 1) {
      decompress<uint8_t, uint8_t>(input, output, output_offsets);
    } else if (string_len_size <= 2) {
      decompress<uint8_t, uint16_t>(input, output, output_offsets);
    } else if (string_len_size <= 4) {
      decompress<uint8_t, uint32_t>(input, output, output_offsets);
    } else {
      decompress<uint8_t, uint64_t>(input, output, output_offsets);
    }
  } else if (rle_len_size <= 2) {
    if (string_len_size <= 1) {
      decompress<uint16_t, uint8_t>(input, output, output_offsets);
    } else if (string_len_size <= 2) {
      decompress<uint16_t, uint16_t>(input, output, output_offsets);
    } else if (string_len_size <= 4) {
      decompress<uint16_t, uint32_t>(input, output, output_offsets);
    } else {
      decompress<uint16_t, uint64_t>(input, output, output_offsets);
    }
  } else if (rle_len_size <= 4) {
    if (string_len_size <= 1) {
      decompress<uint32_t, uint8_t>(input, output, output_offsets);
    } else if (string_len_size <= 2) {
      decompress<uint32_t, uint16_t>(input, output, output_offsets);
    } else if (string_len_size <= 4) {
      decompress<uint32_t, uint32_t>(input, output, output_offsets);
    } else {
      decompress<uint32_t, uint64_t>(input, output, output_offsets);
    }
  } else {
    if (string_len_size <= 1) {
      decompress<uint64_t, uint8_t>(input, output, output_offsets);
    } else if (string_len_size <= 2) {
      decompress<uint64_t, uint16_t>(input, output, output_offsets);
    } else if (string_len_size <= 4) {
      decompress<uint64_t, uint32_t>(input, output, output_offsets);
    } else {
      decompress<uint64_t, uint64_t>(input, output, output_offsets);
    }
  }
}

}  // namespace sm
}  // namespace tiledb
