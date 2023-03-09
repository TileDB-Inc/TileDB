/**
 * @file   dict_compressor.cc
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
 * This file implements the dictionary compression class for strings.
 */

#include "dict_compressor.h"

#include "tiledb/common/common.h"
#include "tiledb/common/logger.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

std::vector<std::string_view> DictEncoding::compress(
    const span<std::string_view> input,
    const uint8_t word_id_size,
    span<std::byte> output) {
  if (input.empty() || output.empty() || word_id_size == 0) {
    throw std::logic_error(
        "Failed compressing strings with dictionary; empty input arguments.");
  }

  if (word_id_size <= 1) {
    return compress<uint8_t>(input, output);
  } else if (word_id_size <= 2) {
    return compress<uint16_t>(input, output);
  } else if (word_id_size <= 4) {
    return compress<uint32_t>(input, output);
  } else {
    return compress<uint64_t>(input, output);
  }
}

void DictEncoding::decompress(
    const span<const std::byte> input,
    const span<const std::string> dict,
    const uint8_t word_id_size,
    span<std::byte> output,
    span<uint64_t> output_offsets) {
  if (input.empty() || word_id_size == 0) {
    throw std::logic_error(
        "Failed decompressing dictionary-encoded strings; empty input "
        "arguments.");
  }

  if (word_id_size <= 1) {
    decompress<uint8_t>(input, dict, output, output_offsets);
  } else if (word_id_size <= 2) {
    decompress<uint16_t>(input, dict, output, output_offsets);
  } else if (word_id_size <= 4) {
    decompress<uint32_t>(input, dict, output, output_offsets);
  } else {
    decompress<uint64_t>(input, dict, output, output_offsets);
  }
}

std::vector<std::byte> DictEncoding::serialize_dictionary(
    const span<std::string_view> dict,
    const size_t strlen_bytesize,
    const size_t dict_size) {
  if (dict.empty() || strlen_bytesize == 0 || dict_size == 0) {
    throw std::logic_error(
        "Failed serializing dictionary when encoding strings; empty input "
        "arguments.");
  }

  if (strlen_bytesize <= 1) {
    return serialize_dictionary<uint8_t>(dict, dict_size);
  } else if (strlen_bytesize <= 2) {
    return serialize_dictionary<uint16_t>(dict, dict_size);
  } else if (strlen_bytesize <= 4) {
    return serialize_dictionary<uint32_t>(dict, dict_size);
  } else {
    return serialize_dictionary<uint64_t>(dict, dict_size);
  }
}

std::vector<std::string> DictEncoding::deserialize_dictionary(
    const span<std::byte> serialized_dict, size_t strlen_bytesize) {
  if (strlen_bytesize <= 1) {
    return deserialize_dictionary<uint8_t>(serialized_dict);
  } else if (strlen_bytesize <= 2) {
    return deserialize_dictionary<uint16_t>(serialized_dict);
  } else if (strlen_bytesize <= 4) {
    return deserialize_dictionary<uint32_t>(serialized_dict);
  } else {
    return deserialize_dictionary<uint64_t>(serialized_dict);
  }
}

}  // namespace sm
}  // namespace tiledb
