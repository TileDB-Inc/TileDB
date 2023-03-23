/**
 * @file categorical_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines class CategoricalFilter.
 */

#include <algorithm>
#include <sstream>

#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filter/categorical_filter.h"
#include "tiledb/sm/misc/endian.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

void CategoricalFilter::dump(FILE* out) const {
  if (out == nullptr) {
    out = stdout;
  }

  std::stringstream ss;
  ss << "Categorical(num_categories=" << std::to_string(categories_.size());
  auto num_to_print = categories_.size() <= 10 ? categories_.size() : 10;
  for (uint64_t i = 0; i < num_to_print; i++) {
    ss << ", " << categories_[i];
  }
  if (categories_.size() > 10) {
    ss << ", ...";
  }
  ss << ")";

  fprintf(out, "%s", ss.str().c_str());
}

Status CategoricalFilter::run_forward(
    const WriterTile&,
    WriterTile* const offsets_tile,
    FilterBuffer*,
    FilterBuffer* input,
    FilterBuffer*,
    FilterBuffer* output) const {
  if (input->num_buffers() != 1) {
    throw std::logic_error(
        "Var-sized string input has to be in single "
        "buffer format to be compressed with a categorical filter");
  }

  auto data = static_cast<const char*>(input->buffers()[0].data());
  auto offsets = static_cast<uint64_t*>(offsets_tile->data());
  auto num_offsets = offsets_tile->size() / constants::cell_var_offset_size;

  // Allocate our output buffer
  auto output_size = (num_offsets + 1) * sizeof(uint64_t);
  throw_if_not_ok(output->prepend_buffer(output_size));
  auto outptr = output->value_ptr<uint8_t>();

  utils::endianness::encode_be<uint64_t>(num_offsets, outptr);
  uint64_t offset = sizeof(uint64_t);

  for (uint64_t i = 0; i < num_offsets; i++) {
    uint64_t len = 0;
    if (i < num_offsets - 1) {
      len = offsets[i + 1] - offsets[i];
    } else {
      len = input->buffers()[0].size() - offsets[i];
    }

    std::string word(data + offsets[i], len);

    auto word_id = 0;
    auto iter = category_ids_.find(word);
    if (iter != category_ids_.end()) {
      word_id = iter->second;
    }
    utils::endianness::encode_be<uint64_t>(word_id, outptr + offset);
    offset += sizeof(uint64_t);
  }

  output->buffer_ptr(0)->advance_size(output_size);

  return Status::Ok();
}

Status CategoricalFilter::run_reverse(
    const Tile&,
    Tile* offsets_tile,
    FilterBuffer*,
    FilterBuffer* input,
    FilterBuffer*,
    FilterBuffer* output,
    const Config&) const {
  if (input->num_buffers() != 1) {
    throw std::logic_error(
        "Var-sized string input has to be in single "
        "buffer format to be decompressed a categorical filter.");
  }

  auto data = static_cast<const uint8_t*>(input->buffers()[0].data());

  // Calculate the output size
  uint64_t output_size = 0;
  auto num_words = utils::endianness::decode_be<uint64_t>(data);
  uint64_t offset = sizeof(uint64_t);
  for (uint64_t i = 0; i < num_words; i++) {
    auto word_id = utils::endianness::decode_be<uint64_t>(data + offset);
    offset += sizeof(uint64_t);

    if (word_id > 0 && (word_id - 1) < categories_.size()) {
      output_size += categories_[word_id - 1].size();
    }
  }

  throw_if_not_ok(output->prepend_buffer(output_size));
  auto outptr = output->value_ptr<uint8_t>();
  auto offsets = span<uint64_t>(
      reinterpret_cast<std::uint64_t*>(offsets_tile->data()),
      num_words * sizeof(uint64_t));
  uint64_t out_offset = 0;

  // Decode categories into our buffers
  offset = sizeof(uint64_t);
  for (uint64_t i = 0; i < num_words; i++) {
    auto word_id = utils::endianness::decode_be<uint64_t>(data + offset);
    offset += sizeof(uint64_t);

    if (word_id > 0 && (word_id - 1) < categories_.size()) {
      std::memcpy(
          outptr + out_offset,
          categories_[word_id - 1].data(),
          categories_[word_id - 1].size());
      offsets[i] = out_offset;
      out_offset += categories_[word_id - 1].size();
    } else {
      // "appending" an empty string
      offsets[i] = out_offset;
    }
  }

  input->advance_offset((num_words + 1) * sizeof(uint64_t));
  output->advance_offset(output_size);

  return Status::Ok();
}

Status CategoricalFilter::set_option_impl(
    FilterOption option, const void* value) {
  switch (option) {
    case FilterOption::CATEGORIES: {
      buffer_to_categories(value);
      break;
    }
    case FilterOption::CATEGORY_BUFFER_LENGTH:
      throw std::logic_error("Categorical filter buffer lenght is read-only.");
    default:
      throw StatusException(
          Status_FilterError("Categorical filter error; Unknown option"));
  }

  return Status::Ok();
}

Status CategoricalFilter::get_option_impl(
    FilterOption option, void* value) const {
  auto length = calculate_buffer_length();
  uint8_t buffer[length];
  std::memset(buffer, 0, length);

  switch (option) {
    case FilterOption::CATEGORIES:
      categories_to_buffer(buffer);
      std::memcpy(value, buffer, length);
      break;
    case FilterOption::CATEGORY_BUFFER_LENGTH:
      *(uint64_t*)value = length;
      break;
    default:
      throw StatusException(
          Status_FilterError("Categorical filter error; Unknown option"));
  }

  return Status::Ok();
}

CategoricalFilter* CategoricalFilter::clone_impl() const {
  auto length = calculate_buffer_length();
  uint8_t buffer[length];
  categories_to_buffer(buffer);

  return tdb_new(CategoricalFilter, static_cast<void*>(buffer));
}

void CategoricalFilter::serialize_impl(Serializer& serializer) const {
  auto length = calculate_buffer_length();
  uint8_t buffer[length];

  categories_to_buffer(buffer);
  serializer.write(buffer, length);
}

uint64_t CategoricalFilter::calculate_buffer_length() const {
  uint64_t len = (categories_.size() + 1) * sizeof(uint64_t);
  for (auto category : categories_) {
    len += category.size();
  }
  return len;
}

void CategoricalFilter::categories_to_buffer(void* buffer) const {
  auto data = static_cast<uint8_t*>(buffer);
  utils::endianness::encode_be<uint64_t>(categories_.size(), data);
  uint64_t offset = sizeof(uint64_t);

  for (auto category : categories_) {
    utils::endianness::encode_be<uint64_t>(category.size(), data + offset);
    offset += sizeof(uint64_t);

    std::memcpy(data + offset, category.data(), category.size());
    offset += category.size();
  }
}

void CategoricalFilter::buffer_to_categories(const void* buffer) {
  categories_.clear();
  category_ids_.clear();

  if (buffer == nullptr) {
    return;
  }

  auto data = static_cast<const char*>(buffer);

  auto num_words = utils::endianness::decode_be<uint64_t>(data);
  uint64_t offset = sizeof(uint64_t);

  categories_.clear();
  category_ids_.clear();
  uint64_t category_id = 1;

  for (uint64_t i = 0; i < num_words; i++) {
    auto word_len = utils::endianness::decode_be<uint64_t>(data + offset);
    offset += sizeof(uint64_t);

    categories_.emplace_back(data + offset, word_len);
    category_ids_[categories_.back()] = category_id++;
    offset += word_len;
  }
}

}  // namespace sm
}  // namespace tiledb
