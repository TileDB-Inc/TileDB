/**
 * @file add_1_including_metadata_filter.cc
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
 */

#include "add_1_including_metadata_filter.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb::sm {
// Just use a dummy filter type
Add1IncludingMetadataFilter::Add1IncludingMetadataFilter(
    Datatype filter_data_type)
    : Filter(FilterType::FILTER_NONE, filter_data_type) {
}

void Add1IncludingMetadataFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto input_size = static_cast<uint32_t>(input->size()),
       input_md_size = static_cast<uint32_t>(input_metadata->size());
  auto nelts = input_size / sizeof(uint64_t),
       md_nelts = input_md_size / sizeof(uint64_t);

  // Add another output buffer.
  throw_if_not_ok(output->prepend_buffer(input_size + input_md_size));
  output->reset_offset();

  // Filter input data
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t inc;
    throw_if_not_ok(input->read(&inc, sizeof(uint64_t)));
    inc++;
    throw_if_not_ok(output->write(&inc, sizeof(uint64_t)));
  }
  // Finish any remaining bytes to ensure no data loss.
  auto rem = input_size % sizeof(uint64_t);
  for (unsigned i = 0; i < rem; i++) {
    char byte;
    throw_if_not_ok(input->read(&byte, sizeof(char)));
    throw_if_not_ok(output->write(&byte, sizeof(char)));
  }

  // Now filter input metadata.
  for (uint64_t i = 0; i < md_nelts; i++) {
    uint64_t inc;
    throw_if_not_ok(input_metadata->read(&inc, sizeof(uint64_t)));
    inc++;
    throw_if_not_ok(output->write(&inc, sizeof(uint64_t)));
  }
  rem = input_md_size % sizeof(uint64_t);
  for (unsigned i = 0; i < rem; i++) {
    char byte;
    throw_if_not_ok(input_metadata->read(&byte, sizeof(char)));
    throw_if_not_ok(output->write(&byte, sizeof(char)));
  }

  // Because this filter modifies the input metadata, we need output metadata
  // that allows the original metadata to be reconstructed on reverse. Also
  // note that contrary to most filters, we don't forward the input metadata.
  throw_if_not_ok(output_metadata->prepend_buffer(2 * sizeof(uint32_t)));
  throw_if_not_ok(output_metadata->write(&input_size, sizeof(uint32_t)));
  throw_if_not_ok(output_metadata->write(&input_md_size, sizeof(uint32_t)));
}

Status Add1IncludingMetadataFilter::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const tiledb::sm::Config& config) const {
  (void)config;

  if (input_metadata->size() != 2 * sizeof(uint32_t))
    return Status_FilterError("Unexpected input metadata length");

  uint32_t orig_input_size, orig_md_size;
  RETURN_NOT_OK(input_metadata->read(&orig_input_size, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&orig_md_size, sizeof(uint32_t)));

  // Add another output buffer.
  RETURN_NOT_OK(output->prepend_buffer(orig_input_size));
  // Add another output metadata buffer.
  RETURN_NOT_OK(output_metadata->prepend_buffer(orig_md_size));

  // Restore original data
  auto nelts = orig_input_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t inc;
    RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
    inc--;
    RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
  }
  auto rem = orig_input_size % sizeof(uint64_t);
  for (unsigned i = 0; i < rem; i++) {
    char byte;
    RETURN_NOT_OK(input->read(&byte, sizeof(char)));
    RETURN_NOT_OK(output->write(&byte, sizeof(char)));
  }

  // Restore original metadata
  nelts = orig_md_size / sizeof(uint64_t);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t inc;
    RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
    inc--;
    RETURN_NOT_OK(output_metadata->write(&inc, sizeof(uint64_t)));
  }
  rem = orig_md_size % sizeof(uint64_t);
  for (unsigned i = 0; i < rem; i++) {
    char byte;
    RETURN_NOT_OK(input->read(&byte, sizeof(char)));
    RETURN_NOT_OK(output_metadata->write(&byte, sizeof(char)));
  }

  return Status::Ok();
}

Add1IncludingMetadataFilter* Add1IncludingMetadataFilter::clone_impl() const {
  return tdb_new(Add1IncludingMetadataFilter, filter_data_type_);
}

}  // namespace tiledb::sm
