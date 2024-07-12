/**
 * @file add_1_out_of_place_filter.cc
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

#include "add_1_out_of_place_filter.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb::sm {
// Just use a dummy filter type
Add1OutOfPlace::Add1OutOfPlace(Datatype filter_data_type)
    : Filter(FilterType::FILTER_NONE, filter_data_type) {
}

void Add1OutOfPlace::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto input_size = input->size();
  auto nelts = input_size / sizeof(uint64_t);

  // Add another output buffer.
  throw_if_not_ok(output->prepend_buffer(input_size));
  output->reset_offset();

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

  // Metadata not modified by this filter.
  throw_if_not_ok(output_metadata->append_view(input_metadata));
}

Status Add1OutOfPlace::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const tiledb::sm::Config& config) const {
  (void)config;

  auto input_size = input->size();
  auto nelts = input->size() / sizeof(uint64_t);

  // Add another output buffer.
  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  output->reset_offset();

  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t inc;
    RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
    inc--;
    RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
  }

  auto rem = input_size % sizeof(uint64_t);
  for (unsigned i = 0; i < rem; i++) {
    char byte;
    RETURN_NOT_OK(input->read(&byte, sizeof(char)));
    RETURN_NOT_OK(output->write(&byte, sizeof(char)));
  }

  // Metadata not modified by this filter.
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));

  return Status::Ok();
}

Add1OutOfPlace* Add1OutOfPlace::clone_impl() const {
  return tdb_new(Add1OutOfPlace, filter_data_type_);
}

}  // namespace tiledb::sm
