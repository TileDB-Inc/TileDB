/**
 * @file   checksum_md5_filter.cc
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
 * This file defines class ChecksumMD5Filter.
 */

#include "tiledb/sm/filter/checksum_md5_filter.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/crypto/crypto.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/tile/tile.h"

#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

ChecksumMD5Filter::ChecksumMD5Filter(Datatype filter_data_type)
    : Filter(FilterType::FILTER_CHECKSUM_MD5, filter_data_type) {
}

ChecksumMD5Filter* ChecksumMD5Filter::clone_impl() const {
  return tdb_new(ChecksumMD5Filter, filter_data_type_);
}

std::ostream& ChecksumMD5Filter::output(std::ostream& os) const {
  os << "ChecksumMD5";
  return os;
}

void ChecksumMD5Filter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Set output buffer to input buffer
  throw_if_not_ok(output->append_view(input));
  // Add original input metadata as a view to the output metadata
  throw_if_not_ok(output_metadata->append_view(input_metadata));

  // Compute and write the metadata
  std::vector<ConstBuffer> data_parts = input->buffers(),
                           metadata_parts = input_metadata->buffers();
  auto num_data_parts = (uint32_t)data_parts.size();
  auto num_metadata_parts = (uint32_t)metadata_parts.size();
  auto total_num_parts = num_data_parts + num_metadata_parts;

  uint32_t part_md_size = Crypto::MD5_DIGEST_BYTES + sizeof(uint64_t);
  uint32_t metadata_size =
      (total_num_parts * part_md_size) + (2 * sizeof(uint32_t));
  throw_if_not_ok(output_metadata->prepend_buffer(metadata_size));
  throw_if_not_ok(
      output_metadata->write(&num_metadata_parts, sizeof(uint32_t)));
  throw_if_not_ok(output_metadata->write(&num_data_parts, sizeof(uint32_t)));

  // Checksum all parts
  for (auto& part : metadata_parts)
    throw_if_not_ok(checksum_part(&part, output_metadata));
  for (auto& part : data_parts)
    throw_if_not_ok(checksum_part(&part, output_metadata));
}

Status ChecksumMD5Filter::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  // Fetch the skip checksum configuration parameter.
  bool found;
  bool skip_validation;
  RETURN_NOT_OK(config.get<bool>(
      "sm.skip_checksum_validation", &skip_validation, &found));
  assert(found);

  // Set output buffer to input buffer
  RETURN_NOT_OK(output->append_view(input));

  // Read the number of parts from input metadata.
  uint32_t num_metadata_parts, num_data_parts;
  RETURN_NOT_OK(input_metadata->read(&num_metadata_parts, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&num_data_parts, sizeof(uint32_t)));

  // Build pairs of checksum to sizes
  std::vector<std::pair<uint64_t, Buffer>> metadata_checksums(
      num_metadata_parts);
  std::vector<std::pair<uint64_t, Buffer>> data_checksums(num_data_parts);

  // First loop through the metadata to pull out the checksums
  for (uint32_t i = 0; i < num_metadata_parts; i++) {
    uint64_t metadata_checksummed_bytes;
    RETURN_NOT_OK(
        input_metadata->read(&metadata_checksummed_bytes, sizeof(uint64_t)));

    // Only fetch and store checksum if we are not going to skip it
    if (!skip_validation) {
      Buffer buff;
      throw_if_not_ok(buff.realloc(Crypto::MD5_DIGEST_BYTES));
      RETURN_NOT_OK(
          input_metadata->read(buff.data(), Crypto::MD5_DIGEST_BYTES));
      metadata_checksums[i] = std::make_pair(metadata_checksummed_bytes, buff);
    } else {
      input_metadata->advance_offset(Crypto::MD5_DIGEST_BYTES);
    }
  }

  for (uint32_t i = 0; i < num_data_parts; i++) {
    uint64_t data_checksummed_bytes;
    RETURN_NOT_OK(
        input_metadata->read(&data_checksummed_bytes, sizeof(uint64_t)));

    // Only fetch and store checksum if we are not going to skip it
    if (!skip_validation) {
      Buffer buff;
      throw_if_not_ok(buff.realloc(Crypto::MD5_DIGEST_BYTES));
      RETURN_NOT_OK(
          input_metadata->read(buff.data(), Crypto::MD5_DIGEST_BYTES));
      data_checksums[i] = std::make_pair(data_checksummed_bytes, buff);
    } else {
      input_metadata->advance_offset(Crypto::MD5_DIGEST_BYTES);
    }
  }

  // Only run checksums if we are not set to skip
  if (!skip_validation) {
    // Now that the checksums are fetched we an run the actual comparisons
    // against the real metadata and data Need to save the metadata offset
    // before we loop through to check it
    uint64_t offset_before_checksum = input_metadata->offset();
    for (uint32_t i = 0; i < num_metadata_parts; i++) {
      auto& checksum_details = metadata_checksums[i];
      RETURN_NOT_OK(compare_checksum_part(
          input_metadata,
          checksum_details.first,
          checksum_details.second.data()));
    }
    // Reset input metadata back to offset only if there was metadata that we
    // read We check this to avoid the edge case where there was not metadata to
    // check and the offset is actually at the end buffer
    if (input_metadata->offset() != offset_before_checksum) {
      input_metadata->set_offset(offset_before_checksum);
    }

    for (uint32_t i = 0; i < num_data_parts; i++) {
      auto& checksum_details = data_checksums[i];
      RETURN_NOT_OK(compare_checksum_part(
          input, checksum_details.first, checksum_details.second.data()));
    }
  }

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

Status ChecksumMD5Filter::checksum_part(
    ConstBuffer* part, FilterBuffer* output_metadata) const {
  // Allocate an initial output buffer.
  tdb_unique_ptr<Buffer> computed_hash =
      tdb_unique_ptr<Buffer>(tdb_new(Buffer));
  throw_if_not_ok(computed_hash->realloc(Crypto::MD5_DIGEST_BYTES));
  RETURN_NOT_OK(Crypto::md5(part, computed_hash.get()));

  // Write metadata.
  uint64_t part_size = part->size();
  RETURN_NOT_OK(output_metadata->write(&part_size, sizeof(uint64_t)));
  RETURN_NOT_OK(output_metadata->write(
      computed_hash->data(), computed_hash->alloced_size()));

  return Status::Ok();
}

Status ChecksumMD5Filter::compare_checksum_part(
    FilterBuffer* part, uint64_t bytes_to_compare, void* checksum) const {
  tdb_unique_ptr<Buffer> byte_buffer_to_compare =
      tdb_unique_ptr<Buffer>(tdb_new(Buffer));
  tdb_unique_ptr<ConstBuffer> buffer_to_compare = tdb_unique_ptr<ConstBuffer>(
      tdb_new(ConstBuffer, byte_buffer_to_compare.get()));

  // First we try to get a view on the bytes we need without copying
  // This might fail if the bytes we need to compare are contained in multiple
  // underlying buffers
  if (!part->get_const_buffer(bytes_to_compare, buffer_to_compare.get()).ok()) {
    // If the bytes we need to compare span multiple buffers we will have to
    // copy them out
    throw_if_not_ok(byte_buffer_to_compare->realloc(bytes_to_compare));
    RETURN_NOT_OK(part->read(byte_buffer_to_compare->data(), bytes_to_compare));
    // Set the buffer back
    buffer_to_compare = tdb_unique_ptr<ConstBuffer>(
        tdb_new(ConstBuffer, byte_buffer_to_compare.get()));
  } else {
    // Move offset location if we used a view so next checksum will read
    // subsequent bytes
    part->advance_offset(bytes_to_compare);
  }

  // Buffer to store the newly computed hash value for comparison
  tdb_unique_ptr<Buffer> computed_hash =
      tdb_unique_ptr<Buffer>(tdb_new(Buffer));
  throw_if_not_ok(computed_hash->realloc(Crypto::MD5_DIGEST_BYTES));

  RETURN_NOT_OK(Crypto::md5(
      buffer_to_compare->data(), bytes_to_compare, computed_hash.get()));

  if (std::memcmp(checksum, computed_hash->data(), Crypto::MD5_DIGEST_BYTES) !=
      0) {
    // If we have a checksum mismatch print hex versions
    unsigned char* digest =
        reinterpret_cast<unsigned char*>(computed_hash->data());
    char md5string[33];
    for (uint64_t i = 0; i < computed_hash->alloced_size(); ++i) {
      snprintf(&md5string[i * 2], 3, "%02x", (unsigned int)digest[i]);
    }

    unsigned char* existing_digest = reinterpret_cast<unsigned char*>(checksum);
    char md5string_existing[33];
    for (uint64_t i = 0; i < Crypto::MD5_DIGEST_BYTES; ++i) {
      snprintf(
          &md5string_existing[i * 2],
          3,
          "%02x",
          (unsigned int)existing_digest[i]);
    }

    std::stringstream message;
    message << "Checksum mismatch for md5 filter, expect ";
    message << md5string_existing;
    message << " got ";
    message << md5string;
    return Status_ChecksumError(message.str());
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
