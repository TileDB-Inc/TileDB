/**
 * @file filter_create.cc
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
 * This file defines class FilterCreate, which contains the factory for the C
 * API and the deserialize function.
 */

#include "filter_create.h"
#include "bit_width_reduction_filter.h"
#include "bitshuffle_filter.h"
#include "byteshuffle_filter.h"
#include "checksum_md5_filter.h"
#include "checksum_sha256_filter.h"
#include "compression_filter.h"
#include "encryption_aes256gcm_filter.h"
#include "filter.h"
#include "float_scaling_filter.h"
#include "noop_filter.h"
#include "positive_delta_filter.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_type.h"

tiledb::sm::Filter* tiledb::sm::FilterCreate::make(FilterType type) {
  switch (type) {
    case tiledb::sm::FilterType::FILTER_NONE:
      return tdb_new(tiledb::sm::NoopFilter);
    case tiledb::sm::FilterType::FILTER_GZIP:
    case tiledb::sm::FilterType::FILTER_ZSTD:
    case tiledb::sm::FilterType::FILTER_LZ4:
    case tiledb::sm::FilterType::FILTER_RLE:
    case tiledb::sm::FilterType::FILTER_BZIP2:
    case tiledb::sm::FilterType::FILTER_DOUBLE_DELTA:
    case tiledb::sm::FilterType::FILTER_DICTIONARY:
      return tdb_new(tiledb::sm::CompressionFilter, type, -1);
    case tiledb::sm::FilterType::FILTER_BIT_WIDTH_REDUCTION:
      return tdb_new(tiledb::sm::BitWidthReductionFilter);
    case tiledb::sm::FilterType::FILTER_BITSHUFFLE:
      return tdb_new(tiledb::sm::BitshuffleFilter);
    case tiledb::sm::FilterType::FILTER_BYTESHUFFLE:
      return tdb_new(tiledb::sm::ByteshuffleFilter);
    case tiledb::sm::FilterType::FILTER_POSITIVE_DELTA:
      return tdb_new(tiledb::sm::PositiveDeltaFilter);
    case tiledb::sm::FilterType::INTERNAL_FILTER_AES_256_GCM:
      return tdb_new(tiledb::sm::EncryptionAES256GCMFilter);
    case tiledb::sm::FilterType::FILTER_CHECKSUM_MD5:
      return tdb_new(tiledb::sm::ChecksumMD5Filter);
    case tiledb::sm::FilterType::FILTER_CHECKSUM_SHA256:
      return tdb_new(tiledb::sm::ChecksumSHA256Filter);
    case tiledb::sm::FilterType::FILTER_SCALE_FLOAT:
      return tdb_new(tiledb::sm::FloatScalingFilter);
    default:
      assert(false);
      return nullptr;
  }
}

tuple<Status, optional<shared_ptr<tiledb::sm::Filter>>>
tiledb::sm::FilterCreate::deserialize(
    ConstBuffer* buff,
    const EncryptionKey& encryption_key,
    const uint32_t version) {
  Status st;
  uint8_t type;
  st = buff->read(&type, sizeof(uint8_t));
  if (!st.ok()) {
    return {st, nullopt};
  }
  FilterType filtertype = static_cast<FilterType>(type);
  uint32_t filter_metadata_len;
  st = buff->read(&filter_metadata_len, sizeof(uint32_t));
  if (!st.ok()) {
    return {st, nullopt};
  }

  auto offset = buff->offset();
  if ((buff->size() - offset) < filter_metadata_len) {
    return {
        Status_FilterError(
            "Deserialization error; not enough data in buffer for metadata"),
        nullopt};
  }
  switch (filtertype) {
    case FilterType::FILTER_NONE:
      return {Status::Ok(), make_shared<NoopFilter>(HERE())};
    case FilterType::FILTER_GZIP:
    case FilterType::FILTER_ZSTD:
    case FilterType::FILTER_LZ4:
    case FilterType::FILTER_RLE:
    case FilterType::FILTER_BZIP2:
    case FilterType::FILTER_DOUBLE_DELTA:
    case FilterType::FILTER_DICTIONARY: {
      uint8_t compressor_char;
      int compression_level;
      st = (buff->read(&compressor_char, sizeof(uint8_t)));
      if (!st.ok()) {
        return {st, nullopt};
      }
      Compressor compressor = static_cast<Compressor>(compressor_char);
      st = buff->read(&compression_level, sizeof(int32_t));
      if (!st.ok()) {
        return {st, nullopt};
      }
      return {Status::Ok(),
              make_shared<CompressionFilter>(
                  HERE(), compressor, compression_level, version)};
    }
    case FilterType::FILTER_BIT_WIDTH_REDUCTION: {
      uint32_t max_window_size;
      st = buff->read(&max_window_size, sizeof(uint32_t));
      if (!st.ok()) {
        return {st, nullopt};
      }
      return {Status::Ok(),
              make_shared<BitWidthReductionFilter>(HERE(), max_window_size)};
    }
    case FilterType::FILTER_BITSHUFFLE:
      return {Status::Ok(), make_shared<BitshuffleFilter>(HERE())};
    case FilterType::FILTER_BYTESHUFFLE:
      return {Status::Ok(), make_shared<ByteshuffleFilter>(HERE())};
    case FilterType::FILTER_POSITIVE_DELTA: {
      uint32_t max_window_size;
      st = buff->read(&max_window_size, sizeof(uint32_t));
      if (!st.ok()) {
        return {st, nullopt};
      } else {
        return {Status::Ok(),
                make_shared<PositiveDeltaFilter>(HERE(), max_window_size)};
      }
    }
    case FilterType::INTERNAL_FILTER_AES_256_GCM:
      if (encryption_key.encryption_type() ==
          tiledb::sm::EncryptionType::AES_256_GCM) {
        return {Status::Ok(),
                make_shared<EncryptionAES256GCMFilter>(HERE(), encryption_key)};
      } else {
        return {Status::Ok(), make_shared<EncryptionAES256GCMFilter>(HERE())};
      }
    case FilterType::FILTER_CHECKSUM_MD5:
      return {Status::Ok(), make_shared<ChecksumMD5Filter>(HERE())};
    case FilterType::FILTER_CHECKSUM_SHA256:
      return {Status::Ok(), make_shared<ChecksumSHA256Filter>(HERE())};
    case FilterType::FILTER_SCALE_FLOAT: {
      FloatScalingFilter::Metadata metadata;
      st = buff->read(&metadata, sizeof(FloatScalingFilter::Metadata));
      if (!st.ok()) {
        return {st, nullopt};
      } else {
        return {Status::Ok(),
                make_shared<FloatScalingFilter>(
                    HERE(), metadata.b, metadata.s, metadata.o)};
      }
    };
    default:
      assert(false);
      return {Status_FilterError("Deserialization error; unknown type"),
              nullopt};
  }
}

tuple<Status, optional<shared_ptr<tiledb::sm::Filter>>>
tiledb::sm::FilterCreate::deserialize(
    ConstBuffer* buff, const uint32_t version) {
  EncryptionKey encryption_key;
  return tiledb::sm::FilterCreate::deserialize(buff, encryption_key, version);
}