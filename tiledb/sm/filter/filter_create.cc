/**
 * @file filter_create.cc
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
#include "noop_filter.h"
#include "positive_delta_filter.h"
#include "tiledb/common/logger_public.h"
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
    default:
      assert(false);
      return nullptr;
  }
}
Status tiledb::sm::FilterCreate::deserialize(
    ConstBuffer* buff, Filter** filter) {
  uint8_t type;
  RETURN_NOT_OK(buff->read(&type, sizeof(uint8_t)));
  uint32_t filter_metadata_len;
  RETURN_NOT_OK(buff->read(&filter_metadata_len, sizeof(uint32_t)));

  auto* f = make(static_cast<FilterType>(type));
  if (f == nullptr)
    return LOG_STATUS(Status_FilterError("Deserialization error."));

  auto offset = buff->offset();
  RETURN_NOT_OK_ELSE(f->deserialize_impl(buff), tdb_delete(f));

  if (buff->offset() - offset != filter_metadata_len) {
    tdb_delete(f);
    return LOG_STATUS(Status_FilterError(
        "Deserialization error; unexpected metadata length"));
  }

  *filter = f;

  return Status::Ok();
}