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
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/stdx/utility/to_underlying.h"
#include "xor_filter.h"

tiledb::sm::Filter* tiledb::sm::FilterCreate::make(FilterType type) {
  switch (type) {
    case tiledb::sm::FilterType::FILTER_NONE:
      return tdb_new(tiledb::sm::NoopFilter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_GZIP:
    case tiledb::sm::FilterType::FILTER_ZSTD:
    case tiledb::sm::FilterType::FILTER_LZ4:
    case tiledb::sm::FilterType::FILTER_RLE:
    case tiledb::sm::FilterType::FILTER_BZIP2:
    case tiledb::sm::FilterType::FILTER_DELTA:
    case tiledb::sm::FilterType::FILTER_DOUBLE_DELTA:
    case tiledb::sm::FilterType::FILTER_DICTIONARY:
      return tdb_new(tiledb::sm::CompressionFilter, type, -1, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_BIT_WIDTH_REDUCTION:
      return tdb_new(tiledb::sm::BitWidthReductionFilter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_BITSHUFFLE:
      return tdb_new(tiledb::sm::BitshuffleFilter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_BYTESHUFFLE:
      return tdb_new(tiledb::sm::ByteshuffleFilter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_POSITIVE_DELTA:
      return tdb_new(tiledb::sm::PositiveDeltaFilter, Datatype::ANY);
    case tiledb::sm::FilterType::INTERNAL_FILTER_AES_256_GCM:
      return tdb_new(tiledb::sm::EncryptionAES256GCMFilter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_CHECKSUM_MD5:
      return tdb_new(tiledb::sm::ChecksumMD5Filter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_CHECKSUM_SHA256:
      return tdb_new(tiledb::sm::ChecksumSHA256Filter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_SCALE_FLOAT:
      return tdb_new(tiledb::sm::FloatScalingFilter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_XOR:
      return tdb_new(tiledb::sm::XORFilter, Datatype::ANY);
    case tiledb::sm::FilterType::FILTER_WEBP: {
      if constexpr (webp_filter_exists) {
        return tdb_new(tiledb::sm::WebpFilter, Datatype::ANY);
      } else {
        throw WebpNotPresentError();
      }
    }
    default:
      throw StatusException(
          "FilterCreate",
          "Invalid filter type " + std::to_string(stdx::to_underlying(type)));
  }
}

shared_ptr<tiledb::sm::Filter> tiledb::sm::FilterCreate::deserialize(
    Deserializer& deserializer,
    const EncryptionKey& encryption_key,
    const uint32_t version,
    Datatype datatype) {
  Status st;
  uint8_t type = deserializer.read<uint8_t>();
  FilterType filtertype = static_cast<FilterType>(type);
  uint32_t filter_metadata_len = deserializer.read<uint32_t>();
  if (deserializer.size() < filter_metadata_len) {
    throw StatusException(
        "FilterCreate",
        "Deserialization error; not enough data in buffer for metadata");
  }

  switch (filtertype) {
    case FilterType::FILTER_NONE:
      return make_shared<NoopFilter>(HERE(), datatype);
    case FilterType::FILTER_GZIP:
    case FilterType::FILTER_ZSTD:
    case FilterType::FILTER_LZ4:
    case FilterType::FILTER_RLE:
    case FilterType::FILTER_BZIP2:
    case FilterType::FILTER_DELTA:
    case FilterType::FILTER_DOUBLE_DELTA:
    case FilterType::FILTER_DICTIONARY: {
      uint8_t compressor_char = deserializer.read<uint8_t>();
      int compression_level = deserializer.read<int32_t>();
      Datatype reinterpret_type = Datatype::ANY;
      if ((version >= 20 && filtertype == FilterType::FILTER_DOUBLE_DELTA) ||
          (version >= 19 && filtertype == FilterType::FILTER_DELTA)) {
        uint8_t reinterpret = deserializer.read<uint8_t>();
        reinterpret_type = static_cast<Datatype>(reinterpret);
      }
      Compressor compressor = static_cast<Compressor>(compressor_char);
      return make_shared<CompressionFilter>(
          HERE(),
          compressor,
          compression_level,
          datatype,
          reinterpret_type,
          version);
    }
    case FilterType::FILTER_BIT_WIDTH_REDUCTION: {
      uint32_t max_window_size = deserializer.read<uint32_t>();
      return make_shared<BitWidthReductionFilter>(
          HERE(), max_window_size, datatype);
    }
    case FilterType::FILTER_BITSHUFFLE:
      return make_shared<BitshuffleFilter>(HERE(), datatype);
    case FilterType::FILTER_BYTESHUFFLE:
      return make_shared<ByteshuffleFilter>(HERE(), datatype);
    case FilterType::FILTER_POSITIVE_DELTA: {
      uint32_t max_window_size = deserializer.read<uint32_t>();
      return make_shared<PositiveDeltaFilter>(
          HERE(), max_window_size, datatype);
    }
    case FilterType::INTERNAL_FILTER_AES_256_GCM:
      if (encryption_key.encryption_type() ==
          tiledb::sm::EncryptionType::AES_256_GCM) {
        return make_shared<EncryptionAES256GCMFilter>(
            HERE(), encryption_key, datatype);
      } else {
        return make_shared<EncryptionAES256GCMFilter>(HERE(), datatype);
      }
    case FilterType::FILTER_CHECKSUM_MD5:
      return make_shared<ChecksumMD5Filter>(HERE(), datatype);
    case FilterType::FILTER_CHECKSUM_SHA256:
      return make_shared<ChecksumSHA256Filter>(HERE(), datatype);
    case FilterType::FILTER_SCALE_FLOAT: {
      auto filter_config =
          deserializer.read<FloatScalingFilter::FilterConfig>();
      return make_shared<FloatScalingFilter>(
          HERE(),
          filter_config.byte_width,
          filter_config.scale,
          filter_config.offset,
          datatype);
    };
    case FilterType::FILTER_XOR: {
      return make_shared<XORFilter>(HERE(), datatype);
    };
    case FilterType::FILTER_WEBP: {
      if constexpr (webp_filter_exists) {
        auto filter_config = deserializer.read<WebpFilter::FilterConfig>();
        return make_shared<WebpFilter>(
            HERE(),
            filter_config.quality,
            filter_config.format,
            filter_config.lossless,
            filter_config.y_extent,
            filter_config.x_extent,
            datatype);
      } else {
        throw WebpNotPresentError();
      }
    }
    default:
      throw StatusException(
          "FilterCreate", "Deserialization error; unknown type");
  }
}
shared_ptr<tiledb::sm::Filter> tiledb::sm::FilterCreate::deserialize(
    Deserializer& deserializer, const uint32_t version, Datatype datatype) {
  EncryptionKey encryption_key;
  return tiledb::sm::FilterCreate::deserialize(
      deserializer, encryption_key, version, datatype);
}
