/**
 * @file webp_filter.cc
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
 * This file implements class WebpFilter.
 */

#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb::sm {
std::ostream& WebpFilter::output(std::ostream& os) const {
  os << "WebpFilter";
  return os;
}

bool WebpFilter::accepts_input_datatype(Datatype datatype) const {
  return datatype == Datatype::UINT8;
}
}  // namespace tiledb::sm

#ifndef TILEDB_WEBP
namespace tiledb::sm {
/*
 * Stub implementations of externally-visible WebpFilter functions ensure that
 * any defect in calling these function when they shouldn't does not result in
 * a program crash.
 */

void WebpFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*) const {
  throw tiledb::sm::WebpNotPresentError();
}

Status WebpFilter::run_reverse(
    const Tile&,
    Tile* const,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*,
    FilterBuffer*,
    const Config&) const {
  throw tiledb::sm::WebpNotPresentError();
}

Status WebpFilter::set_option_impl(FilterOption, const void*) {
  throw tiledb::sm::WebpNotPresentError();
}

Status WebpFilter::get_option_impl(FilterOption, void*) const {
  throw tiledb::sm::WebpNotPresentError();
}

void WebpFilter::serialize_impl(Serializer&) const {
  throw tiledb::sm::WebpNotPresentError();
}

WebpFilter* WebpFilter::clone_impl() const {
  throw tiledb::sm::WebpNotPresentError();
}

}  // namespace tiledb::sm

#else
#include "webp/decode.h"
#include "webp/encode.h"

using namespace tiledb::common;

namespace tiledb::sm {

void WebpFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  return run_forward(input_metadata, input, output_metadata, output);
}

void WebpFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto input_parts = input->buffers();
  auto num_parts = static_cast<uint32_t>(input_parts.size());
  uint32_t metadata_size = sizeof(uint32_t) + (num_parts * sizeof(uint32_t));
  throw_if_not_ok(output_metadata->append_view(input_metadata));
  throw_if_not_ok(output_metadata->prepend_buffer(metadata_size));
  throw_if_not_ok(output_metadata->write(&num_parts, sizeof(uint32_t)));

  int extent_y = extents_.first, extent_x = extents_.second,
      pixel_depth = format_ < WebpInputFormat::WEBP_RGBA ? 3 : 4;
  for (const auto& i : input_parts) {
    auto data = static_cast<const uint8_t*>(i.data());
    // Number of bytes encoded; Encoded result data buffer.
    size_t enc_size = 0;
    uint8_t* result = nullptr;
    // Cleanup allocated data when we leave scope.
    ScopedExecutor cleanup([&]() {
      if (enc_size > 0 && result != nullptr)
        WebPFree(result);
    });
    // We divide extent_x by colorspace value count to get pixel-width of image,
    // extent_x currently represents row stride.
    switch (format_) {
      case WebpInputFormat::WEBP_RGB:
        if (lossless_) {
          enc_size = WebPEncodeLosslessRGB(
              data, extent_x / pixel_depth, extent_y, extent_x, &result);
        } else {
          enc_size = WebPEncodeRGB(
              data,
              extent_x / pixel_depth,
              extent_y,
              extent_x,
              quality_,
              &result);
        }
        break;
      case WebpInputFormat::WEBP_RGBA:
        if (lossless_) {
          enc_size = WebPEncodeLosslessRGBA(
              data, extent_x / pixel_depth, extent_y, extent_x, &result);
        } else {
          enc_size = WebPEncodeRGBA(
              data,
              extent_x / pixel_depth,
              extent_y,
              extent_x,
              quality_,
              &result);
        }
        break;
      case WebpInputFormat::WEBP_BGR:
        if (lossless_) {
          enc_size = WebPEncodeLosslessBGR(
              data, extent_x / pixel_depth, extent_y, extent_x, &result);
        } else {
          enc_size = WebPEncodeBGR(
              data,
              extent_x / pixel_depth,
              extent_y,
              extent_x,
              quality_,
              &result);
        }
        break;
      case WebpInputFormat::WEBP_BGRA:
        if (lossless_) {
          enc_size = WebPEncodeLosslessBGRA(
              data, extent_x / pixel_depth, extent_y, extent_x, &result);
        } else {
          enc_size = WebPEncodeBGRA(
              data,
              extent_x / pixel_depth,
              extent_y,
              extent_x,
              quality_,
              &result);
        }
        break;
      case WebpInputFormat::WEBP_NONE:
        throw StatusException(Status_FilterError(
            "Filter option TILEDB_FILTER_WEBP_FORMAT must be set"));
      default:
        throw StatusException(Status_FilterError(
            "Filter option TILEDB_FILTER_WEBP_FORMAT is invalid"));
    }

    // Check if encoding failed.
    if (enc_size == 0) {
      throw StatusException(Status_FilterError("Error encoding image data"));
    }

    // Write encoded data to output buffer.
    throw_if_not_ok(output_metadata->write(&enc_size, sizeof(uint32_t)));
    throw_if_not_ok(output->prepend_buffer(enc_size));
    throw_if_not_ok(output->write(result, enc_size));
  }
}

Status WebpFilter::run_reverse(
    const Tile&,
    Tile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  if (filter_data_type_ != Datatype::UINT8) {
    throw StatusException(Status_FilterError("Unsupported input type"));
  }
  return run_reverse(input_metadata, input, output_metadata, output);
}

Status WebpFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  uint32_t num_parts;
  throw_if_not_ok(input_metadata->read(&num_parts, sizeof(uint32_t)));
  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t enc_size;
    uint8_t* result = nullptr;
    // Cleanup allocated data when we leave scope.
    ScopedExecutor cleanup([&]() {
      if (enc_size > 0 && result != nullptr)
        WebPFree(result);
    });
    // Read size of data encoded from input metadata.
    throw_if_not_ok(input_metadata->read(&enc_size, sizeof(uint32_t)));
    // Read encoded data from input buffer.
    ConstBuffer part(nullptr, 0);
    throw_if_not_ok(input->get_const_buffer(enc_size, &part));

    // Decode data.
    auto data = static_cast<const uint8_t*>(part.data());
    int width, height;
    switch (format_) {
      case WebpInputFormat::WEBP_RGB:
        result = WebPDecodeRGB(data, enc_size, &width, &height);
        break;
      case WebpInputFormat::WEBP_RGBA:
        result = WebPDecodeRGBA(data, enc_size, &width, &height);
        break;
      case WebpInputFormat::WEBP_BGR:
        result = WebPDecodeBGR(data, enc_size, &width, &height);
        break;
      case WebpInputFormat::WEBP_BGRA:
        result = WebPDecodeBGRA(data, enc_size, &width, &height);
        break;
      default:
        throw StatusException(Status_FilterError(
            "Filter option TILEDB_FILTER_WEBP_FORMAT must be set"));
    }

    // Check if decoding failed.
    if (result == nullptr) {
      throw StatusException(Status_FilterError("Error decoding image data"));
    }
    throw_if_not_ok(output->write(result, output->size()));
  }

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  throw_if_not_ok(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

Status WebpFilter::set_option_impl(FilterOption option, const void* value) {
  if (value == nullptr)
    throw StatusException(
        Status_FilterError("Webp filter error; Invalid option value"));

  switch (option) {
    case FilterOption::WEBP_QUALITY: {
      auto val = *(float*)value;
      if (val < 0.0f || val > 100.0f) {
        throw StatusException(Status_FilterError(
            "Webp filter error; Quality must be in range [0.0, 100.0]"));
      }
      quality_ = val;
      break;
    }
    case FilterOption::WEBP_INPUT_FORMAT: {
      auto val = static_cast<WebpInputFormat>(*(uint8_t*)value);
      if (val < WebpInputFormat::WEBP_NONE ||
          val > WebpInputFormat::WEBP_BGRA) {
        throw StatusException(Status_FilterError(
            "Webp filter error; Invalid input format option setting"));
      }
      format_ = val;
      break;
    }
    case FilterOption::WEBP_LOSSLESS: {
      auto val = *(uint8_t*)value;
      if (val > 1) {
        throw StatusException(Status_FilterError(
            "Webp filter error; Lossless compression must be either enabled "
            "(1) or disabled (0)"));
      }
      lossless_ = val;
      break;
    }
    default:
      throw StatusException(
          Status_FilterError("Webp filter error; Unknown option"));
  }

  return Status::Ok();
}

Status WebpFilter::get_option_impl(FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::WEBP_QUALITY:
      *(float*)value = quality_;
      break;
    case FilterOption::WEBP_INPUT_FORMAT:
      *(WebpInputFormat*)value = format_;
      break;
    case FilterOption::WEBP_LOSSLESS:
      *(uint8_t*)value = lossless_;
      break;
    default:
      throw StatusException(
          Status_FilterError("Webp filter error; Unknown option"));
  }
  return Status::Ok();
}

[[nodiscard]] WebpFilter* WebpFilter::clone_impl() const {
  return tdb_new(
      WebpFilter,
      quality_,
      format_,
      lossless_,
      extents_.first,
      extents_.second,
      filter_data_type_);
}

void WebpFilter::serialize_impl(Serializer& serializer) const {
  FilterConfig filter_config{
      quality_, format_, lossless_, extents_.first, extents_.second};
  serializer.write(filter_config);
}

template <typename T>
void WebpFilter::set_extents(const std::vector<ByteVecValue>& extents) {
  extents_ = {extents[0].rvalue_as<T>(), extents[1].rvalue_as<T>()};
  uint8_t pixel_depth = format_ < WebpInputFormat::WEBP_RGBA ? 3 : 4;
  // X should be divisible by pixel_depth or RGB values will skew.
  if (extents_.second % pixel_depth != 0) {
    throw StatusException(Status_FilterError(
        pixel_depth == 3 ?
            "Colorspace with no alpha must use extents divisible by 3" :
            "Colorspace with alpha must use extents divisible by 4"));
  }
  if (extents_.first > 16383 || (extents_.second / pixel_depth) > 16383) {
    throw StatusException(Status_FilterError(
        "Tile extents too large; Max size WebP image is 16383x16383 pixels"));
  }
}

template void WebpFilter::set_extents<uint8_t>(
    const std::vector<ByteVecValue>& extents);
template void WebpFilter::set_extents<uint16_t>(
    const std::vector<ByteVecValue>& extents);
template void WebpFilter::set_extents<uint32_t>(
    const std::vector<ByteVecValue>& extents);
template void WebpFilter::set_extents<uint64_t>(
    const std::vector<ByteVecValue>& extents);
template void WebpFilter::set_extents<int8_t>(
    const std::vector<ByteVecValue>& extents);
template void WebpFilter::set_extents<int16_t>(
    const std::vector<ByteVecValue>& extents);
template void WebpFilter::set_extents<int32_t>(
    const std::vector<ByteVecValue>& extents);
template void WebpFilter::set_extents<int64_t>(
    const std::vector<ByteVecValue>& extents);

}  // namespace tiledb::sm

#endif  // TILEDB_WEBP
