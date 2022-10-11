/**
 * @file   webp_filter.cc
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

#ifdef TILEDB_WEBP

#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/tile/tile.h"

#include "webp/decode.h"
#include "webp/encode.h"

using namespace tiledb::common;

namespace tiledb::sm {
void WebpFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(out, "WebpFilter");
}

Status WebpFilter::run_forward(
    const Tile& tile,
    Tile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  if (tile.type() != Datatype::UINT8) {
    throw StatusException(Status_FilterError("Unsupported input type"));
  }
  return run_forward(input_metadata, input, output_metadata, output);
}

Status WebpFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto input_parts = input->buffers();
  auto num_parts = static_cast<uint32_t>(input_parts.size());
  uint32_t metadata_size = sizeof(uint32_t) + (num_parts * sizeof(uint32_t));
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // TODO: Use X / Y dimension bounds for image dimensions
  int width = 284, height = 178;
  for (const auto& i : input_parts) {
    auto data = static_cast<const uint8_t*>(i.data());
    // Number of bytes encoded; Encoded result data buffer
    size_t enc_size = 0;
    uint8_t* result = nullptr;
    // Cleanup allocated data when we leave scope
    ScopedExecutor cleanup([&]() {
      if (enc_size > 0 && result != nullptr)
        WebPFree(result);
    });
    switch (format_) {
      case WebpInputFormat::WEBP_RGB:
        enc_size =
            WebPEncodeRGB(data, width, height, width * 3, quality_, &result);
        break;
      case WebpInputFormat::WEBP_RGBA:
        enc_size =
            WebPEncodeRGBA(data, width, height, width * 4, quality_, &result);
        break;
      case WebpInputFormat::WEBP_BGR:
        enc_size =
            WebPEncodeBGR(data, width, height, width * 3, quality_, &result);
        break;
      case WebpInputFormat::WEBP_BGRA:
        enc_size =
            WebPEncodeBGRA(data, width, height, width * 4, quality_, &result);
        break;
      case WebpInputFormat::WEBP_NONE:
        throw StatusException(Status_FilterError(
            "Filter option TILEDB_FILTER_WEBP_FORMAT must be set"));
    }

    // Check if encoding failed
    if (enc_size == 0) {
      throw StatusException(Status_FilterError("Error encoding image data"));
    }

    // Write encoded data to output buffer
    RETURN_NOT_OK(output_metadata->write(&enc_size, sizeof(uint32_t)));
    RETURN_NOT_OK(output->prepend_buffer(enc_size));
    RETURN_NOT_OK(output->write(result, enc_size));
  }

  return Status::Ok();
}

Status WebpFilter::run_reverse(
    const Tile& tile,
    Tile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  if (tile.type() != Datatype::UINT8) {
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
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));
  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t enc_size;
    uint8_t* result = nullptr;
    // Cleanup allocated data when we leave scope
    ScopedExecutor cleanup([&]() {
      if (enc_size > 0 && result != nullptr)
        WebPFree(result);
    });
    // Read size of data encoded from input metadata
    RETURN_NOT_OK(input_metadata->read(&enc_size, sizeof(uint32_t)));
    // Read encoded data from input buffer
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(enc_size, &part));

    // Decode data
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

    // Check if decoding failed
    if (result == nullptr) {
      throw StatusException(Status_FilterError("Error decoding image data"));
    }
    RETURN_NOT_OK(output->write(result, output->size()));
  }

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

Status WebpFilter::set_option_impl(FilterOption option, const void* value) {
  if (value == nullptr)
    throw StatusException(
        Status_FilterError("Webp filter error; Invalid option value"));

  switch (option) {
    case FilterOption::WEBP_QUALITY:
      quality_ = *(float*)value;
      break;
    case FilterOption::WEBP_INPUT_FORMAT:
      format_ = static_cast<WebpInputFormat>(*(uint8_t*)value);
      break;
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
    default:
      throw StatusException(
          Status_FilterError("Webp filter error; Unknown option"));
  }
  return Status::Ok();
}

[[nodiscard]] WebpFilter* WebpFilter::clone_impl() const {
  return tdb_new(WebpFilter, quality_, format_);
}

void WebpFilter::serialize_impl(Serializer& serializer) const {
  FilterConfig filter_config{quality_, format_};
  serializer.write(filter_config);
}

}  // namespace tiledb::sm

#endif  // TILEDB_WEBP