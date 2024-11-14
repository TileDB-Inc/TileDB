/**
 * @file mime_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This defines the TileDB MimeType enum that maps to tiledb_mime_type_t
 * C-API enum.
 *
 * Note: This enum is deprecated and will be removed in the next release.
 */

#ifndef TILEDB_MIME_TYPE_H
#define TILEDB_MIME_TYPE_H

#include <cassert>
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A mime type.
 *
 */
enum class MimeType : uint8_t {
#define TILEDB_MIME_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_MIME_TYPE_ENUM
};

/** Returns the string representation of the input mime type. */
inline const std::string& mime_type_str(MimeType mime_type) {
  switch (mime_type) {
    case MimeType::MIME_AUTODETECT:
      return constants::mime_autodetect_str;
    case MimeType::MIME_TIFF:
      return constants::mime_tiff_str;
    case MimeType::MIME_PDF:
      return constants::mime_pdf_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the mime type given a string representation. */
inline Status mime_type_enum(
    const std::string& mime_type_str, MimeType* mime_type) {
  if (mime_type_str == constants::mime_autodetect_str)
    *mime_type = MimeType::MIME_AUTODETECT;
  else if (mime_type_str == constants::mime_tiff_str)
    *mime_type = MimeType::MIME_TIFF;
  else if (mime_type_str == constants::mime_pdf_str)
    *mime_type = MimeType::MIME_PDF;
  else {
    return Status_Error("Invalid MimeType " + mime_type_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_MIME_TYPE_H
