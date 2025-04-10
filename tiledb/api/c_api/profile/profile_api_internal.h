/**
 * @file tiledb/api/c_api/profile/profile_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file declares the internals of the profile section of the C API.
 */

#ifndef TILEDB_CAPI_PROFILE_INTERNAL_H
#define TILEDB_CAPI_PROFILE_INTERNAL_H

#include "profile_api_external.h"

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/rest/rest_profile.h"

/** Handle `struct` for CAPI profile objects. */
struct tiledb_profile_handle_t
    : public tiledb::api::CAPIHandle<tiledb_profile_handle_t> {
  /** Type name */
  static constexpr std::string_view object_type_name{"profile"};

 private:
  using profile_type = shared_ptr<tiledb::sm::RestProfile>;
  profile_type profile_;

 public:
  template <class... Args>
  explicit tiledb_profile_handle_t(Args... args)
      : profile_{make_shared<tiledb::sm::RestProfile>(
            HERE(), std::forward<Args>(args)...)} {
  }

  /**
   * Constructor from `shared_ptr<RestProfile>` copies the shared pointer.
   */
  explicit tiledb_profile_handle_t(const profile_type& x)
      : profile_(x) {
  }

  profile_type profile() const {
    return profile_;
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating a profile.
 *
 * @param profile Possibly-valid pointer to a profile
 */
inline void ensure_profile_is_valid(const tiledb_profile_t* profile) {
  ensure_handle_is_valid(profile);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_PROFILE_INTERNAL_H
