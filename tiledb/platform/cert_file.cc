/**
 * @file cert_file.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Platform/machine config of the TileDB library.
 */

#include "tiledb/platform/cert_file.h"

#if defined(__linux__)
#include <array>
#include <filesystem>
#endif

namespace tiledb::platform::PlatformCertFile {

#if defined(__linux__)
std::string get() {
  // This will execute only once since C++11.
  static auto cert_file = []() -> std::string {
    const std::array<std::string, 6> cert_files = {
        "/etc/ssl/certs/ca-certificates.crt",  // Debian/Ubuntu/Gentoo etc.
        "/etc/pki/tls/certs/ca-bundle.crt",    // Fedora/RHEL 6
        "/etc/ssl/ca-bundle.pem",              // OpenSUSE
        "/etc/pki/tls/cacert.pem",             // OpenELEC
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",  // CentOS/RHEL 7
        "/etc/ssl/cert.pem"                                   // Alpine Linux
    };

    for (const std::string& cert : cert_files) {
      std::error_code ec;
      // Do not throw on errors.
      if (std::filesystem::exists(cert, ec)) {
        return cert;
      }
    }
    return "";
  }();

  return cert_file;
}
#else
std::string get() {
  return "";
}
#endif

}  // namespace tiledb::platform::PlatformCertFile
