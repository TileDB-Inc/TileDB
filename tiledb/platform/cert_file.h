/**
 * @file   tiledb/platform/cert_file.h
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
 * Platform/machine config of the TileDB library.
 */

#ifndef TILEDB_CERT_FILE_H
#define TILEDB_CERT_FILE_H

#include <array>
#include <mutex>
#include <string>
#include <system_error>

#include "tiledb/platform/os.h"

// We can remove this preprocessor block once our minimum
// macOS version is at least 10.15. Until then macOS won't
// compile even though this std::filesystem is only run
// on Linux.
#if defined(__APPLE__) && defined(__MACH__)
static bool file_exists(const std::string&) {
  throw std::runtime_error("cert file discovery not supported");
}
#else
#include <filesystem>
static bool file_exists(const std::string& path) {
  std::error_code ec;
  return std::filesystem::exists(path, ec);
}
#endif

namespace tiledb::platform {

/**
 * A base traits class for determining whether the current
 * platform might have a CA Cert file on disk in a well
 * known location.
 */
template <class T>
class CertFileTraits {
  CertFileTraits() = delete;
};

/**
 * A platform dependent alias to the appropriate
 * certficate file trait.
 */
using PlatformCertFile = CertFileTraits<PlatformOS>;

/**
 * A trait specialization for when the current platform does
 * not have a certificate file in a well known location.
 */
template <>
class CertFileTraits<BaseOS> {
 public:
  /**
   * A boolean flag indicating that the current platform does
   * not have a certificate file in a well known location.
   */
  static constexpr bool enabled = false;

  /**
   * Unconditionally returns an empty string for platforms
   * that do not have a certificate file in a well known
   * location.
   *
   * \return An empty string.
   */
  static std::string get() {
    return {};
  }
};

/**
 * A trait specialization for when the current platform might
 * have a certificate file in a well known location.
 */
template <>
class CertFileTraits<LinuxOS> {
 public:
  /**
   * A boolean flag indicating that the current platform has
   * a certficate file in a well known location.
   */
  static constexpr bool enabled = true;

  /**
   * An instance of `std::once_flag` to search for a
   * certificate file at most once per process lifetime.
   */
  static std::once_flag cert_file_initialized_;

  /**
   * The cached certificate file location.
   */
  static std::string cert_file_;

  /**
   * Return the possibly cached certificate file location. Only
   * the first call to this function actually performs the search.
   * All subsequent calls return the cached location.
   *
   * \return The path to a well known certificate file path or
   * an empty string if no such file is found.
   */
  static std::string get() {
    std::call_once(cert_file_initialized_, []() {
      const std::array<std::string, 6> cert_files = {
          "/etc/ssl/certs/ca-certificates.crt",  // Debian/Ubuntu/Gentoo etc.
          "/etc/pki/tls/certs/ca-bundle.crt",    // Fedora/RHEL 6
          "/etc/ssl/ca-bundle.pem",              // OpenSUSE
          "/etc/pki/tls/cacert.pem",             // OpenELEC
          "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",  // CentOS/RHEL 7
          "/etc/ssl/cert.pem"                                   // Alpine Linux
      };

      for (const std::string& cert : cert_files) {
        if (file_exists(cert)) {
          cert_file_ = cert;
          return;
        }
      }
    });

    return cert_file_;
  }
};

}  // namespace tiledb::platform
#endif  // TILEDB_CERT_FILE_H
