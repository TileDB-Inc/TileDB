/**
 * @file   rest_capabilities.h
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
 * Helper struct to encapsulate REST supported versions and capabilities.
 */

namespace tiledb::sm {

struct RestCapabilities {
  struct TileDBVersion {
    TileDBVersion() = default;

    TileDBVersion(int major, int minor, int patch)
        : major_(major)
        , minor_(minor)
        , patch_(patch) {
    }

    bool operator==(const TileDBVersion& rhs) const = default;

    uint16_t major_, minor_, patch_;
  };

  /**
   * Default constructor allows the class to be constructed without submitting
   * a REST request to initialize member variables.
   */
  RestCapabilities() = default;

  ~RestCapabilities() = default;

  /**
   * Fully initialized constructor contains all REST version and capabilities
   * information required for handling edge cases between client & server
   * releases.
   */
  RestCapabilities(
      TileDBVersion rest_version,
      TileDBVersion rest_minimum_version,
      bool legacy = false)
      : detected_(true)
      , legacy_(legacy)
      , rest_tiledb_version_(rest_version)
      , rest_minimum_supported_version_(rest_minimum_version) {
  }

  bool operator==(const RestCapabilities& rhs) const = default;

  /// Whether or not the REST capabilities have been initialized.
  bool detected_ = false;

  /// True if the configured REST server is legacy.
  bool legacy_ = false;

  /// The currently deployed TileDB version available on the REST server.
  TileDBVersion rest_tiledb_version_{};

  /// The minimum TileDB version supported by the REST server.
  TileDBVersion rest_minimum_supported_version_{};
};

}  // namespace tiledb::sm
