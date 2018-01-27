/**
 * @file   tiledb_cpp_api_config.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Config object.
 */

#ifndef TILEDB_CPP_API_CONFIG_H
#define TILEDB_CPP_API_CONFIG_H

#include "tiledb_cpp_api_config_iter.h"
#include "tiledb_cpp_api_utils.h"
#include "tiledb.h"

#include <memory>
#include <string>

namespace tdb {

namespace impl {
class ConfigProxy;

struct ConfigDeleter {
  tiledb_error_t *err;

  void operator()(tiledb_config_t *p) {
    tiledb_error_t *err;
    tiledb_config_free(p, &err);
    check_error(err);
  }
};
}

/** Carries configuration parameters that will be passed to a Context object. */
class Config {
 public:
  using iterator = impl::ConfigIter;
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Basic constructor. */
  Config();

  /**
   * Constructor that takes as input a filename (URI) that stores the config
   * parameters. The file must have the following (text) format:
   *
   * parameter value
   *
   * Anything following a `#` character is considered a comment and, thus, is
   * ignored.
   *
   * See `set` for the various TileDB config parameters and allowed values.
   *
   * @param filename The name of the file where the parameters will be read
   *     from.
   */
  explicit Config(const std::string& filename);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the pointer to the TileDB C config object. */
  std::shared_ptr<tiledb_config_t> ptr() const;

  /** Auxiliary operator for getting the underlying C TileDB object. */
  operator tiledb_config_t*() const;

  /**
   * Sets a config parameter-value pair.
   *
   * **Parameters**
   *
   * - `sm.tile_cache_size` <br>
   *    The tile cache size in bytes. Any `uint64_t` value is acceptable.
   * - `sm.array_schema_cache_size` <br>
   *    The array schema cache size in bytes. Any `uint64_t` value is
   * acceptable.
   * - `sm.fragment_metadata_cache_size` <br>
   *    The fragment metadata cache size in bytes. Any `uint64_t` value is
   *    acceptable.
   * - `vfs.s3.region` <br>
   *    The S3 region, if S3 is enabled.
   * - `vfs.s3.scheme` <br>
   *    The S3 scheme (`http` or `https`), if S3 is enabled.
   * - `vfs.s3.endpoint_override` <br>
   *    The S3 endpoint, if S3 is enabled.
   * - `vfs.s3.use_virtual_addressing` <br>
   *    The S3 use of virtual addressing (`true` or `false`), if S3 is enabled.
   * - `vfs.s3.file_buffer_size` <br>
   *    The file buffer size (in bytes) used in S3 writes, if S3 is enables. Any
   *    `uint64_t` value is acceptable.
   * - `vfs.s3.connect_timeout_ms` <br>
   *    The connection timeout in ms. Any `long` value is acceptable.
   * - `vfs.s3.request_timeout_ms` <br>
   *    The request timeout in ms. Any `long` value is acceptable.
   */
  Config& set(const std::string& param, const std::string& value);

  /**
   * Get a parameter from the configuration by key.
   * @param param Key
   * @return Value
   */
  std::string get(const std::string& param) const;


  impl::ConfigProxy operator[](const std::string &param);

  /** Unsets a config parameter. */
  Config& unset(const std::string& param);

  /** Iterate over params starting with a prefix **/
  iterator begin(const std::string &prefix) {
    return iterator{*this, prefix, false};
  }

  /** Iterate over some params. **/
  iterator begin() {
    return iterator{*this, "", false};
  }

  /** End iterator **/
  iterator end() {
    return iterator{*this, "", true};
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The TileDB C config object. */
  std::shared_ptr<tiledb_config_t> config_;

  /** The URI path to the config file. */
  std::string filename_;

  impl::ConfigDeleter deleter_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Creates the TileDB C config object. */
  void create_config();
};

namespace impl {

/** Proxy to set params via operator[] **/
struct ConfigProxy {
  ConfigProxy(Config &conf, const std::string &param) : conf(conf), param(param) {}

  template<typename T>
  void operator=(const T &val) {
    conf.set(param, std::to_string(val));
  }

  void operator=(const char *val) {
    conf.set(param, std::string(val));
  }

  void operator=(const std::string &val) {
    conf.set(param, val);
  }

  operator std::string() {
    return conf.get(param);
  }

  Config &conf;
  const std::string param;
};

}

}  // namespace tdb

#endif  // TILEDB_CPP_API_CONFIG_H
