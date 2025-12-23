/**
 * @file   rest_client.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 * This file declares a REST client class.
 */

#include "tiledb/sm/rest/rest_client.h"

#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/misc/constants.h"

namespace tiledb::sm {

/*
 * This is one of two possible initializations of `factory_override_`.
 * Critically, this one is considered "constant initialization" and happens
 * during static initialization, which is usually at compile time, but
 * regardless always happens before dynamic initialization time.
 *
 * The second, and only optional, initialization happens through
 * `RestClientFactoryAssistant::override_factory`. This function is designed so
 * that it may be the initializer of a non-local variable. Non-local variables
 * are dynamically initialized.
 */
RestClientFactory::factory_type* RestClientFactory::factory_override_{nullptr};

RestClient::RestClient(const Config& config)
    : rest_server_{}  // reinitialized below
{
  /*
   * Initialization of `rest_server_`
   */
  rest_server_ = std::string(
      config.get<std::string_view>("rest.server_address", Config::must_find));
  if (rest_server_.ends_with('/')) {
    size_t pos = rest_server_.find_last_not_of('/');
    rest_server_.resize(pos + 1);
  }
  if (rest_server_.empty()) {
    throw RestClientException(
        "Error initializing rest client; server address is empty.");
  }

  /*
   * Initialization of `extra_headers_`
   */
  const auto version = std::to_string(constants::library_version[0]) + "." +
                       std::to_string(constants::library_version[1]) + "." +
                       std::to_string(constants::library_version[2]);
  extra_headers_["x-tiledb-version"] = version;
  extra_headers_["x-tiledb-api-language"] = "c";

  for (auto iter = ConfigIter(config, constants::rest_header_prefix);
       !iter.end();
       iter.next()) {
    auto key = iter.param();
    if (key.size() == 0) {
      continue;
    }
    extra_headers_[key] = iter.value();
  }

  if (auto payer = config.get<std::string_view>(
          "rest.payer_namespace", Config::must_find);
      !payer.empty()) {
    extra_headers_["X-Payer"] = std::string(payer);
  }
}

void RestClient::add_header(const std::string& name, const std::string& value) {
  extra_headers_[name] = value;
}

std::shared_ptr<RestClient> RestClientFactory::make(
    stats::Stats& parent_stats,
    const Config& config,
    ThreadPool& compute_tp,
    Logger& logger,
    shared_ptr<MemoryTracker>&& tracker) {
  if (factory_override_ == nullptr) {
    return tdb::make_shared<RestClient>(HERE(), RestClient(config));
  } else {
    return factory_override_(
        parent_stats, config, compute_tp, logger, std::move(tracker));
  }
}

RestClientFactoryAssistant::factory_type*
RestClientFactoryAssistant::override_factory(factory_type* f) {
  if (RestClientFactory::factory_override_ != nullptr) {
    throw std::logic_error(
        "Defective use of `override_factory`; "
        "function is called more than once with nonnull argument.");
  }
  auto return_value{RestClientFactory::factory_override_};
  RestClientFactory::factory_override_ = f;
  return return_value;
}

}  // namespace tiledb::sm
