/**
 * @file   curl.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines high-level libcurl helper functions.
 */

#ifndef TILEDB_REST_CURL_HPP
#define TILEDB_REST_CURL_HPP

#include <curl/curl.h>
#include <cstdlib>
#include <string>
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/storage_manager/config.h"

namespace tiledb {
namespace curl {

/**
 * Simple wrapper for posting data to server
 *
 * @param curl instance
 * @param config tiledb config used to get auth information
 * @param url to post to
 * @param dataString data encoded string for posting
 * @param returned_data where response is stored
 * @return
 */
tiledb::sm::Status post_data(
    CURL* curl,
    const tiledb::sm::Config* config,
    const std::string& url,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Buffer* data,
    tiledb::sm::Buffer* returned_data);

/**
 * Simple wrapper for getting data from server
 *
 * @param curl instance
 * @param config tiledb config used to get auth information
 * @param url to get
 * @param serialization_type format data is serialized in, effects headers
 * @param returned_data response is stored
 * @return
 */
tiledb::sm::Status get_data(
    CURL* curl,
    const tiledb::sm::Config* config,
    const std::string& url,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Buffer* returned_data);

/**
 * Simple wrapper for sending delete requests to server
 * @param curl instance
 * @param config tiledb config used to get auth information
 * @param url to get
 * @param serialization_type format data is serialized in, effects headers
 * @param returned_data response is stored
 * @return
 */
tiledb::sm::Status delete_data(
    CURL* curl,
    const tiledb::sm::Config* config,
    const std::string& url,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Buffer* returned_data);

}  // namespace curl
}  // namespace tiledb

#endif  // TILEDB_REST_CURL_HPP
