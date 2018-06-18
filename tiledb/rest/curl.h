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
 * This file defines curl fetch functions.
 */

#ifndef TILEDB_REST_CURL_HPP
#define TILEDB_REST_CURL_HPP

#include <curl/curl.h>
#include <cstdlib>
#include <string>

struct MemoryStruct {
  char* memory;
  size_t size;
};

/**
 *
 * Helper callback function from libcurl examples
 *
 * @param contents
 * @param size
 * @param nmemb
 * @param userp
 * @return
 */
size_t WriteMemoryCallback(
    void* contents, size_t size, size_t nmemb, void* userp);

/**
 * Help to make url fetches
 * @param curl pointer to curl instance
 * @param url to post/get
 * @param fetch data for response
 * @return
 */
CURLcode curl_fetch_url(
    CURL* curl, const char* url, struct MemoryStruct* fetch);

/**
 *
 * Simple wrapper for posting json to server
 *
 * @param curl instance
 * @param url to post to
 * @param jsonString json encoded string for posting
 * @param memoryStruct where response is stored
 * @return
 */
CURLcode post_json(
    CURL* curl,
    std::string url,
    std::string jsonString,
    MemoryStruct* memoryStruct);

/**
 * Simple wrapper for getting json from server
 *
 * @param curl instance
 * @param url to get
 * @param memoryStruct response is stored
 * @return
 */
CURLcode get_json(CURL* curl, std::string url, MemoryStruct* memoryStruct);

#endif  // TILEDB_REST_CURL_HPP
