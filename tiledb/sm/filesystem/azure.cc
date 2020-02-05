/**
 * @file   azure.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file implements the Azure class.
 */

#ifdef HAVE_AZURE

#include <cpprest/containerstream.h>
#include <cpprest/filestream.h>
#include <was/blob.h>
#include <was/storage_account.h>

#include "tiledb/sm/filesystem/azure.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"

namespace tiledb {
namespace sm {

Azure::Azure() {
  account_ = new azure::storage::cloud_storage_account();
}

Azure::~Azure() {
  free(account_);
}

Status Azure::init(const Config& /*config*/, ThreadPool* /*thread_pool*/) {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::create_container(const URI& /*container*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::disconnect() {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::empty_container(const URI& /*container*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::flush_object(const URI& /*uri*/) {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

bool Azure::is_container(const URI& /*uri*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return false;
}

Status Azure::is_dir(const URI& /*uri*/, bool* /*exists*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

bool Azure::is_object(const URI& /*uri*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return false;
}

Status Azure::ls(
    const URI& /*prefix*/,
    std::vector<std::string>* /*paths*/,
    const std::string& /*delimiter*/,
    int /*max_paths*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::move_object(const URI& /*old_uri*/, const URI& /*new_uri*/) {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::move_dir(const URI& /*old_uri*/, const URI& /*new_uri*/) {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::object_size(const URI& /*uri*/, uint64_t* /*nbytes*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::read(
    const URI& /*uri*/,
    off_t /*offset*/,
    void* /*buffer*/,
    uint64_t /*length*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::remove_container(const URI& /*container*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::remove_object(const URI& /*uri*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::remove_dir(const URI& /*prefix*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::touch(const URI& /*uri*/) const {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

Status Azure::write(
    const URI& /*uri*/, const void* /*buffer*/, uint64_t /*length*/) {
  LOG_FATAL("Azure::" + std::string(__FUNCTION__) + " unimplemented");
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif