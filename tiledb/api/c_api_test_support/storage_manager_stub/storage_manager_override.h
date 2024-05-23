/**
 * @file storage_manager_override.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file declares `class StorageManagerStub`.
 */

#ifndef TILEDB_C_API_TEST_SUPPORT_STORAGE_MANAGER_OVERRIDE_H
#define TILEDB_C_API_TEST_SUPPORT_STORAGE_MANAGER_OVERRIDE_H

#include <memory>
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/storage_manager/context_resources.h"

namespace tiledb::common {
class ThreadPool;
class Logger;
}  // namespace tiledb::common
namespace tiledb::stats {
class Stats;
}
namespace tiledb::sm {
class Config;
class VFS;

class StorageManagerStub {
  ContextResources& resources_;
  Config config_;

 public:
  static constexpr bool is_overriding_class = true;
  StorageManagerStub(
      ContextResources& resources,
      std::shared_ptr<common::Logger>,
      const Config& config)
      : resources_(resources)
      , config_(config) {
  }

  inline common::ThreadPool* io_tp() {
    return &resources_.io_tp();
  }
  inline Status cancel_all_tasks() {
    return Status{};
  };
  inline Status group_create(const std::string&) {
    throw std::logic_error(
        "StorageManagerStub does not support group creation");
  }
  inline Status group_metadata_consolidate(const char*, const Config&) {
    throw std::logic_error(
        "StorageManagerStub does not support group metadata consolidation");
  }
  inline Status group_metadata_vacuum(const char*, const Config&) {
    throw std::logic_error(
        "StorageManagerStub does not support group metadata vacuum");
  }
  inline Status set_tag(const std::string&, const std::string&) {
    return Status{};
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_C_API_TEST_SUPPORT_STORAGE_MANAGER_OVERRIDE_H
