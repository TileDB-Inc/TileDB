/**
 * @file unit-vfs-read-log-modes.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2025 TileDB Inc.
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
 * Tests for VFS read log modes.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/common/logger.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::sm;

TEST_CASE("VFS Read Log Modes", "[vfs][read-logging-modes]") {
  global_logger().set_level(Logger::Level::INFO);
  std::string mode = GENERATE(
      "",
      "Don't set config",
      "fragments",
      "fragment_files",
      "all_files",
      "all_reads",
      "all_reads_always",
      "bad_value");

  LOG_WARN("Checking vfs.read_logging_mode '" + mode + "'");

  Config cfg;
  throw_if_not_ok(cfg.set("config.logging_level", "3"));
  if (mode != std::string("Don't set config")) {
    throw_if_not_ok(cfg.set("vfs.read_logging_mode", mode));
  }

  auto logger = make_shared<Logger>(HERE(), "");
  ContextResources res(cfg, logger, 1, 1, "test");

  std::vector<std::string> uris_to_read = {
      "foo",
      "foo__fragments",
      "foo/__fragments/fragment_name",
      "foo/__fragments/fragment_name/baz.tdb",
      "foo/__meta/thing.tdb"};

  // This outer loop exists to show that we're only logging once when not
  // in all_reads mode.
  for (int i = 0; i < 2; i++) {
    char buffer[123];
    for (auto& uri : uris_to_read) {
      // None of these files exist, so we expect every read to fail.
      REQUIRE_THROWS(
          throw_if_not_ok(res.vfs().read_exactly(URI(uri), 123, buffer, 456)));
    }
  }
}
