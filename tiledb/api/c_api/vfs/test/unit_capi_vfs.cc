/**
 * @file tiledb/api/c_api/vfs/test/unit_capi_vfs.cc
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
 * Validates the arguments for the VFS C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../vfs_api_internal.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_vfs.h"
#include "tiledb/platform/platform.h"

using tiledb::api::test_support::ordinary_vfs;
using tiledb::api::test_support::ordinary_vfs_fh;
const char* TEST_URI = "unit_capi_vfs";

TEST_CASE("C API: tiledb_vfs_alloc argument validation", "[capi][vfs]") {
  tiledb_ctx_t* ctx;
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  tiledb_vfs_t* vfs;

  capi_return_t rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  rc = tiledb_config_alloc(&config, &error);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  REQUIRE(config != nullptr);

  SECTION("success") {
    rc = tiledb_vfs_alloc(ctx, config, &vfs);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(vfs != nullptr);
    tiledb_vfs_free(&vfs);
  }
  SECTION("null context") {
    rc = tiledb_vfs_alloc(nullptr, config, &vfs);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null config") {
    rc = tiledb_vfs_alloc(ctx, nullptr, &vfs);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null vfs pointer") {
    rc = tiledb_vfs_alloc(ctx, config, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_ctx_free(&ctx);
  tiledb_config_free(&config);
}

TEST_CASE("C API: tiledb_vfs_free argument validation", "[capi][vfs]") {
  REQUIRE_NOTHROW(tiledb_vfs_free(nullptr));
}

TEST_CASE("C API: tiledb_vfs_get_config argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  auto rc{tiledb_config_alloc(&config, &error)};
  CHECK(tiledb_status(rc) == TILEDB_OK);

  SECTION("success") {
    auto rc{tiledb_vfs_get_config(x.ctx, x.vfs, &config)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_get_config(nullptr, x.vfs, &config)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_get_config(x.ctx, nullptr, &config)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config pointer") {
    auto rc{tiledb_vfs_get_config(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_config_free(&config);
}

TEST_CASE(
    "C API: tiledb_vfs_create_bucket argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_vfs_create_bucket(nullptr, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_create_bucket(x.ctx, nullptr, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_create_bucket(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_is_bucket argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  int32_t is_bucket;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_vfs_is_bucket(nullptr, x.vfs, TEST_URI, &is_bucket)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_is_bucket(x.ctx, nullptr, TEST_URI, &is_bucket)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_is_bucket(x.ctx, x.vfs, nullptr, &is_bucket)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null flag") {
    auto rc{tiledb_vfs_is_bucket(x.ctx, x.vfs, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_empty_bucket argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_vfs_empty_bucket(nullptr, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_empty_bucket(x.ctx, nullptr, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_empty_bucket(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_vfs_is_empty_bucket argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  int32_t is_empty;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_vfs_is_empty_bucket(nullptr, x.vfs, TEST_URI, &is_empty)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_is_empty_bucket(x.ctx, nullptr, TEST_URI, &is_empty)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_is_empty_bucket(x.ctx, x.vfs, nullptr, &is_empty)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null flag") {
    auto rc{tiledb_vfs_is_empty_bucket(x.ctx, x.vfs, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_vfs_remove_bucket argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_vfs_remove_bucket(nullptr, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_remove_bucket(x.ctx, nullptr, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_remove_bucket(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_create_dir argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_create_dir(x.ctx, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_create_dir(nullptr, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_create_dir(x.ctx, nullptr, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_create_dir(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_is_dir argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  int32_t is_dir;
  SECTION("success") {
    auto rc{tiledb_vfs_is_dir(x.ctx, x.vfs, TEST_URI, &is_dir)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_dir == 1);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_is_dir(nullptr, x.vfs, TEST_URI, &is_dir)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_is_dir(x.ctx, nullptr, TEST_URI, &is_dir)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    if constexpr (tiledb::platform::is_os_windows) {
      // Windows handles empty (which gets converted from null) paths
      // differently. Reconsider when the logic gets unified across paltforms
      // (SC-28225).
      return;
    }
    auto rc{tiledb_vfs_is_dir(x.ctx, x.vfs, nullptr, &is_dir)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null flag") {
    auto rc{tiledb_vfs_is_dir(x.ctx, x.vfs, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_ls argument validation", "[capi][vfs]") {
  /*
   * No "success" sections here; too much overhead to set up.
   */
  ordinary_vfs x;
  int32_t data;
  SECTION("null context") {
    auto rc{tiledb_vfs_ls(nullptr, x.vfs, TEST_URI, nullptr, &data)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_ls(x.ctx, nullptr, TEST_URI, nullptr, &data)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_ls(x.ctx, x.vfs, nullptr, nullptr, &data)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_dir_size argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  uint64_t size;
  SECTION("success") {
    auto rc{tiledb_vfs_dir_size(x.ctx, x.vfs, TEST_URI, &size)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(size == 0);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_dir_size(nullptr, x.vfs, TEST_URI, &size)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_dir_size(x.ctx, nullptr, TEST_URI, &size)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_dir_size(x.ctx, x.vfs, nullptr, &size)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null flag") {
    auto rc{tiledb_vfs_dir_size(x.ctx, x.vfs, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_move_dir argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_move_dir(x.ctx, x.vfs, TEST_URI, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_move_dir(nullptr, x.vfs, TEST_URI, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_move_dir(x.ctx, nullptr, TEST_URI, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null old_uri") {
    auto rc{tiledb_vfs_move_dir(x.ctx, nullptr, nullptr, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null new_dir") {
    auto rc{tiledb_vfs_move_dir(x.ctx, nullptr, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("success") {
    // Move dir back to original location
    auto rc{tiledb_vfs_move_dir(x.ctx, x.vfs, "new_dir", TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
}

TEST_CASE("C API: tiledb_vfs_copy_dir argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_copy_dir(x.ctx, x.vfs, TEST_URI, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_copy_dir(nullptr, x.vfs, TEST_URI, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_copy_dir(x.ctx, nullptr, TEST_URI, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null old_uri") {
    auto rc{tiledb_vfs_copy_dir(x.ctx, nullptr, nullptr, "new_dir")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null new_dir") {
    auto rc{tiledb_vfs_copy_dir(x.ctx, nullptr, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_vfs_remove_dir(x.ctx, x.vfs, "new_dir");
}

TEST_CASE("C API: tiledb_vfs_remove_dir argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_remove_dir(x.ctx, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_remove_dir(nullptr, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_remove_dir(x.ctx, nullptr, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_remove_dir(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_touch argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_touch(x.ctx, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_touch(nullptr, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs file handle") {
    auto rc{tiledb_vfs_touch(x.ctx, nullptr, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_touch(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_is_file argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  int32_t is_file;
  SECTION("success") {
    auto rc{tiledb_vfs_is_file(x.ctx, x.vfs, TEST_URI, &is_file)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(is_file == 1);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_is_file(nullptr, x.vfs, TEST_URI, &is_file)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_is_file(x.ctx, nullptr, TEST_URI, &is_file)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    if constexpr (tiledb::platform::is_os_windows) {
      // Windows handles empty (which gets converted from null) paths
      // differently. Reconsider when the logic gets unified across paltforms
      // (SC-28225).
      return;
    }
    auto rc{tiledb_vfs_is_file(x.ctx, x.vfs, nullptr, &is_file)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null flag") {
    auto rc{tiledb_vfs_is_file(x.ctx, x.vfs, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_file_size argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  uint64_t size;
  SECTION("success") {
    auto rc{tiledb_vfs_file_size(x.ctx, x.vfs, TEST_URI, &size)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(size == 0);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_file_size(nullptr, x.vfs, TEST_URI, &size)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_file_size(x.ctx, nullptr, TEST_URI, &size)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_file_size(x.ctx, x.vfs, nullptr, &size)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null flag") {
    auto rc{tiledb_vfs_file_size(x.ctx, x.vfs, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_move_file argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_move_file(x.ctx, x.vfs, TEST_URI, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_move_file(nullptr, x.vfs, TEST_URI, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_move_file(x.ctx, nullptr, TEST_URI, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null old_uri") {
    auto rc{tiledb_vfs_move_file(x.ctx, nullptr, nullptr, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null new_uri") {
    auto rc{tiledb_vfs_move_file(x.ctx, nullptr, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("success") {
    // Move file back to original location
    auto rc{tiledb_vfs_move_file(x.ctx, x.vfs, "new_uri", TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
}

TEST_CASE("C API: tiledb_vfs_copy_file argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_copy_file(x.ctx, x.vfs, TEST_URI, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_copy_file(nullptr, x.vfs, TEST_URI, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_copy_file(x.ctx, nullptr, TEST_URI, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null old_uri") {
    auto rc{tiledb_vfs_copy_file(x.ctx, nullptr, nullptr, "new_uri")};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null new_uri") {
    auto rc{tiledb_vfs_copy_file(x.ctx, nullptr, TEST_URI, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  tiledb_vfs_remove_file(x.ctx, x.vfs, "new_uri");
}

TEST_CASE("C API: tiledb_vfs_remove_file argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  SECTION("success") {
    auto rc{tiledb_vfs_remove_file(x.ctx, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_remove_file(nullptr, x.vfs, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_remove_file(x.ctx, nullptr, TEST_URI)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_remove_file(x.ctx, x.vfs, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_open argument validation", "[capi][vfs]") {
  ordinary_vfs x;
  tiledb_vfs_fh_handle_t* vfs_fh;
  SECTION("success") {
    auto rc{tiledb_vfs_open(x.ctx, x.vfs, TEST_URI, TILEDB_VFS_WRITE, &vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(vfs_fh != nullptr);
    tiledb_vfs_fh_free(&vfs_fh);
  }
  SECTION("null context") {
    auto rc{
        tiledb_vfs_open(nullptr, x.vfs, TEST_URI, TILEDB_VFS_WRITE, &vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{
        tiledb_vfs_open(x.ctx, nullptr, TEST_URI, TILEDB_VFS_WRITE, &vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_open(x.ctx, x.vfs, nullptr, TILEDB_VFS_WRITE, &vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid vfs mode") {
    auto rc{tiledb_vfs_open(
        x.ctx, nullptr, TEST_URI, tiledb_vfs_mode_t(6), &vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null file handle") {
    auto rc{tiledb_vfs_open(x.ctx, x.vfs, TEST_URI, TILEDB_VFS_WRITE, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_write argument validation", "[capi][vfs]") {
  ordinary_vfs_fh x;
  const void* buffer = nullptr;
  const char* non_null_buffer = "Hello, world!";
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_vfs_write(nullptr, x.vfs_fh, buffer, 0)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs file handle") {
    auto rc{tiledb_vfs_write(x.ctx, nullptr, buffer, 0)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid number of bytes") {
    auto rc{tiledb_vfs_write(x.ctx, x.vfs_fh, non_null_buffer, UINT64_MAX)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_read argument validation", "[capi][vfs]") {
  ordinary_vfs_fh x;
  void* buffer = nullptr;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null context") {
    auto rc{tiledb_vfs_read(nullptr, x.vfs_fh, 0, buffer, 0)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs file handle") {
    auto rc{tiledb_vfs_read(x.ctx, nullptr, 0, buffer, 0)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid offset") {
    auto rc{
        tiledb_vfs_read(x.ctx, x.vfs_fh, static_cast<uint64_t>(-1), buffer, 0)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid number of bytes") {
    auto rc{
        tiledb_vfs_read(x.ctx, x.vfs_fh, 0, buffer, static_cast<uint64_t>(-1))};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_sync argument validation", "[capi][vfs]") {
  ordinary_vfs_fh x;
  SECTION("success") {
    auto rc{tiledb_vfs_sync(x.ctx, x.vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_sync(nullptr, x.vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs file handle") {
    auto rc{tiledb_vfs_sync(x.ctx, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_close argument validation", "[capi][vfs]") {
  ordinary_vfs_fh x;
  SECTION("success") {
    auto rc{tiledb_vfs_close(x.ctx, x.vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_close(nullptr, x.vfs_fh)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs file handle") {
    auto rc{tiledb_vfs_close(x.ctx, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_fh_is_closed argument validation", "[capi][vfs]") {
  ordinary_vfs_fh x;
  int32_t is_closed;
  SECTION("success") {
    auto rc{tiledb_vfs_fh_is_closed(x.ctx, x.vfs_fh, &is_closed)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    auto rc{tiledb_vfs_fh_is_closed(nullptr, x.vfs_fh, &is_closed)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs file handle") {
    auto rc{tiledb_vfs_fh_is_closed(x.ctx, nullptr, &is_closed)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null flag") {
    auto rc{tiledb_vfs_fh_is_closed(x.ctx, x.vfs_fh, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE("C API: tiledb_vfs_fh_free argument validation", "[capi][vfs]") {
  REQUIRE_NOTHROW(tiledb_vfs_fh_free(nullptr));
}

TEST_CASE(
    "C API: tiledb_vfs_open reports error when open fails", "[capi][vfs]") {
  ordinary_vfs x;
  tiledb_vfs_fh_handle_t* vfs_fh;

  tiledb_error_t* error = nullptr;

  capi_return_t rc = tiledb_vfs_open(
      x.ctx, x.vfs, "doesnotexistfile", TILEDB_VFS_READ, &vfs_fh);
  REQUIRE(tiledb_status(rc) == TILEDB_ERR);

  rc = tiledb_ctx_get_last_error(x.ctx, &error);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  CHECK(error != nullptr);
}

TEST_CASE("C API: tiledb_vfs_ls_recursive argument validation", "[capi][vfs]") {
  /*
   * No "success" sections here; too much overhead to set up.
   */
  ordinary_vfs x;
  int32_t data;
  auto cb = [](const char*, size_t, uint64_t, void*) { return 0; };
  SECTION("null context") {
    auto rc{tiledb_vfs_ls_recursive(nullptr, x.vfs, TEST_URI, cb, &data)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null vfs") {
    auto rc{tiledb_vfs_ls_recursive(x.ctx, nullptr, TEST_URI, cb, &data)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    auto rc{tiledb_vfs_ls_recursive(x.ctx, x.vfs, nullptr, cb, &data)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null callback") {
    auto rc{tiledb_vfs_ls_recursive(x.ctx, x.vfs, TEST_URI, nullptr, &data)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null data ptr") {
    auto rc{tiledb_vfs_ls_recursive(x.ctx, x.vfs, TEST_URI, cb, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: VFS recursive ls unsupported backends",
    "[capi][vfs][ls_recursive]") {
  ordinary_vfs vfs;
  int ls_data;
  auto cb = [](const char*, size_t, uint64_t, void*) { return 0; };
  // Recursive ls is currently only supported for S3.
  tiledb::sm::URI uri{GENERATE(
      "file:///path/", "mem:///path/", "azure://path/", "gcs://path/")};
  DYNAMIC_SECTION(
      "Test recursive ls usupported backend over " << uri.backend_name()) {
    if (!vfs.vfs->vfs()->supports_uri_scheme(uri)) {
      return;
    }
    CHECK(
        tiledb_vfs_ls_recursive(vfs.ctx, vfs.vfs, uri.c_str(), cb, &ls_data) ==
        TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: CallbackWrapperCAPI operator() validation",
    "[ls_recursive][callback][wrapper]") {
  tiledb::sm::LsCallback cb = [](const char* path,
                                 size_t path_len,
                                 uint64_t object_size,
                                 void* data) -> int32_t {
    if (object_size > 100) {
      // Throw if object size is greater than 100 bytes.
      throw std::runtime_error("Throwing callback");
    } else if (!std::string(path, path_len).ends_with(".txt")) {
      // Reject non-txt files.
      return 0;
    }
    auto* ls_data = static_cast<tiledb::sm::LsObjects*>(data);
    ls_data->push_back({{path, path_len}, object_size});
    return 1;
  };

  tiledb::sm::LsObjects data{};
  tiledb::sm::CallbackWrapperCAPI wrapper(cb, static_cast<void*>(&data));

  SECTION("Callback return 1 signals to continue traversal") {
    CHECK(wrapper("file.txt", 10) == 1);
    CHECK(data.size() == 1);
  }
  SECTION("Callback return 0 signals to stop traversal") {
    CHECK_THROWS_AS(wrapper("some/dir/", 0) == 0, tiledb::sm::LsStopTraversal);
  }
  SECTION("Callback exception is propagated") {
    CHECK_THROWS_WITH(wrapper("path", 101) == 0, "Throwing callback");
  }
}

TEST_CASE(
    "C API: CallbackWrapperCAPI construction validation",
    "[ls_recursive][callback][wrapper]") {
  using tiledb::sm::CallbackWrapperCAPI;
  tiledb::sm::LsObjects data;
  auto cb = [](const char*, size_t, uint64_t, void*) -> int32_t { return 1; };
  SECTION("Null callback") {
    CHECK_THROWS(CallbackWrapperCAPI(nullptr, &data));
  }
  SECTION("Null data") {
    CHECK_THROWS(CallbackWrapperCAPI(cb, nullptr));
  }
  SECTION("Null callback and data") {
    CHECK_THROWS(CallbackWrapperCAPI(nullptr, nullptr));
  }
  SECTION("Valid callback and data") {
    CHECK_NOTHROW(CallbackWrapperCAPI(cb, &data));
  }
}
