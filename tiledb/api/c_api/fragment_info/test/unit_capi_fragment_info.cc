/**
 * @file tiledb/api/c_api/fragment_info/test/unit_capi_fragment_info.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * Validates the arguments for the FragmentInfo C API.
 */

#define CATCH_CONFIG_MAIN
#include <test/support/tdb_catch.h>
#include "../../../c_api_test_support/testsupport_capi_context.h"
#include "../../../c_api_test_support/testsupport_capi_fragment_info.h"
#include "../fragment_info_api_experimental.h"
#include "../fragment_info_api_external.h"
#include "../fragment_info_api_internal.h"

using namespace tiledb::api::test_support;

const char* TEST_URI = "unit_capi_fragment_info";

TEST_CASE(
    "C API: tiledb_fragment_info_alloc argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_context ctx{};
  tiledb_fragment_info_handle_t* frag_info{};

  SECTION("success") {
    rc = tiledb_fragment_info_alloc(ctx.context, TEST_URI, &frag_info);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    tiledb_fragment_info_free(&frag_info);
    CHECK(frag_info == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_alloc(nullptr, TEST_URI, &frag_info);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("invalid uri") {
    rc = tiledb_fragment_info_alloc(ctx.context, "", &frag_info);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_alloc(ctx.context, TEST_URI, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_free argument validation",
    "[capi][fragment_info]") {
  ordinary_context ctx{};
  tiledb_fragment_info_handle_t* frag_info{};
  auto rc = tiledb_fragment_info_alloc(ctx.context, TEST_URI, &frag_info);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);
  SECTION("success") {
    REQUIRE_NOTHROW(tiledb_fragment_info_free(&frag_info));
    CHECK(frag_info == nullptr);
  }
  SECTION("null fragment_info") {
    /*
     * `tiledb_fragment_info_free` is a void function, otherwise we would check
     * for an error.
     */
    REQUIRE_NOTHROW(tiledb_fragment_info_free(nullptr));
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_set_config argument validation",
    "[capi][fragment_info]") {
  ordinary_fragment_info_without_fragments x{};
  tiledb_config_t* config{};
  tiledb_error_handle_t* error{};
  auto rc = tiledb_config_alloc(&config, &error);
  REQUIRE(tiledb_status(rc) == TILEDB_OK);

  SECTION("success") {
    rc = tiledb_fragment_info_set_config(x.ctx(), x.fragment_info, config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_set_config(nullptr, x.fragment_info, config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_set_config(x.ctx(), nullptr, config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_fragment_info_set_config(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  REQUIRE_NOTHROW(tiledb_config_free(&config));
  CHECK(config == nullptr);
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_config argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info_without_fragments x{};
  tiledb_config_t* config;
  SECTION("success") {
    rc = tiledb_fragment_info_get_config(x.ctx(), x.fragment_info, &config);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_config(nullptr, x.fragment_info, &config);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_config(x.ctx(), nullptr, &config);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null config") {
    rc = tiledb_fragment_info_get_config(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_load argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_context x{};
  std::string array_name =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/non_split_coords_v1_4_0";
  tiledb_fragment_info_handle_t* fragment_info{nullptr};

  // Create fragment info object
  REQUIRE(
      tiledb_fragment_info_alloc(
          x.context, array_name.c_str(), &fragment_info) == TILEDB_OK);

  SECTION("success") {
    rc = tiledb_fragment_info_load(x.context, fragment_info);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    REQUIRE_NOTHROW(tiledb_fragment_info_free(&fragment_info));
    CHECK(fragment_info == nullptr);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_load(nullptr, fragment_info);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_load(x.context, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_name_v2 argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  tiledb_string_t* name;
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_name_v2(
        x.ctx(), x.fragment_info, 0, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_name_v2(
        nullptr, x.fragment_info, 0, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_name_v2(x.ctx(), nullptr, 0, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid index") {
    rc = tiledb_fragment_info_get_fragment_name_v2(x.ctx(), nullptr, -1, &name);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null name") {
    rc = tiledb_fragment_info_get_fragment_name_v2(
        x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_num argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint32_t fragment_num;
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_num(
        x.ctx(), x.fragment_info, &fragment_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(fragment_num == 1);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_num(
        nullptr, x.fragment_info, &fragment_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_num(x.ctx(), nullptr, &fragment_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null fragment_num") {
    rc = tiledb_fragment_info_get_fragment_num(
        x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_uri argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  const char* uri;
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_uri(
        x.ctx(), x.fragment_info, 0, &uri);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_uri(
        nullptr, x.fragment_info, 0, &uri);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_uri(x.ctx(), nullptr, 0, &uri);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_fragment_uri(
        x.ctx(), x.fragment_info, -1, &uri);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    rc = tiledb_fragment_info_get_fragment_uri(
        x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_size argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint64_t size;
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_size(
        x.ctx(), x.fragment_info, 0, &size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_size(
        nullptr, x.fragment_info, 0, &size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_size(x.ctx(), nullptr, 0, &size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_fragment_size(
        x.ctx(), x.fragment_info, -1, &size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null size") {
    rc = tiledb_fragment_info_get_fragment_size(
        x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_dense argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  int32_t dense;
  SECTION("success") {
    rc = tiledb_fragment_info_get_dense(x.ctx(), x.fragment_info, 0, &dense);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(dense == 0);  // the array is not dense.
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_dense(nullptr, x.fragment_info, 0, &dense);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_dense(x.ctx(), nullptr, 0, &dense);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_dense(x.ctx(), x.fragment_info, -1, &dense);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dense") {
    rc = tiledb_fragment_info_get_dense(x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_sparse argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  int32_t sparse;
  SECTION("success") {
    rc = tiledb_fragment_info_get_sparse(x.ctx(), x.fragment_info, 0, &sparse);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(sparse == 1);  // the array is sparse.
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_sparse(nullptr, x.fragment_info, 0, &sparse);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_sparse(x.ctx(), nullptr, 0, &sparse);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_sparse(x.ctx(), x.fragment_info, -1, &sparse);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null sparse") {
    rc = tiledb_fragment_info_get_sparse(x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_timestamp_range argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint64_t start, end;
  SECTION("success") {
    rc = tiledb_fragment_info_get_timestamp_range(
        x.ctx(), x.fragment_info, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(start == end);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_timestamp_range(
        nullptr, x.fragment_info, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_timestamp_range(
        x.ctx(), nullptr, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_timestamp_range(
        x.ctx(), x.fragment_info, -1, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start timestamp") {
    rc = tiledb_fragment_info_get_timestamp_range(
        x.ctx(), x.fragment_info, 0, nullptr, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end timestamp") {
    rc = tiledb_fragment_info_get_timestamp_range(
        x.ctx(), x.fragment_info, 0, &start, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_from_index "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  int64_t domain[2];
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(
        x.ctx(), x.fragment_info, 0, 0, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(
        nullptr, x.fragment_info, 0, 0, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(
        x.ctx(), nullptr, 0, 0, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(
        x.ctx(), x.fragment_info, -1, 0, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension index") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(
        x.ctx(), x.fragment_info, 0, -1, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(
        x.ctx(), x.fragment_info, 0, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_from_name "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  const char* dim_name = "d1";  // the test array's dimension name.
  int64_t domain[2];
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(
        x.ctx(), x.fragment_info, 0, dim_name, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(
        nullptr, x.fragment_info, 0, dim_name, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(
        x.ctx(), nullptr, 0, dim_name, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(
        x.ctx(), x.fragment_info, -1, dim_name, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension name") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(
        x.ctx(), x.fragment_info, 0, "e", &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dimension name") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(
        x.ctx(), x.fragment_info, 0, nullptr, &domain);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null domain") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(
        x.ctx(), x.fragment_info, 0, dim_name, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_size_from_index "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  bool is_var = true;
  ordinary_fragment_info x{is_var};  // use var-sized array
  uint64_t start_size, end_size;
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.fragment_info, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        nullptr, x.fragment_info, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        x.ctx(), nullptr, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.fragment_info, -1, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension index") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.fragment_info, 0, -1, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.fragment_info, 0, 0, nullptr, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
        x.ctx(), x.fragment_info, 0, 0, &start_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_size_from_name "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  bool is_var = true;
  ordinary_fragment_info x{is_var};  // use var-sized array
  const char* dim_name = "d";        // the test array's dimension name.
  uint64_t start_size, end_size;
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.fragment_info, 0, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        nullptr, x.fragment_info, 0, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        x.ctx(), nullptr, 0, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.fragment_info, -1, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension name") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.fragment_info, 0, "e", &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dimension name") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.fragment_info, 0, nullptr, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.fragment_info, 0, dim_name, nullptr, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
        x.ctx(), x.fragment_info, 0, dim_name, &start_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_from_index "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{true};  // use var-sized array
  char start[10], end[10];
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
        x.ctx(), x.fragment_info, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
        nullptr, x.fragment_info, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
        x.ctx(), nullptr, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
        x.ctx(), x.fragment_info, -1, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension index") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
        x.ctx(), x.fragment_info, 0, -1, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
        x.ctx(), x.fragment_info, 0, 0, nullptr, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(
        x.ctx(), x.fragment_info, 0, 0, &start, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_from_name "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{true};  // use var-sized array
  const char* dim_name = "d";      // the test array's dimension name.
  char start[10], end[10];
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        x.ctx(), x.fragment_info, 0, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        nullptr, x.fragment_info, 0, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        x.ctx(), nullptr, 0, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        x.ctx(), x.fragment_info, -1, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension name") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        x.ctx(), x.fragment_info, 0, "e", &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dimension name") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        x.ctx(), x.fragment_info, 0, nullptr, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        x.ctx(), x.fragment_info, 0, 0, nullptr, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(
        x.ctx(), x.fragment_info, 0, 0, &start, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_num argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint64_t mbr_num;
  SECTION("success") {
    rc =
        tiledb_fragment_info_get_mbr_num(x.ctx(), x.fragment_info, 0, &mbr_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(mbr_num == 1);
  }
  SECTION("null context") {
    rc =
        tiledb_fragment_info_get_mbr_num(nullptr, x.fragment_info, 0, &mbr_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_num(x.ctx(), nullptr, 0, &mbr_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_mbr_num(
        x.ctx(), x.fragment_info, -1, &mbr_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null mbr_num") {
    rc = tiledb_fragment_info_get_mbr_num(x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_from_index argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint64_t mbr[2];
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_from_index(
        nullptr, x.fragment_info, 0, 0, 0, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_from_index(
        x.ctx(), nullptr, 0, 0, 0, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_mbr_from_index(
        x.ctx(), x.fragment_info, -1, 0, 0, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid mbr id") {
    rc = tiledb_fragment_info_get_mbr_from_index(
        x.ctx(), x.fragment_info, 0, -1, 0, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension index") {
    rc = tiledb_fragment_info_get_mbr_from_index(
        x.ctx(), x.fragment_info, 0, 0, -1, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null mbr") {
    rc = tiledb_fragment_info_get_mbr_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_from_name argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  const char* dim_name = "d1";  // the test array's dimension name.
  uint64_t mbr[2];
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_from_name(
        nullptr, x.fragment_info, 0, 0, dim_name, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_from_name(
        x.ctx(), nullptr, 0, 0, dim_name, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_mbr_from_name(
        x.ctx(), x.fragment_info, -1, 0, dim_name, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid mbr index") {
    rc = tiledb_fragment_info_get_mbr_from_name(
        x.ctx(), x.fragment_info, 0, -1, dim_name, &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension name") {
    rc = tiledb_fragment_info_get_mbr_from_name(
        x.ctx(), x.fragment_info, 0, 0, "e", &mbr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null mbr") {
    rc = tiledb_fragment_info_get_mbr_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_size_from_index "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{true};  // use var-sized array
  uint64_t start_size, end_size;
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        nullptr, x.fragment_info, 0, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        x.ctx(), nullptr, 0, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        x.ctx(), x.fragment_info, -1, 0, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid mbr id") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        x.ctx(), x.fragment_info, 0, -1, 0, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension index") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        x.ctx(), x.fragment_info, 0, 0, -1, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, nullptr, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, &start_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_size_from_name "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{true};  // use var-sized array
  const char* dim_name = "d";      // the test array's dimension name.
  uint64_t start_size, end_size;
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        nullptr, x.fragment_info, 0, 0, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), nullptr, 0, 0, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), x.fragment_info, -1, 0, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid mbr id") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), x.fragment_info, 0, -1, dim_name, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension name") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), x.fragment_info, 0, 0, "e", &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dimension name") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), x.fragment_info, 0, 0, nullptr, &start_size, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start_size") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, nullptr, &end_size);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end_size") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, &start_size, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_from_index argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{true};  // use var-sized array
  char start[10], end[10];
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        nullptr, x.fragment_info, 0, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        x.ctx(), nullptr, 0, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        x.ctx(), x.fragment_info, -1, 0, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid mbr id") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        x.ctx(), x.fragment_info, 0, -1, 0, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension index") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        x.ctx(), x.fragment_info, 0, 0, -1, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, nullptr, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(
        x.ctx(), x.fragment_info, 0, 0, 0, &start, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_from_name argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{true};  // use var-sized array
  const char* dim_name = "d";      // the test array's dimension name.
  char start[10], end[10];
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        nullptr, x.fragment_info, 0, 0, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), nullptr, 0, 0, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), x.fragment_info, -1, 0, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid mbr index") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), x.fragment_info, 0, -1, dim_name, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid dimension name") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), x.fragment_info, 0, 0, "e", &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null dimension name") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), x.fragment_info, 0, 0, nullptr, &start, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null start") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, nullptr, &end);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null end") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(
        x.ctx(), x.fragment_info, 0, 0, dim_name, &start, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_cell_num argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint64_t cell_num;
  SECTION("success") {
    rc = tiledb_fragment_info_get_cell_num(
        x.ctx(), x.fragment_info, 0, &cell_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_cell_num(
        nullptr, x.fragment_info, 0, &cell_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_cell_num(x.ctx(), nullptr, 0, &cell_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_cell_num(
        x.ctx(), x.fragment_info, -1, &cell_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null cell_num") {
    rc =
        tiledb_fragment_info_get_cell_num(x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_total_cell_num argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint64_t cell_num;
  SECTION("success") {
    rc = tiledb_fragment_info_get_total_cell_num(
        x.ctx(), x.fragment_info, &cell_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_total_cell_num(
        nullptr, x.fragment_info, &cell_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_total_cell_num(x.ctx(), nullptr, &cell_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null cell_num") {
    rc = tiledb_fragment_info_get_total_cell_num(
        x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_version argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint32_t version;
  SECTION("success") {
    rc =
        tiledb_fragment_info_get_version(x.ctx(), x.fragment_info, 0, &version);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc =
        tiledb_fragment_info_get_version(nullptr, x.fragment_info, 0, &version);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_version(x.ctx(), nullptr, 0, &version);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_version(
        x.ctx(), x.fragment_info, -1, &version);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null version") {
    rc = tiledb_fragment_info_get_version(x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_has_consolidated_metadata argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  int32_t has;
  SECTION("success") {
    rc = tiledb_fragment_info_has_consolidated_metadata(
        x.ctx(), x.fragment_info, 0, &has);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_has_consolidated_metadata(
        nullptr, x.fragment_info, 0, &has);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_has_consolidated_metadata(
        x.ctx(), nullptr, 0, &has);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_has_consolidated_metadata(
        x.ctx(), x.fragment_info, -1, &has);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null has_consolidated_metadata") {
    rc = tiledb_fragment_info_has_consolidated_metadata(
        x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_unconsolidated_metadata_num "
    "argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint32_t unconsolidated;
  SECTION("success") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
        x.ctx(), x.fragment_info, &unconsolidated);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
        nullptr, x.fragment_info, &unconsolidated);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
        x.ctx(), nullptr, &unconsolidated);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null unconsolidated") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(
        x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_to_vacuum_num argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  uint32_t to_vacuum_num;
  SECTION("success") {
    rc = tiledb_fragment_info_get_to_vacuum_num(
        x.ctx(), x.fragment_info, &to_vacuum_num);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    CHECK(to_vacuum_num == 0);  // there are no fragments to vacuum.
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_to_vacuum_num(
        nullptr, x.fragment_info, &to_vacuum_num);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_to_vacuum_num(
        x.ctx(), nullptr, &to_vacuum_num);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null to_vacuum_num") {
    rc = tiledb_fragment_info_get_to_vacuum_num(
        x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_to_vacuum_uri argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  const char* uri;
  /*
   * No "success" section here; There are no fragments to vacuum.
   */
  SECTION("null context") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(
        nullptr, x.fragment_info, 0, &uri);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(x.ctx(), nullptr, 0, &uri);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(
        x.ctx(), x.fragment_info, -1, &uri);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null uri") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(
        x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_array_schema argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  tiledb_array_schema_t* schema;
  SECTION("success") {
    rc = tiledb_fragment_info_get_array_schema(
        x.ctx(), x.fragment_info, 0, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_array_schema(
        nullptr, x.fragment_info, 0, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_array_schema(x.ctx(), nullptr, 0, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_array_schema(
        x.ctx(), x.fragment_info, -1, &schema);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null array_schema") {
    rc = tiledb_fragment_info_get_array_schema(
        x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_get_array_schema_name argument validation",
    "[capi][fragment_info]") {
  capi_return_t rc;
  ordinary_fragment_info x{};
  const char* schema_name;
  SECTION("success") {
    rc = tiledb_fragment_info_get_array_schema_name(
        x.ctx(), x.fragment_info, 0, &schema_name);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_array_schema_name(
        nullptr, x.fragment_info, 0, &schema_name);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_array_schema_name(
        x.ctx(), nullptr, 0, &schema_name);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("invalid fragment_id") {
    rc = tiledb_fragment_info_get_array_schema_name(
        x.ctx(), x.fragment_info, -1, &schema_name);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null schema_name") {
    rc = tiledb_fragment_info_get_array_schema_name(
        x.ctx(), x.fragment_info, 0, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_fragment_info_dump argument validation",
    "[capi][fragment_info]") {
  /**
   * This function is not conducive to a cross-platform test,
   * as it requires a FILE*. Thus, this test case shall be omitted.
   */
}
