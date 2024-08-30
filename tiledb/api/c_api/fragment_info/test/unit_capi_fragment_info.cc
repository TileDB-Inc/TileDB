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
// #include "../../../c_api_test_support/testsupport_capi_fragment_info.h"
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

/*TEST_CASE(
    "C API: tiledb_fragment_info_set_config argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_set_config(
    tiledb_fragment_info_t* fragment_info, tiledb_config_t* config)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_set_config(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_set_config(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_set_config(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_set_config(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_config argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_config(
    tiledb_fragment_info_t* fragment_info, tiledb_config_t** config)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_config(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_config(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_config(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_config(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_load argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_load(tiledb_fragment_info_t* fragment_info)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_load(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_load(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_load(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_load(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_name_v2 argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_fragment_name_v2(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_string_t** name)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_name_v2(x.ctx(), x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_name_v2(nullptr, x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_name_v2(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_fragment_name_v2(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_num argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_fragment_num(
    tiledb_fragment_info_t* fragment_info, uint32_t* fragment_num)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_num(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_num(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_num(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_fragment_num(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_uri argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_fragment_uri(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, const char** uri)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_uri(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_uri(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_uri(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_fragment_uri(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_fragment_size argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_fragment_size(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint64_t* size)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_fragment_size(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_fragment_size(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_fragment_size(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_fragment_size(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_dense argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_dense(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, int32_t* dense)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_dense(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_dense(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_dense(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_dense(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_sparse argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_sparse(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, int32_t* sparse)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_sparse(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_sparse(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_sparse(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_sparse(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_timestamp_range argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_timestamp_range(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* start,
    uint64_t* end)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_timestamp_range(x.ctx(), x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_timestamp_range(nullptr, x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_timestamp_range(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_timestamp_range(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_from_index argument
validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_non_empty_domain_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* domain)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(x.ctx(), nullptr,
attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_index(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_from_name argument
validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_non_empty_domain_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* domain)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(x.ctx(), nullptr,
attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_non_empty_domain_from_name(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_size_from_index
argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(x.ctx(),
nullptr, attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_index(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_size_from_name
argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(x.ctx(),
nullptr, attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_size_from_name(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_from_index argument
validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_non_empty_domain_var_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* start,
    void* end)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(x.ctx(),
nullptr, attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_index(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_non_empty_domain_var_from_name argument
validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_non_empty_domain_var_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* start,
    void* end)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(x.ctx(),
nullptr, attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_non_empty_domain_var_from_name(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_num argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_mbr_num(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint64_t* mbr_num)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_num(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_num(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_num(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_mbr_num(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_from_index argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_mbr_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* mbr)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_from_index(x.ctx(), x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_from_index(nullptr, x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_from_index(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_mbr_from_index(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_from_name argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_mbr_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* mbr)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_from_name(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_from_name(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_from_name(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_mbr_from_name(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_size_from_index argument
validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_mbr_var_size_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(x.ctx(), nullptr,
attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_index(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_size_from_name argument
validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_mbr_var_size_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(x.ctx(), nullptr,
attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_mbr_var_size_from_name(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_from_index argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_mbr_var_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* start,
    void* end)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(x.ctx(), x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(nullptr, x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_mbr_var_from_index(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_mbr_var_from_name argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_mbr_var_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* start,
    void* end)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(x.ctx(), x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(nullptr, x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_mbr_var_from_name(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_cell_num argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_cell_num(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint64_t* cell_num)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_cell_num(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_cell_num(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_cell_num(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_cell_num(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_total_cell_num argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_total_cell_num(
    tiledb_fragment_info_t* fragment_info, uint64_t* cell_num)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_total_cell_num(x.ctx(), x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_total_cell_num(nullptr, x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_total_cell_num(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_total_cell_num(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_version argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_version(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint32_t* version)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_version(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_version(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_version(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_version(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_has_consolidated_metadata argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_has_consolidated_metadata(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, int32_t* has)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_has_consolidated_metadata(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_has_consolidated_metadata(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_has_consolidated_metadata(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_has_consolidated_metadata(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_unconsolidated_metadata_num argument
validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_unconsolidated_metadata_num(
    tiledb_fragment_info_t* fragment_info, uint32_t* unconsolidated)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(x.ctx(),
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(nullptr,
x.fragment_info, attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(x.ctx(), nullptr,
attr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_unconsolidated_metadata_num(x.ctx(),
x.fragment_info, nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_to_vacuum_num argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_to_vacuum_num(
    tiledb_fragment_info_t* fragment_info, uint32_t* to_vacuum_num)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_to_vacuum_num(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_to_vacuum_num(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_to_vacuum_num(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_to_vacuum_num(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_to_vacuum_uri argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_to_vacuum_uri(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, const char** uri)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_to_vacuum_uri(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_array_schema argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_array_schema(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_array_schema_t** array_schema)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_array_schema(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_array_schema(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_array_schema(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_array_schema(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_get_array_schema_name argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_get_array_schema_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** schema_name)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_get_array_schema_name(x.ctx(), x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_get_array_schema_name(nullptr, x.fragment_info,
attr); REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_get_array_schema_name(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_get_array_schema_name(x.ctx(), x.fragment_info,
nullptr); REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/

/*TEST_CASE(
    "C API: tiledb_fragment_info_dump argument validation",
    "[capi][fragment_info]") {

  tiledb_fragment_info_dump(
    const tiledb_fragment_info_t* fragment_info, FILE* out)


  capi_return_t rc;
  ordinary_fragment_info x{};
  SECTION("success") {
    rc = tiledb_fragment_info_dump(x.ctx(), x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null context") {
    rc = tiledb_fragment_info_dump(nullptr, x.fragment_info, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_CONTEXT);
  }
  SECTION("null fragment_info") {
    rc = tiledb_fragment_info_dump(x.ctx(), nullptr, attr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null attribute") {
    rc = tiledb_fragment_info_dump(x.ctx(), x.fragment_info, nullptr);
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
  }
}*/
