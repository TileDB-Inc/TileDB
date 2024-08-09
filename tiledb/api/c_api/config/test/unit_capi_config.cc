/**
 * @file tiledb/api/c_api/config/test/unit_capi_config.cc
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
 * @section DESCRIPTION
 */

#include <catch2/catch_test_macros.hpp>
#include "../config_api_external.h"

TEST_CASE("C API: tiledb_config_alloc argument validation", "[capi][config]") {
  tiledb_error_handle_t* error{nullptr};
  tiledb_config_handle_t* config{nullptr};

  SECTION("success") {
    capi_return_t rc{tiledb_config_alloc(&config, &error)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    REQUIRE(config != nullptr);
    CHECK(error == nullptr);
    tiledb_config_free(&config);
  }
  SECTION("null config pointer") {
    capi_return_t rc{tiledb_config_alloc(nullptr, &error)};
    REQUIRE(tiledb_status(rc) == TILEDB_ERR);
    CHECK(error != nullptr);
  }
  SECTION("null error pointer") {
    capi_return_t rc{tiledb_config_alloc(&config, nullptr)};
    REQUIRE(tiledb_status(rc) == TILEDB_INVALID_ERROR);
    CHECK(config == nullptr);
  }
}

struct ordinary_config {
  tiledb_config_handle_t* config;
  tiledb_error_handle_t* error;
  ordinary_config()
      : config{nullptr}
      , error{nullptr} {
    capi_return_t rc{tiledb_config_alloc(&config, &error)};
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    if (tiledb_status(rc) != TILEDB_OK) {
      throw std::runtime_error("can't set up ordinary_config");
    }
  }
  ~ordinary_config() {
    if (config != nullptr) {
      tiledb_config_free(&config);
    }
    if (error != nullptr) {
      tiledb_error_free(&error);
    }
  }
};

TEST_CASE("C API: tiledb_config_free argument validation", "[capi][config]") {
  /*
   * tiledb_config_free is a void function. We cannot check the return values,
   * but we can verify that the calls don't throw.
   */
  SECTION("success") {
    ordinary_config x;
    REQUIRE_NOTHROW(tiledb_config_free(&x.config));
  }
  SECTION("null config argument") {
    REQUIRE_NOTHROW(tiledb_config_free(nullptr));
  }
  SECTION("non-null bad config") {
    tiledb_config_handle_t* config{nullptr};
    REQUIRE_NOTHROW(tiledb_config_free(&config));
  }
}

TEST_CASE("C API: tiledb_config_set argument validation", "[capi][config]") {
  ordinary_config x;
  SECTION("success") {
    auto rc{tiledb_config_set(x.config, "foo", "bar", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
    CHECK(x.error == nullptr);
  }
  SECTION("null config") {
    auto rc{tiledb_config_set(nullptr, "foo", "bar", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null param") {
    auto rc{tiledb_config_set(x.config, nullptr, "bar", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    auto rc{tiledb_config_set(x.config, "foo", nullptr, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_set(x.config, "foo", "bar", nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE("C API: tiledb_config_get argument validation", "[capi][config]") {
  ordinary_config x;
  auto rc{tiledb_config_set(x.config, "foo", "bar", &x.error)};
  REQUIRE(rc == TILEDB_OK);
  const char* output_value;
  SECTION("success") {
    rc = tiledb_config_get(x.config, "foo", &output_value, &x.error);
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null config") {
    rc = tiledb_config_get(nullptr, "foo", &output_value, &x.error);
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null param") {
    rc = tiledb_config_get(x.config, nullptr, &output_value, &x.error);
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null output value") {
    rc = tiledb_config_get(x.config, "foo", nullptr, &x.error);
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    rc = tiledb_config_get(x.config, "foo", &output_value, nullptr);
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE("C API: tiledb_config_unset argument validation", "[capi][config]") {
  ordinary_config x;
  auto rc{tiledb_config_set(x.config, "foo", "bar", &x.error)};
  REQUIRE(rc == TILEDB_OK);
  SECTION("success") {
    rc = tiledb_config_unset(x.config, "foo", &x.error);
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null config") {
    rc = tiledb_config_unset(nullptr, "foo", &x.error);
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null param") {
    rc = tiledb_config_unset(x.config, nullptr, &x.error);
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    rc = tiledb_config_unset(x.config, "foo", nullptr);
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE(
    "C API: tiledb_config_load_from_file argument validation",
    "[capi][config]") {
  ordinary_config x;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null config") {
    auto rc{tiledb_config_load_from_file(nullptr, "foo", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filename") {
    auto rc{tiledb_config_load_from_file(x.config, nullptr, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_load_from_file(x.config, "foo", nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE(
    "C API: tiledb_config_save_to_file argument validation", "[capi][config]") {
  ordinary_config x;
  /*
   * No "success" sections here; too much overhead to set up.
   */
  SECTION("null config") {
    auto rc{tiledb_config_save_to_file(nullptr, "foo", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null filename") {
    auto rc{tiledb_config_save_to_file(x.config, nullptr, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_save_to_file(x.config, "foo", nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE(
    "C API: tiledb_config_compare argument validation", "[capi][config]") {
  ordinary_config x, y;  // Empty configurations are equal.
  uint8_t result;
  SECTION("success") {
    auto rc{tiledb_config_compare(x.config, y.config, &result)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null lhs") {
    auto rc{tiledb_config_compare(nullptr, y.config, &result)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null rhs") {
    auto rc{tiledb_config_compare(x.config, nullptr, &result)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null result") {
    auto rc{tiledb_config_compare(x.config, y.config, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
}

TEST_CASE(
    "C API: tiledb_config_iter_alloc argument validation", "[capi][config]") {
  ordinary_config x;
  tiledb_config_iter_t* i;
  SECTION("success") {
    auto rc{tiledb_config_iter_alloc(x.config, "", &i, &x.error)};
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_iter_free(&i);
  }
  SECTION("null config") {
    auto rc{tiledb_config_iter_alloc(nullptr, "", &i, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  /*
   * No test for null prefix; that's legal. (It's mapped to an empty string.)
   */
  SECTION("null output iterator") {
    auto rc{tiledb_config_iter_alloc(x.config, "", nullptr, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_iter_alloc(x.config, "", &i, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

struct ordinary_config_with_iterator : public ordinary_config {
  tiledb_config_iter_t* iterator;
  ordinary_config_with_iterator()
      : ordinary_config{}
      , iterator{nullptr} {
    auto rc{tiledb_config_iter_alloc(
        ordinary_config::config, "", &iterator, &(ordinary_config::error))};
    REQUIRE(tiledb_status(rc) == TILEDB_OK);
    if (rc != TILEDB_OK) {
      throw std::runtime_error("can't set up ordinary_config_iter");
    }
  }
  ~ordinary_config_with_iterator() {
    if (iterator != nullptr) {
      tiledb_config_iter_free(&iterator);
    }
  }
};

TEST_CASE(
    "C API: tiledb_config_iter_reset argument validation", "[capi][config]") {
  ordinary_config_with_iterator x;
  ordinary_config y;
  SECTION("success") {
    auto rc{tiledb_config_iter_reset(y.config, x.iterator, "", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null config") {
    auto rc{tiledb_config_iter_reset(nullptr, x.iterator, "", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null iterator") {
    auto rc{tiledb_config_iter_reset(y.config, nullptr, "", &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_iter_reset(y.config, x.iterator, "", nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE(
    "C API: tiledb_config_iter_free argument validation", "[capi][config]") {
  SECTION("null iterator") {
    CHECK_NOTHROW(tiledb_config_iter_free(nullptr));
  }
}

TEST_CASE(
    "C API: tiledb_config_iter_here argument validation", "[capi][config]") {
  ordinary_config_with_iterator x;
  const char* param;
  const char* value;
  SECTION("success") {
    auto rc{tiledb_config_iter_here(x.iterator, &param, &value, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null iterator") {
    auto rc{tiledb_config_iter_here(nullptr, &param, &value, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null param") {
    auto rc{tiledb_config_iter_here(x.iterator, nullptr, &value, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null value") {
    auto rc{tiledb_config_iter_here(x.iterator, &param, nullptr, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_iter_here(x.iterator, &param, &value, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE(
    "C API: tiledb_config_iter_next argument validation", "[capi][config]") {
  ordinary_config_with_iterator x;
  SECTION("success") {
    auto rc{tiledb_config_iter_next(x.iterator, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null iterator") {
    auto rc{tiledb_config_iter_next(nullptr, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_iter_next(x.iterator, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}

TEST_CASE(
    "C API: tiledb_config_iter_done argument validation", "[capi][config]") {
  ordinary_config_with_iterator x;
  int32_t done;
  SECTION("success") {
    auto rc{tiledb_config_iter_done(x.iterator, &done, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_OK);
  }
  SECTION("null iterator") {
    auto rc{tiledb_config_iter_done(nullptr, &done, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null done") {
    auto rc{tiledb_config_iter_done(x.iterator, nullptr, &x.error)};
    CHECK(tiledb_status(rc) == TILEDB_ERR);
  }
  SECTION("null error") {
    auto rc{tiledb_config_iter_done(x.iterator, &done, nullptr)};
    CHECK(tiledb_status(rc) == TILEDB_INVALID_ERROR);
  }
}
