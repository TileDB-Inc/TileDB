/**
 * @file tiledb/storage_format/uri/test/unit_uri_format.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * Tests for the storage format's uri manipulation APIs.
 */

#include <test/support/tdb_catch.h>
#include <locale>

#include "../generate_uri.h"
#include "../parse_uri.h"
#include "tiledb/common/scoped_executor.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::storage_format;
using namespace tiledb::sm::utils::parse;

TEST_CASE(
    "StorageFormat: Generate: generate_timestamped_name",
    "[storage_format][generate][generate_timestamped_name]") {
  // Note: all format versions will generate the same format of fragment name
  uint64_t start = 1;
  uint64_t end = 2;
  format_version_t format_version = 5;
  std::string name;

  SECTION("one timestamp") {
    // Generate new fragment name
    name = generate_timestamped_name(start, format_version);

    // Check new fragment name's timestamp range
    auto name_timestamps{name.substr(0, 6)};
    CHECK(name_timestamps == "/__1_1");

    // Check new fragment name's format version
    auto name_version{name.substr(name.find_last_of("_"))};
    CHECK(name_version == "_5");
  }

  SECTION("two timestamps") {
    SECTION("with format version") {
      // Generate new fragment name
      name = generate_timestamped_name(start, end, format_version);

      // Check new fragment name's format version
      auto name_version{name.substr(name.find_last_of("_"))};
      CHECK(name_version == "_5");
    }

    SECTION("without format version") {
      // Generate new fragment name
      name = generate_timestamped_name(start, end, std::nullopt);

      // Ensure new fragment name has no format version (ends in uuid)
      // Precautionary measure: Check that the length is greater than 10
      // Note that format versions are currently no longer than 2 chars
      auto name_end{name.substr(name.find_last_of("_"))};
      CHECK(name_end.length() > 5);
    }

    // Check new fragment name's timestamp range
    auto name_timestamps{name.substr(0, 6)};
    CHECK(name_timestamps == "/__1_2");
  }
}

TEST_CASE(
    "StorageFormat: Generate: locale-independent timestamped names",
    "[storage_format][generate][generate_timestamped_name][locale]") {
  const bool grouped = GENERATE(false, true);
  const auto version = GENERATE(
      std::optional<format_version_t>{}, std::optional<format_version_t>{1234});
  std::string name;
  {
    struct GroupedNumbers : std::numpunct<char> {
      char do_thousands_sep() const override {
        return '.';
      }
      std::string do_grouping() const override {
        return "\3";
      }
    };

    const auto previous_locale = std::locale();
    const tiledb::common::ScopedExecutor restore_locale(
        [&]() { std::locale::global(previous_locale); });
    std::locale::global(
        grouped ? std::locale(std::locale::classic(), new GroupedNumbers) :
                  std::locale::classic());
    name = generate_timestamped_name(123456789, 123456790, version);
  }

  const std::string prefix = "/__123456789_123456790_";
  CHECK(name.substr(0, prefix.size()) == prefix);
  CHECK(name.size() == prefix.size() + 32 + (version.has_value() ? 5 : 0));
  const auto label = name.substr(prefix.size(), 32);
  CHECK(label.find_first_not_of("0123456789abcdef") == std::string::npos);
  if (version.has_value()) {
    CHECK(name.substr(name.find_last_of('_')) == "_1234");
  }
}

TEST_CASE(
    "StorageFormat: Generate: generate_consolidated_fragment_name",
    "[storage_format][generate][generate_consolidated_fragment_name]") {
  // Create fragments at timestamps 1 and 2
  auto start = tiledb::sm::URI(
      "file:///array_name/__fragments/__1_1_44318efd44f546b18db13edc8d10805b");
  auto end = tiledb::sm::URI(
      "file:///array_name/__fragments/__2_2_44318efd44f546b18db13edc8d10806c");

  SECTION("valid, unique timestamps") {
    // Generate fragment name for fragments at timestamps 1, 2
    auto name{generate_consolidated_fragment_name(
        start, end, constants::format_version)};

    // Check new fragment name timestamp range
    auto name_substr{name.substr(name.find_last_of("/"), 6)};
    CHECK(name_substr == "/__1_2");
  }

  SECTION("valid, same timestamps") {
    // Generate fragment name for fragments at timestamps 1, 1
    auto name{generate_consolidated_fragment_name(
        start, start, constants::format_version)};

    // Check new fragment name timestamp range
    auto name_substr{name.substr(name.find_last_of("/"), 6)};
    CHECK(name_substr == "/__1_1");
  }

  SECTION("invalid timestamps") {
    // Try to generate fragment name for fragments at timestamps 2, 1
    REQUIRE_THROWS_WITH(
        generate_consolidated_fragment_name(
            end, start, constants::format_version),
        Catch::Matchers::ContainsSubstring(
            "start timestamp cannot be after end timestamp"));
  }
}
