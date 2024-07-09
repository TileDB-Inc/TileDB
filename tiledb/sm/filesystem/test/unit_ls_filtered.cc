/**
 * @file unit_ls_filtered.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Tests internal ls recursive filter.
 */

#include <test/support/tdb_catch.h>
#include <filesystem>
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/vfs.h"

namespace tiledb::sm {
/**
 * @return true if the URI represents a regular file, false if not
 */
[[maybe_unused]] static bool accept_only_regular_files(
    const std::string_view& uri, uint64_t) {
  const std::string path = URI(uri).to_path();
  return std::filesystem::is_regular_file(path);
}

}  // namespace tiledb::sm

class VFSTest {
 public:
  /**
   * Requires derived class to create a temporary directory.
   *
   * @param test_tree Vector used to build test directory and objects.
   *    For each element we create a nested directory with N objects.
   * @param prefix The URI prefix to use for the test directory.
   */
  explicit VFSTest(
      const std::vector<size_t>& test_tree, const std::string& prefix)
      : stats_("unit_ls_filtered")
      , io_(4)
      , compute_(4)
      , vfs_(&stats_, &io_, &compute_, tiledb::sm::Config())
      , test_tree_(test_tree)
      , prefix_(prefix)
      , temp_dir_(prefix_)
      , init_open_files_(count_open_files()) {
  }

  virtual ~VFSTest() {
    bool is_dir = false;
    vfs_.is_dir(temp_dir_, &is_dir).ok();
    if (is_dir) {
      vfs_.remove_dir(temp_dir_).ok();
    }
  }

  Status mkdir() const {
    return vfs_.create_dir(temp_dir_);
  }

#ifdef __windows__
  std::optional<uint64_t> count_open_files() const {
    return std::nullopt;
  }
#else
  std::optional<uint64_t> count_open_files() const {
    const std::string fddir = "/proc/" + std::to_string(getpid()) + "/fd";

    std::vector<tiledb::sm::URI> ls;
    const auto st = vfs_.ls(tiledb::sm::URI(fddir), &ls);
    REQUIRE(st.ok());
    return ls.size();
  }
#endif

  /**
   * @return true if the number of open files is the same
   * as it was when we started the test
   */
  bool check_open_files() const {
    const auto maybe_updated_open_files = count_open_files();
    if (maybe_updated_open_files) {
      const uint64_t updated_open_files = *maybe_updated_open_files;
      return (updated_open_files == init_open_files_);
    } else {
      /* not enough information to say otherwise */
      return true;
    }
  }

  /** Resources needed to construct VFS */
  tiledb::sm::stats::Stats stats_;
  ThreadPool io_, compute_;
  tiledb::sm::VFS vfs_;

  std::vector<size_t> test_tree_;
  std::string prefix_;
  tiledb::sm::URI temp_dir_;

  std::optional<uint64_t> init_open_files_;

 private:
  tiledb::sm::LsObjects expected_results_;
};

/**
 * Represents a path used in the test.
 * Encapsulates absolute and relative paths, and can be extended for URI
 * if we determine that `ls_recursive` should output that instead.
 */
struct TestPath {
  VFSTest& vfs_test;
  std::filesystem::path relpath;
  std::filesystem::path abspath;
  uint64_t size;

  TestPath(VFSTest& vfs_test, std::string_view relpath, uint64_t size = 0)
      : vfs_test(vfs_test)
      , relpath(relpath)
      , abspath(
            std::filesystem::path(vfs_test.temp_dir_.to_path()).append(relpath))
      , size(size) {
  }

  TestPath(const TestPath& copy)
      : vfs_test(copy.vfs_test)
      , relpath(copy.relpath)
      , abspath(copy.abspath)
      , size(copy.size) {
  }

  /**
   * Create a file at the test path.
   * @param mkdirs if true, then also create each parent directory in the path
   */
  void touch(bool mkdirs = false) {
    if (mkdirs) {
      std::vector<tiledb::sm::URI> parents;

      tiledb::sm::URI absuri(abspath.string());
      do {
        absuri = absuri.parent_path();
        parents.push_back(absuri);
      } while (absuri != vfs_test.temp_dir_);

      parents.pop_back(); /* temp_dir_ */
      while (!parents.empty()) {
        REQUIRE(vfs_test.vfs_.create_dir(parents.back()).ok());
        parents.pop_back();
      }
    }
    REQUIRE(vfs_test.vfs_.touch(tiledb::sm::URI(abspath.string())).ok());
    std::filesystem::resize_file(abspath, size);
  }

  void mkdir() {
    REQUIRE(vfs_test.vfs_.create_dir(tiledb::sm::URI(abspath.string())).ok());
  }

  /**
   * @return a string containing the way this is expected
   * to appear in the ls_recursive output
   */
  std::string lsresult() const {
    return tiledb::sm::URI(abspath.string()).to_string();
  }

  bool matches(const std::pair<std::string, uint64_t>& lsout) const {
    return (lsresult() == lsout.first && size == lsout.second);
  }
};

tiledb::sm::LsObjects sort_by_name(const tiledb::sm::LsObjects& in_objs) {
  tiledb::sm::LsObjects out_objs(in_objs);
  std::sort(
      out_objs.begin(), out_objs.end(), [](const auto& f1, const auto& f2) {
        return f1.first < f2.first;
      });
  return out_objs;
}

TEST_CASE("VFS: ls_recursive unfiltered", "[vfs][ls_recursive]") {
  std::string prefix = GENERATE("file://");
  prefix += std::filesystem::current_path().string() + "/ls_recursive_test/";

  VFSTest vfs_test({0}, prefix);
  const auto mkst = vfs_test.mkdir();
  REQUIRE(mkst.ok());

  std::vector<TestPath> testpaths = {
      TestPath(vfs_test, "a1.txt", 30),
      TestPath(vfs_test, "a2.txt", 40),
      TestPath(vfs_test, "f1.txt", 10),
      TestPath(vfs_test, "f2.txt", 20),
      TestPath(vfs_test, "d1/f1.txt", 45),
      TestPath(vfs_test, "d1/c1.txt", 55),
      TestPath(vfs_test, "d1/d1sub1/d1sub1sub1/g1.txt", 33),
      TestPath(vfs_test, "d1/d1sub1/d1sub1sub1/d1sub1sub1sub1/b1.txt", 12),
      TestPath(vfs_test, "d1/d1sub1/d1sub1sub1/d1sub1sub1sub1/h1.txt", 33),
  };

  SECTION("Empty directory") {
    const auto ls = sort_by_name(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_all_files,
        tiledb::sm::accept_all_dirs));
    CHECK(ls.empty());
  }

  SECTION("Files only") {
    testpaths[0].touch();
    testpaths[1].touch();
    testpaths[2].touch();
    testpaths[3].touch();

    const auto ls = sort_by_name(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_all_files,
        tiledb::sm::accept_all_dirs));
    REQUIRE(ls.size() == 4);
    CHECK(testpaths[0].matches(ls[0]));
    CHECK(testpaths[1].matches(ls[1]));
    CHECK(testpaths[2].matches(ls[2]));
    CHECK(testpaths[3].matches(ls[3]));
  }

  SECTION("Empty subdirectory") {
    auto d1 = TestPath(vfs_test, "d1");
    d1.mkdir();

    const auto ls = sort_by_name(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_all_files,
        tiledb::sm::accept_all_dirs));

    CHECK(ls.size() == 1);
    if (ls.size() >= 1) {
      CHECK(d1.matches(ls[0]));
    }
  }

  SECTION("Empty subdirectory and files") {
    testpaths[0].touch();
    testpaths[1].touch();
    auto d1 = TestPath(vfs_test, "d1");
    d1.mkdir();
    testpaths[2].touch();
    testpaths[3].touch();

    const auto ls = sort_by_name(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_all_files,
        tiledb::sm::accept_all_dirs));
    CHECK(ls.size() == 5);
    if (ls.size() >= 1) {
      CHECK(testpaths[0].matches(ls[0]));
    }
    if (ls.size() >= 2) {
      CHECK(testpaths[1].matches(ls[1]));
    }
    if (ls.size() >= 3) {
      CHECK(d1.matches(ls[2]));
    }
    if (ls.size() >= 4) {
      CHECK(testpaths[2].matches(ls[3]));
    }
    if (ls.size() >= 5) {
      CHECK(testpaths[3].matches(ls[4]));
    }
  }

  SECTION("Empty sub-subdirectory") {
    auto d1 = TestPath(vfs_test, "d1");
    auto d1sub1 = TestPath(vfs_test, "d1/d1sub1");
    d1.mkdir();
    d1sub1.mkdir();

    const auto ls = sort_by_name(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_all_files,
        tiledb::sm::accept_all_dirs));

    CHECK(ls.size() == 2);
    if (ls.size() >= 1) {
      CHECK(d1.matches(ls[0]));
    }
    if (ls.size() >= 2) {
      CHECK(d1sub1.matches(ls[1]));
    }
  }

  SECTION("Deeply-nested files") {
    auto d1 = TestPath(vfs_test, "d1");
    auto d1sub1 = TestPath(vfs_test, "d1/d1sub1");
    auto d1sub1sub1 = TestPath(vfs_test, "d1/d1sub1/d1sub1sub1");
    auto d1sub1sub1sub1 =
        TestPath(vfs_test, "d1/d1sub1/d1sub1sub1/d1sub1sub1sub1");
    d1.mkdir();
    d1sub1.mkdir();
    d1sub1sub1.mkdir();
    d1sub1sub1sub1.mkdir();
    testpaths[7].touch();

    const auto ls = sort_by_name(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_all_files,
        tiledb::sm::accept_all_dirs));

    CHECK(ls.size() == 5);
    if (ls.size() >= 1) {
      CHECK(d1.matches(ls[0]));
    }
    if (ls.size() >= 2) {
      CHECK(d1sub1.matches(ls[1]));
    }
    if (ls.size() >= 3) {
      CHECK(d1sub1sub1.matches(ls[2]));
    }
    if (ls.size() >= 4) {
      CHECK(d1sub1sub1sub1.matches(ls[3]));
    }
    if (ls.size() >= 5) {
      CHECK(testpaths[7].matches(ls[4]));
    }
  }

  SECTION("Recursion") {
    auto d1 = TestPath(vfs_test, "d1");
    auto d1sub1 = TestPath(vfs_test, "d1/d1sub1");
    auto d1sub1sub1 = TestPath(vfs_test, "d1/d1sub1/d1sub1sub1");
    auto d1sub1sub1sub1 =
        TestPath(vfs_test, "d1/d1sub1/d1sub1sub1/d1sub1sub1sub1");
    d1.mkdir();
    d1sub1.mkdir();
    d1sub1sub1.mkdir();
    d1sub1sub1sub1.mkdir();
    for (unsigned i = 0; i < testpaths.size(); i++) {
      testpaths[i].touch();
    }

    const auto ls = sort_by_name(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_all_files,
        tiledb::sm::accept_all_dirs));
    CHECK(ls.size() == testpaths.size() + 4);

    if (ls.size() >= 1) {
      CHECK(testpaths[0].matches(ls[0]));
    }
    if (ls.size() >= 2) {
      CHECK(testpaths[1].matches(ls[1]));
    }
    if (ls.size() >= 3) {
      CHECK(d1.matches(ls[2]));
    }
    if (ls.size() >= 4) {
      CHECK(testpaths[5].matches(ls[3]));
    }
    if (ls.size() >= 5) {
      CHECK(d1sub1.matches(ls[4]));
    }
    if (ls.size() >= 6) {
      CHECK(d1sub1sub1.matches(ls[5]));
    }
    if (ls.size() >= 7) {
      CHECK(d1sub1sub1sub1.matches(ls[6]));
    }
    if (ls.size() >= 8) {
      CHECK(testpaths[7].matches(ls[7]));
    }
    if (ls.size() >= 9) {
      CHECK(testpaths[8].matches(ls[8]));
    }
    if (ls.size() >= 10) {
      CHECK(testpaths[6].matches(ls[9]));
    }
    if (ls.size() >= 11) {
      CHECK(testpaths[4].matches(ls[10]));
    }
    if (ls.size() >= 12) {
      CHECK(testpaths[2].matches(ls[11]));
    }
    if (ls.size() >= 13) {
      CHECK(testpaths[3].matches(ls[12]));
    }
  }

  /* all tests must close all the files that they opened, in normal use of the
   * API */
  REQUIRE(vfs_test.check_open_files());
}

TEST_CASE("VFS: ls_recursive file filter", "[vfs][ls_recursive]") {
  std::string prefix = GENERATE("file://");
  prefix += std::filesystem::current_path().string() + "/ls_recursive_test/";

  VFSTest vfs_test({0}, prefix);
  const auto mkst = vfs_test.mkdir();
  REQUIRE(mkst.ok());

  std::vector<TestPath> testpaths = {
      TestPath(vfs_test, "year=2021/month=8/day=27/log1.txt", 30),
      TestPath(vfs_test, "year=2021/month=8/day=27/log2.txt", 31),
      TestPath(vfs_test, "year=2021/month=8/day=28/log1.txt", 40),
      TestPath(vfs_test, "year=2021/month=8/day=28/log2.txt", 41),
      TestPath(vfs_test, "year=2021/month=9/day=27/log1.txt", 50),
      TestPath(vfs_test, "year=2021/month=9/day=27/log2.txt", 51),
      TestPath(vfs_test, "year=2021/month=9/day=28/log1.txt", 60),
      TestPath(vfs_test, "year=2021/month=9/day=28/log2.txt", 61),
      TestPath(vfs_test, "year=2022/month=8/day=27/log1.txt", 70),
      TestPath(vfs_test, "year=2022/month=8/day=27/log2.txt", 71),
      TestPath(vfs_test, "year=2022/month=8/day=28/log1.txt", 80),
      TestPath(vfs_test, "year=2022/month=8/day=28/log2.txt", 81),
      TestPath(vfs_test, "year=2022/month=9/day=27/log1.txt", 90),
      TestPath(vfs_test, "year=2022/month=9/day=27/log2.txt", 91),
      TestPath(vfs_test, "year=2022/month=9/day=28/log1.txt", 20),
      TestPath(vfs_test, "year=2022/month=9/day=28/log2.txt", 21),
  };

  SECTION("File predicate returning false is discarded from results") {
    for (auto& testpath : testpaths) {
      testpath.touch(true);
    }

    /*
     * This also shows us that the file filter is only called on the leaves,
     * since "log1.txt" only appears in the basename component of the test
     * paths.
     */
    auto log_is_1 = [](const std::string_view& path, uint64_t) -> bool {
      return (path.find("log1.txt") != std::string::npos);
    };

    auto ls = vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_, log_is_1, tiledb::sm::accept_all_dirs);

    /* directories appear in the result set, we aren't interested in those,
     * and the callback doesn't (yet?) have a way to descend into a directory
     * without also including it in the result set */
    std::erase_if(ls, [](const auto& obj) { return obj.second == 0; });

    CHECK(ls.size() == testpaths.size() / 2);

    ls = sort_by_name(ls); /* ensure order matches the testpaths order */

    for (uint64_t i = 0; i < testpaths.size(); i += 2) {
      CHECK(testpaths[i].matches(ls[i / 2]));
    }
  }
}

TEST_CASE("VFS: ls_recursive directory filter", "[vfs][ls_recursive]") {
  std::string prefix = GENERATE("file://");
  prefix += std::filesystem::current_path().string() + "/ls_recursive_test/";

  VFSTest vfs_test({0}, prefix);
  const auto mkst = vfs_test.mkdir();
  REQUIRE(mkst.ok());

  std::vector<TestPath> testpaths = {
      TestPath(vfs_test, "year=2021/month=8/day=27/log1.txt", 30),
      TestPath(vfs_test, "year=2021/month=8/day=27/log2.txt", 31),
      TestPath(vfs_test, "year=2021/month=8/day=28/log1.txt", 40),
      TestPath(vfs_test, "year=2021/month=8/day=28/log2.txt", 41),
      TestPath(vfs_test, "year=2021/month=9/day=28/log1.txt", 50),
      TestPath(vfs_test, "year=2021/month=9/day=28/log2.txt", 51),
      TestPath(vfs_test, "year=2021/month=9/day=29/log1.txt", 60),
      TestPath(vfs_test, "year=2021/month=9/day=29/log2.txt", 61),
      TestPath(vfs_test, "year=2022/month=8/day=27/log1.txt", 70),
      TestPath(vfs_test, "year=2022/month=8/day=27/log2.txt", 71),
      TestPath(vfs_test, "year=2022/month=8/day=28/log1.txt", 80),
      TestPath(vfs_test, "year=2022/month=8/day=28/log2.txt", 81),
      TestPath(vfs_test, "year=2022/month=9/day=28/log1.txt", 90),
      TestPath(vfs_test, "year=2022/month=9/day=28/log2.txt", 91),
      TestPath(vfs_test, "year=2022/month=9/day=29/log1.txt", 20),
      TestPath(vfs_test, "year=2022/month=9/day=29/log2.txt", 21),
  };

  /* create all files and dirs */
  for (auto& testpath : testpaths) {
    testpath.touch(true);
  }

  SECTION("Directory predicate returning true is filtered from results") {
    auto month_is_august = [](std::string_view dirname) -> bool {
      if (dirname.find("month") == std::string::npos) {
        /* haven't descended far enough yet */
        return true;
      } else if (dirname.find("month=8") == std::string::npos) {
        /* not august */
        return false;
      } else {
        return true;
      }
    };

    auto ls = vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_only_regular_files,
        month_is_august);

    /* directories appear in the result set, we aren't interested in those,
     * and the callback doesn't (yet?) have a way to descend into a directory
     * without also including it in the result set */
    std::erase_if(ls, [](const auto& obj) { return obj.second == 0; });

    CHECK(ls.size() == testpaths.size() / 2);

    ls = sort_by_name(ls); /* ensure order matches the testpaths order */

    CHECK(ls.size() == 8);
    if (ls.size() >= 1) {
      testpaths[0].matches(ls[0]);
    }
    if (ls.size() >= 2) {
      testpaths[1].matches(ls[1]);
    }
    if (ls.size() >= 3) {
      testpaths[2].matches(ls[2]);
    }
    if (ls.size() >= 4) {
      testpaths[3].matches(ls[3]);
    }
    if (ls.size() >= 5) {
      testpaths[8].matches(ls[4]);
    }
    if (ls.size() >= 6) {
      testpaths[9].matches(ls[5]);
    }
    if (ls.size() >= 7) {
      testpaths[10].matches(ls[6]);
    }
    if (ls.size() >= 8) {
      testpaths[11].matches(ls[7]);
    }
  }

  /* note: this should be true for POSIX but is not for S3 without hierarchical
   * list API */
  SECTION(
      "Directory predicate returning true does not descend into directory") {
    /*
     * In the test data we only find "day=29" beneath "month=9",
     * so the `ls` should throw with this directory filter if and only if
     * we descend into directories with "month=9".
     */
    std::string monthstr = "month=9";
    auto throw_if_day_is_29 = [&monthstr](std::string_view dirname) -> bool {
      if (dirname.find("month") == std::string::npos) {
        /* haven't descended far enough yet */
        return true;
      } else if (dirname.find(monthstr) == std::string::npos) {
        /* not august */
        return false;
      } else if (dirname.find("day=29") == std::string::npos) {
        /* not the 29th */
        return true;
      } else {
        /* it is the 29th, throw */
        throw std::logic_error("Throwing FileFilter: day=29");
      }
    };

    CHECK_THROWS_AS(
        vfs_test.vfs_.ls_recursive(
            vfs_test.temp_dir_,
            tiledb::sm::accept_only_regular_files,
            throw_if_day_is_29),
        std::logic_error);
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive(
            vfs_test.temp_dir_,
            tiledb::sm::accept_only_regular_files,
            throw_if_day_is_29),
        Catch::Matchers::ContainsSubstring("Throwing FileFilter: day=29"));

    monthstr = "month=8";

    /* now the result should be the same as the first section */
    auto ls = vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_,
        tiledb::sm::accept_only_regular_files,
        throw_if_day_is_29);

    /* directories appear in the result set, we aren't interested in those,
     * and the callback doesn't (yet?) have a way to descend into a directory
     * without also including it in the result set */
    std::erase_if(ls, [](const auto& obj) { return obj.second == 0; });

    CHECK(ls.size() == testpaths.size() / 2);

    ls = sort_by_name(ls); /* ensure order matches the testpaths order */

    CHECK(ls.size() == 8);
    if (ls.size() >= 1) {
      testpaths[0].matches(ls[0]);
    }
    if (ls.size() >= 2) {
      testpaths[1].matches(ls[1]);
    }
    if (ls.size() >= 3) {
      testpaths[2].matches(ls[2]);
    }
    if (ls.size() >= 4) {
      testpaths[3].matches(ls[3]);
    }
    if (ls.size() >= 5) {
      testpaths[8].matches(ls[4]);
    }
    if (ls.size() >= 6) {
      testpaths[9].matches(ls[5]);
    }
    if (ls.size() >= 7) {
      testpaths[10].matches(ls[6]);
    }
    if (ls.size() >= 8) {
      testpaths[11].matches(ls[7]);
    }
  }

  /*
   * Note that since we throw in the previous section, this check
   * demonstrates that all directories are closed whether or not we return
   * from ls_recursive normally
   */
  REQUIRE(vfs_test.check_open_files());
}

TEST_CASE("VFS: Throwing FileFilter ls_recursive", "[vfs][ls_recursive]") {
  std::string prefix = GENERATE("file://");
  prefix += std::filesystem::current_path().string() + "/ls_filtered_test";

  VFSTest vfs_test({0}, prefix);
  const auto mkst = vfs_test.mkdir();
  REQUIRE(mkst.ok());

  auto always_throw_filter = [](const std::string_view&, uint64_t) -> bool {
    throw std::logic_error("Throwing FileFilter");
  };
  SECTION("Throwing FileFilter with 0 objects should not throw") {
    CHECK_NOTHROW(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_, always_throw_filter, tiledb::sm::accept_all_dirs));
  }
  SECTION(
      "Throwing FileFilter will not throw if ls_recursive only visits "
      "directories") {
  }
  SECTION("Throwing FileFilter with N objects should throw") {
    vfs_test.vfs_.touch(vfs_test.temp_dir_.join_path("file")).ok();
    CHECK_THROWS_AS(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, always_throw_filter),
        std::logic_error);
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, always_throw_filter),
        Catch::Matchers::ContainsSubstring("Throwing FileFilter"));
  }

  /* all tests must close all the files that they opened, regardless of
   * exception behavior */
  REQUIRE(vfs_test.check_open_files());
}

TEST_CASE(
    "VFS: ls_recursive throws for unsupported filesystems",
    "[vfs][ls_recursive]") {
  std::string prefix = GENERATE("mem://");
  prefix += std::filesystem::current_path().string() + "/ls_filtered_test";

  VFSTest vfs_test({1}, prefix);
  std::string backend = vfs_test.temp_dir_.backend_name();
  DYNAMIC_SECTION(backend << " unsupported backend should throw") {
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive(
            vfs_test.temp_dir_, tiledb::sm::accept_all_files),
        Catch::Matchers::ContainsSubstring("storage backend is not supported"));
  }

  REQUIRE(vfs_test.check_open_files());
}
