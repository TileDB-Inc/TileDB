/*
 * @file tiledb/sm/storage_manager/test/unit_job.cpp
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
 * @section DESCRIPTION
 *
 * This file provides unit tests for `template <class T> class Registry`.
 *
 * @section Maturity
 *
 * This test file combines tests for both the job system as well as the
 * production mixin  for that job system. The job system and the mixin are in
 * separate files and in different namespaces. This structure anticipates moving
 * the generic job system out of this directory, but these tests have not yet
 * been separated.
 */

#include <catch2/catch_test_macros.hpp>

#include "../context.h"
#include "../job.h"
#include "tiledb/common/logger.h"

/*
 * This `using` declarations pulls in production version of JobParent, JobRoot,
 * and all the others.
 */
using namespace tiledb::sm;
namespace tcj = tiledb::common::job;

//-------------------------------------------------------
//
//-------------------------------------------------------
/*
 * This block is to test the constructors for all the intermediates of a job
 * system. Initialization of virtual base classes is error-prone, so we are
 * explicitly exercising all of them separately.
 */
namespace test {
using namespace tiledb::common::job;

struct WhiteboxActivityBase : public ActivityBase<NullMixin> {
  WhiteboxActivityBase()
      : ActivityBase<NullMixin>() {
  }
};
using TestActivityBase = WhiteboxActivityBase;
using TestActivity = NullMixin::ActivityMixin;
using TestChildBase = ChildBase<NullMixin>;
using TestChild = NullMixin::ChildMixin;
using TestNonchildBase = NonchildBase<NullMixin>;
using TestNonchild = NullMixin::NonchildMixin;

template <class>
struct WhiteboxSupervisionBase : public SupervisionBase<NullMixin> {
  WhiteboxSupervisionBase(TestActivity& activity)
      : SupervisionBase<NullMixin>(activity) {
  }
};
using TestSupervisionBase = SupervisionBase<NullMixin>;
using TestSupervision = NullMixin::SupervisionMixin;
using TestParentBase = ParentBase<NullMixin>;
using TestParent = NullMixin::ParentMixin;
using TestNonparentBase = ParentBase<NullMixin>;
using TestNonparent = NullMixin::NonparentMixin;

TEST_CASE("Default `Supervision` Hierarchy - construct") {
  TestActivity act{};
  SECTION("Supervision Base") {
    TestSupervisionBase svb{act};
  }
  SECTION("Supervision Mixin") {
    TestSupervision svm{act};
  }
  SECTION("Parent Base") {
    TestParentBase pb{act};
  }
  SECTION("Parent Mixin") {
    TestParent p{act};
  }
  SECTION("Nonparent Base") {
    TestNonparentBase npb{act};
  }
  SECTION("Nonparent Mixin") {
    TestNonparent np{act};
  }
}

TEST_CASE("Default `Activity` Hierarchy - construct") {
  SECTION("Activity Base") {
    TestActivityBase ab{};
  }
  SECTION("Activity Mixin") {
    TestActivity am{};
  }
  SECTION("Child") {
    TestActivity p_act{};
    TestParent p{p_act};
    SECTION("its Base") {
      auto cb{TestChildBase{p}};
    }
    SECTION("itself") {
      TestChild c{p};
    }
  }
  SECTION("Nonchild Base") {
    TestNonchildBase ncb{};
  }
  SECTION("Nonchild") {
    TestNonchild nc{};
  }
}
}  // namespace test

/*
 * Construction tests for the default `JobSystem`
 */
namespace test {

/*
 * This namespace uses the default job system with it's default template
 * argument. Internally this is the NullMixin, but we don't reference it
 * explicitly in this statement in order to ensure the default works.
 */
using JS = JobSystem<>;

TEST_CASE("common::JobSystem - construct") {
  SECTION("Rooted classes") {
    JS::JobRoot root{};
    SECTION("Root itself") {
      // Just the initialization
    }
    SECTION("Root + Branch") {
      JS::JobBranch y{root};
    }
    SECTION("Root + Leaf") {
      JS::JobLeaf y{root};
    }
    SECTION("Root + Branch + Leaf") {
      JS::JobBranch y{root};
      JS::JobLeaf z{y};
    }
  }
  SECTION("Isolate class") {
    JS::JobIsolate x{};
  }
}
}  // namespace test

/**
 * Direct access to the Child class inside the production job system. We use the
 * production job system in order to test with `class Context`.
 */
using DirectTestChild = tiledb::sm::job::JobResourceMixin::ChildMixin;

struct TestJobChild : public DirectTestChild {
  explicit TestJobChild(Context& context)
      : DirectTestChild(context) {
  }

  static std::shared_ptr<TestJobChild> factory(Context& context) {
    auto job{std::make_shared<TestJobChild>(context)};
    job->register_shared_ptr(job);
    return job;
  }
};

TEST_CASE("job::Child - construct 0") {
  Config config{};
  Context context{config};
  /*
   * This is never how we'd construct a job that needs to be fully registered,
   * since it's not managed by a `shared_ptr`.
   */
  auto job{TestJobChild(context)};
}

TEST_CASE("job::Child - construct 1") {
  Config config{};
  Context context{config};
  auto job{TestJobChild::factory(context)};
}

struct TestJobChildFactory {
  Config config_;
  Context context_;

  TestJobChildFactory()
      : config_()
      , context_(config_) {
  }

  std::shared_ptr<TestJobChild> operator()() {
    return TestJobChild::factory(context_);
  }
};

TEST_CASE("job::Child - construct 2") {
  TestJobChildFactory jf{};
  auto job{jf()};
}

TEST_CASE("job::Child - construct and size") {
  TestJobChildFactory jf{};
  CHECK(jf.context_.number_of_jobs() == 0);
  auto job{jf()};
  CHECK(jf.context_.number_of_jobs() == 1);
  job.reset();
  CHECK(jf.context_.number_of_jobs() == 0);
}

class TestJobRoot : public JobRoot {
  Config config_{};
  ContextResources resources_{
      config_, std::make_shared<Logger>("log"), 1, 1, ""};
  StorageManager sm_{resources_, config_};

 public:
  TestJobRoot()
      : JobRoot(sm_) {
  }
  ContextResources& resources() override {
    return resources_;
  }
};
static_assert(TestJobRoot::is_parent);
static_assert(!TestJobRoot::is_child);

TEST_CASE("TestJobRoot - construct") {
  TestJobRoot root{};
}

class TestJobBranch : public JobBranch {
 public:
  TestJobBranch(JobParent& parent)
      : JobBranch(parent) {
  }
  ContextResources& resources() override {
    return parent().resources();
  }
};
static_assert(TestJobBranch::is_parent);
static_assert(TestJobBranch::is_child);

TEST_CASE("TestJobBranch - construct from root") {
  TestJobRoot root{};
  TestJobBranch first{root};
}

TEST_CASE("TestJobBranch - construct from branch") {
  TestJobRoot root{};
  TestJobBranch first{root};
  // Need a cast because otherwise it means the copy constructor
  TestJobBranch second{static_cast<JobParent&>(first)};
}

class TestJobLeaf : public JobLeaf {
 public:
  TestJobLeaf(JobParent& parent)
      : JobLeaf(parent) {
  }
};
static_assert(!TestJobLeaf::is_parent);
static_assert(TestJobLeaf::is_child);

TEST_CASE("TestJobLeaf - construct from root") {
  TestJobRoot root{};
  TestJobLeaf first{root};
}

TEST_CASE("TestJobLeaf - construct from branch") {
  TestJobRoot root{};
  TestJobBranch branch{root};
  TestJobLeaf leaf{branch};
}

class TestJobIsolate : public JobIsolate {
  Config config_{};
  ContextResources resources_{
      config_, std::make_shared<Logger>("log"), 1, 1, ""};
  StorageManager sm_{resources_, config_};

 public:
  TestJobIsolate()
      : JobIsolate(sm_) {
  }
};
static_assert(!TestJobIsolate::is_parent);
static_assert(!TestJobIsolate::is_child);

TEST_CASE("TestJobIsolate - construct") {
  TestJobIsolate x{};
}

//-------------------------------------------------------
// Mixin
//-------------------------------------------------------

class NotResources {};

struct TestMixin : public tiledb::common::job::NullMixin {
  using Self = TestMixin;
  struct ParentMixin;

  /**
   * Mixin class for Activity
   */
  struct ActivityMixin : public tcj::ActivityBase<Self> {
    explicit ActivityMixin()
        : ActivityBase<Self>(){};
  };

  /**
   * Mixin class for Child
   */
  struct ChildMixin : public tcj::ChildBase<Self> {
    ChildMixin() = delete;
    explicit ChildMixin(ParentMixin& parent)
        : ChildBase<Self>(parent){};
  };

  /**
   * Mixin class for Nonchild
   */
  struct NonchildMixin : public tcj::NonchildBase<Self> {
    explicit NonchildMixin()
        : NonchildBase<Self>() {
    }
  };

  /**
   * Mixin class for Supervision
   */
  struct SupervisionMixin : public tcj::SupervisionBase<Self> {
    SupervisionMixin() = delete;
    explicit SupervisionMixin(ActivityMixin& activity)
        : SupervisionBase<Self>(activity){};
  };

  /**
   * Mixin class for Parent
   */
  struct ParentMixin : public tcj::ParentBase<Self> {
    ParentMixin() = delete;
    explicit ParentMixin(ActivityMixin& activity)
        : ParentBase<Self>(activity) {
    }
    [[nodiscard]] virtual NotResources& resources2() = 0;
  };

  /**
   * Mixin class for Nonparent
   */
  struct NonparentMixin : public tcj::NonparentBase<Self> {
    NonparentMixin() = delete;
    explicit NonparentMixin(ActivityMixin& activity)
        : NonparentBase<Self>(activity) {
    }
  };

};
using mixin_job_system = tcj::JobSystem<TestMixin>;

class MixinJobRoot : public mixin_job_system::JobRoot {
 public:
  MixinJobRoot()
      : JobRoot() {
  }
  NotResources& resources2() override {
    static NotResources nothing_;
    return nothing_;
  }
};

class MixinJobBranch : public mixin_job_system::JobBranch {
 public:
  explicit MixinJobBranch(mixin_job_system::JobParent& p)
      : JobBranch(p) {
  }
  NotResources& resources2() override {
    static NotResources nothing_;
    return nothing_;
  }
};

class MixinJobIsolate : public mixin_job_system::JobIsolate {
 public:
  MixinJobIsolate()
      : JobIsolate() {
  }
};

TEST_CASE("MixinWithSupervision - construct") {
  SECTION("Branch") {
    MixinJobRoot x{};
    MixinJobBranch y{x};
  }
  SECTION("Isolate") {
    MixinJobIsolate x{};
  }
}
