// Copyright 2020 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

#include "google/cloud/bigtable/internal/common_client.h"
#include "google/cloud/testing_util/assert_ok.h"
#include <gmock/gmock.h>

namespace google {
namespace cloud {
namespace bigtable {
inline namespace BIGTABLE_CLIENT_NS {
namespace internal {

using TimerFuture = future<StatusOr<std::chrono::system_clock::time_point>>;

class OutstandingTimersTest : public ::testing::Test {
 public:
  OutstandingTimersTest() : thread_([this] { cq_.Run(); }) {}
  ~OutstandingTimersTest() override {
    cq_.Shutdown();
    thread_.join();
  }

 protected:
  CompletionQueue cq_;
  std::thread thread_;
};

TEST_F(OutstandingTimersTest, Trivial) {
  // With MSVC 2019 (19.28.29334) the value from make_shared<>
  // must be captured.
  auto unused = 
  std::make_shared<OutstandingTimers>(
      std::make_shared<CompletionQueue>(cq_));
}

TEST_F(OutstandingTimersTest, TimerFinishes) {
  auto registry = std::make_shared<OutstandingTimers>(
      std::make_shared<CompletionQueue>(cq_));
  promise<void> continuation_promise;
  auto t = cq_.MakeRelativeTimer(std::chrono::milliseconds(2))
               .then([&](TimerFuture fut) {
                 EXPECT_STATUS_OK(fut.get());
                 continuation_promise.set_value();
               });
  registry->RegisterTimer(std::move(t));
  continuation_promise.get_future().get();
  // This should be a noop.
  registry->CancelAll();
  // Calling it twice shouldn't hurt.
  registry->CancelAll();
}

TEST_F(OutstandingTimersTest, TimerIsCancelled) {
  auto registry = std::make_shared<OutstandingTimers>(
      std::make_shared<CompletionQueue>(cq_));
  promise<void> continuation_promise;
  auto t =
      cq_.MakeRelativeTimer(std::chrono::hours(10)).then([&](TimerFuture fut) {
        auto res = fut.get();
        EXPECT_FALSE(res);
        EXPECT_EQ(StatusCode::kCancelled, res.status().code());
        continuation_promise.set_value();
      });
  registry->RegisterTimer(std::move(t));
  registry->CancelAll();
  continuation_promise.get_future().get();
}

TEST_F(OutstandingTimersTest, TimerOutlivesRegistry) {
  auto registry = std::make_shared<OutstandingTimers>(
      std::make_shared<CompletionQueue>(cq_));
  promise<void> continuation_promise;
  auto t = cq_.MakeRelativeTimer(std::chrono::milliseconds(10))
               .then([&](TimerFuture fut) {
                 auto res = fut.get();
                 EXPECT_STATUS_OK(res);
                 continuation_promise.set_value();
               });
  registry->RegisterTimer(std::move(t));
  registry.reset();
  continuation_promise.get_future().get();
}

TEST_F(OutstandingTimersTest, TimerRegisteredAfterCancelAllGetCancelled) {
  auto registry = std::make_shared<OutstandingTimers>(
      std::make_shared<CompletionQueue>(cq_));
  promise<void> continuation_promise;
  auto t =
      cq_.MakeRelativeTimer(std::chrono::hours(10)).then([&](TimerFuture fut) {
        auto res = fut.get();
        EXPECT_FALSE(res);
        EXPECT_EQ(StatusCode::kCancelled, res.status().code());
        continuation_promise.set_value();
      });
  registry->CancelAll();
  registry->RegisterTimer(std::move(t));
  continuation_promise.get_future().get();
}

}  // namespace internal
}  // namespace BIGTABLE_CLIENT_NS
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
