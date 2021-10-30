/**
 * @file unit-TaskGraphExecutor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests the `TaskGraphExecutor` class.
 */

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/task_graph/task_graph_executor.h"
#include "tiledb/common/thread_pool.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::common;

TEST_CASE(
    "TaskGraphExecutor: Test basic functionality",
    "[TaskGraphExecutor][basic]") {
  // Create thread pool
  ThreadPool tp;
  CHECK(tp.init(std::thread::hardware_concurrency()).ok());

  // Create task graph
  auto uninit_task_graph = tdb_make_shared(TaskGraph);

  // Check initializations
  TaskGraphExecutor tge;
  CHECK(!tge.execute().ok());
  tge.set_thread_pool(&tp);
  CHECK(!tge.execute().ok());
  tge.set_task_graph(uninit_task_graph);
  CHECK(tge.execute().ok());
  CHECK(tge.wait().ok());

  // Simple graph
  std::vector<int32_t> vec(3, 0);
  auto task_graph = tdb_make_shared(TaskGraph);
  auto task0 = task_graph->emplace(
      [&vec]() {
        vec[0] = 1;
        return Status::Ok();
      },
      "0");
  auto task1 = task_graph->emplace(
      [&vec]() {
        vec[1] = 1;
        return Status::Ok();
      },
      "1");
  auto task2 = task_graph->emplace(
      [&vec]() {
        vec[2] = 1;
        return Status::Ok();
      },
      "2");
  task_graph->succeeds(task2, task0, task1);
  tge.set_task_graph(task_graph);
  CHECK(tge.execute().ok());
  CHECK(tge.wait().ok());
  CHECK(vec[0] == 1);
  CHECK(vec[1] == 1);
  CHECK(vec[2] == 1);
}

TEST_CASE(
    "TaskGraphExecutor: Test dynamic creation of tasks",
    "[TaskGraphExecutor][dynamic]") {
  std::vector<bool> vec(5, false);

  // Prepare the function that will generate a task graph
  auto lambda = [&vec]() {
    auto task_graph = tdb_make_shared(TaskGraph);
    auto task3 = task_graph->emplace(
        [&vec]() {
          vec[3] = true;
          return Status::Ok();
        },
        "gen_1");
    auto task4 = task_graph->emplace(
        [&vec]() {
          vec[4] = true;
          return Status::Ok();
        },
        "gen_2");
    task_graph->succeeds(task4, task3);
    vec[1] = true;

    return std::pair<Status, tdb_shared_ptr<TaskGraph>>(
        Status::Ok(), task_graph);
  };

  auto task_graph = tdb_make_shared(TaskGraph);
  auto task0 = task_graph->emplace(
      [&vec]() {
        vec[0] = true;
        return Status::Ok();
      },
      "0");
  auto task1 = task_graph->emplace(lambda, "1");
  auto task2 = task_graph->emplace(
      [&vec]() {
        vec[2] = true;
        return Status::Ok();
      },
      "2");
  task_graph->succeeds(task1, task0);
  task_graph->succeeds(task2, task0);

  // Execute the task graph
  ThreadPool tp;
  CHECK(tp.init(std::thread::hardware_concurrency()).ok());
  TaskGraphExecutor tge(&tp, task_graph);
  tge.execute();
  tge.wait();

  // Check correctness
  std::vector<bool> vec_c(5, true);
  CHECK(vec == vec_c);
}
