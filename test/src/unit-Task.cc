/**
 * @file unit-Task.cc
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
 * Tests the `Task` class.
 */

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/task_graph/task.h"
#include "tiledb/common/task_graph/task_graph.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::common;

TEST_CASE("Task: Test basic functionality", "[Task][basic]") {
  // Uninitialized task
  Task task;
  CHECK(task.id() == std::numeric_limits<uint64_t>::max());
  CHECK(task.name() == std::string());
  CHECK(!task.execute().ok());

  // Initialized task
  std::vector<int32_t> v;
  auto lambda1 = [&]() {
    v.push_back(1);
    return Status::Ok();
  };
  CHECK(v.empty());
  Task task1(1, lambda1, "1");
  CHECK(v.empty());
  CHECK(task1.id() == 1);
  CHECK(task1.name() == "1");
  CHECK(task1.execute().ok());
  CHECK(v.size() == 1);
  CHECK(v[0] == 1);

  // Task with function arguments
  v.clear();
  auto lambda2_tmp = [&](int i) {
    v.push_back(i);
    return Status::Ok();
  };
  auto lambda2 = std::bind(lambda2_tmp, 2);
  CHECK(v.empty());
  Task task2(2, lambda2, "2");
  CHECK(v.empty());
  CHECK(task2.id() == 2);
  CHECK(task2.name() == "2");
  CHECK(task2.execute().ok());
  CHECK(v.size() == 1);
  CHECK(v[0] == 2);

  // Tasks with successors and predecessors
  auto task3 = tdb_make_shared(Task, 3, []() { return Status::Ok(); }, "3");
  auto task4 = tdb_make_shared(Task, 4, []() { return Status::Ok(); }, "4");
  auto task5 = tdb_make_shared(Task, 5, []() { return Status::Ok(); }, "5");
  auto task6 = tdb_make_shared(Task, 6, []() { return Status::Ok(); }, "6");

  CHECK(task3->add_successor(task4).ok());
  CHECK(task3->successors_num() == 1);
  CHECK(!task3->add_successor(task4).ok());
  CHECK(task3->add_successor(task5).ok());
  CHECK(task3->successors_num() == 2);
  CHECK(task3->predecessors_num() == 0);
  CHECK(task3->add_predecessor(task6).ok());
  CHECK(task3->predecessors_num() == 1);

  auto successors3 = task3->successors();
  auto predecessors3 = task3->predecessors();
  REQUIRE(successors3.size() == 2);
  CHECK(successors3[0]->id() == 4);
  CHECK(successors3[1]->id() == 5);
  REQUIRE(predecessors3.size() == 1);
  CHECK(predecessors3[0]->id() == 6);

  // Test generated task graph (it should be null here)
  CHECK(task3->generated_task_graph() == nullptr);
  CHECK(task3->generated_by() == std::numeric_limits<uint64_t>::max());

  // Clean up
  task3.reset(nullptr);
  task4.reset(nullptr);
  task5.reset(nullptr);
  task6.reset(nullptr);
}

TEST_CASE("Task: Test generated task graph", "[Task][generated_task_graph]") {
  auto lambda = []() {
    auto task_graph = tdb_make_shared(TaskGraph);
    auto task1 = task_graph->emplace([]() { return Status::Ok(); }, "1");
    auto task2 = task_graph->emplace([]() { return Status::Ok(); }, "2");
    task1->set_generated_by(0);
    task2->set_generated_by(0);
    task_graph->succeeds(task2, task1);

    return std::pair<Status, tdb_shared_ptr<TaskGraph>>(
        Status::Ok(), task_graph);
  };
  Task task0(0, lambda, "0");
  CHECK(task0.generated_task_graph() == nullptr);
  CHECK(task0.execute().ok());
  CHECK(task0.generated_task_graph() != nullptr);
  auto tg = task0.generated_task_graph();
  auto tasks_map = tg->tasks_map();
  CHECK(tasks_map.size() == 2);
  CHECK(tasks_map[0]->id() == 0);
  CHECK(tasks_map[0]->generated_by() == 0);
  CHECK(tasks_map[1]->id() == 1);
  CHECK(tasks_map[1]->generated_by() == 0);
}