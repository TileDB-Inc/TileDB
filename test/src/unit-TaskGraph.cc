/**
 * @file unit-TaskGraph.cc
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
 * Tests the `TaskGraph` class.
 */

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/task_graph/task_graph.h"

#include <catch.hpp>
#include <iostream>

using namespace tiledb::common;

// Helper that returns true if `tasks` contains exactly tasks with ids
// from 0 to `max_id` (and no duplicates).
bool check_tasks(const std::set<tdb_shared_ptr<Task>>& tasks, uint64_t max_id) {
  auto task_num = max_id + 1;
  if (task_num != tasks.size())
    return false;

  std::vector<bool> ids(task_num, false);
  for (const auto& task : tasks) {
    ids[task->id()] = true;
  }

  for (auto id : ids) {
    if (!id)
      return false;
  }

  return true;
}

// Helper that returns true if `tasks` contains exactly tasks with ids
// from 0 to `max_id` (and no duplicates).
bool check_tasks_map(
    const std::unordered_map<uint64_t, tdb_shared_ptr<Task>>& tasks_map,
    uint64_t max_id) {
  auto task_num = max_id + 1;
  if (task_num != tasks_map.size())
    return false;

  std::vector<bool> ids(task_num, false);
  for (const auto& it : tasks_map) {
    ids[it.second->id()] = true;
  }

  for (auto id : ids) {
    if (!id)
      return false;
  }

  return true;
}

TEST_CASE("TaskGraph: Test basic functionality", "[TaskGraph][basic]") {
  // Test succeeds
  auto task_graph = tdb_make_shared(TaskGraph);
  auto task0 = task_graph->emplace([]() { return Status::Ok(); }, "0");
  auto task1 = task_graph->emplace([]() { return Status::Ok(); }, "1");
  auto task2 = task_graph->emplace([]() { return Status::Ok(); }, "2");
  auto task3 = task_graph->emplace([]() { return Status::Ok(); }, "3");
  auto task4 = task_graph->emplace([]() { return Status::Ok(); }, "4");
  auto tasks = task_graph->tasks();
  check_tasks(tasks, 4);
  auto tasks_map = task_graph->tasks_map();
  check_tasks_map(tasks_map, 4);
  CHECK(task_graph->succeeds(task2, task0, task1).ok());
  CHECK(task_graph->succeeds(task3, task2).ok());
  CHECK(task_graph->succeeds(task4, task2).ok());
  auto preds2 = task2->predecessors();
  REQUIRE(preds2.size() == 2);
  CHECK(preds2[0]->id() == 0);
  CHECK(preds2[1]->id() == 1);
  auto preds3 = task3->predecessors();
  REQUIRE(preds3.size() == 1);
  CHECK(preds3[0]->id() == 2);
  auto preds4 = task4->predecessors();
  REQUIRE(preds4.size() == 1);
  CHECK(preds4[0]->id() == 2);
  CHECK(task0->successors_num() == 1);
  CHECK(task2->predecessors_num() == 2);

  // Test precedes
  task_graph = tdb_make_shared(TaskGraph);
  task0 = task_graph->emplace([]() { return Status::Ok(); }, "0");
  task1 = task_graph->emplace([]() { return Status::Ok(); }, "1");
  task2 = task_graph->emplace([]() { return Status::Ok(); }, "2");
  task3 = task_graph->emplace([]() { return Status::Ok(); }, "3");
  task4 = task_graph->emplace([]() { return Status::Ok(); }, "4");
  tasks = task_graph->tasks();
  check_tasks(tasks, 4);
  tasks_map = task_graph->tasks_map();
  check_tasks_map(tasks_map, 4);
  CHECK(task_graph->precedes(task2, task3, task4).ok());
  CHECK(task_graph->precedes(task0, task2).ok());
  CHECK(task_graph->precedes(task1, task2).ok());
  auto succs2 = task2->successors();
  REQUIRE(succs2.size() == 2);
  CHECK(succs2[0]->id() == 3);
  CHECK(succs2[1]->id() == 4);
  auto succs0 = task0->successors();
  REQUIRE(succs0.size() == 1);
  CHECK(succs0[0]->id() == 2);
  auto succs1 = task1->successors();
  REQUIRE(succs1.size() == 1);
  CHECK(succs1[0]->id() == 2);
  CHECK(task0->successors_num() == 1);
  CHECK(task2->predecessors_num() == 2);

  // Errors
  CHECK(!task_graph->succeeds(task4, task2).ok());

  // Roots
  auto roots = task_graph->roots();
  CHECK(roots.size() == 2);
  CHECK(roots[0]->id() == 0);
  CHECK(roots[1]->id() == 1);

  // to_dot
  auto dot = task_graph->to_dot();
  // To test manually, uncomment the line below:
  // std::cout << dot << "\n";
  // Then copy this into a source file, say `graph.txt`.
  // Then run `dot -Tps graph.txt -o graph.ps`. The graph will
  // be visualized in file `graph.ps`.

  // Cyclic
  CHECK(!task_graph->is_cyclic());
  task_graph->precedes(task4, task0);
  CHECK(task_graph->is_cyclic());

  // Test invalid task in `succeeds` and `precedes`
  auto task_inv =
      tdb_make_shared(Task, 100, []() { return Status::Ok(); }, "invalid");
  CHECK(!task_graph->succeeds(task0, task_inv).ok());
  CHECK(!task_graph->succeeds(task_inv, task0).ok());
  CHECK(!task_graph->precedes(task0, task_inv).ok());
  CHECK(!task_graph->precedes(task_inv, task0).ok());

  // Clean up
  task_graph.reset(nullptr);
}

TEST_CASE(
    "TaskGraph: Test merge generated task graph",
    "[TaskGraph][merge_generated_task_graph]") {
  // Prepare the function that will generate a task graph
  auto lambda = []() {
    auto task_graph = tdb_make_shared(TaskGraph);
    auto task1 = task_graph->emplace([]() { return Status::Ok(); }, "gen_1");
    auto task2 = task_graph->emplace([]() { return Status::Ok(); }, "gen_2");
    task_graph->succeeds(task2, task1);

    return std::pair<Status, tdb_shared_ptr<TaskGraph>>(
        Status::Ok(), task_graph);
  };

  auto task_graph = tdb_make_shared(TaskGraph);
  auto task0 = task_graph->emplace([]() { return Status::Ok(); }, "0");
  auto task1 = task_graph->emplace(lambda, "1");
  task_graph->succeeds(task1, task0);
  CHECK(task1->execute().ok());

  // Before merging
  CHECK(task_graph->tasks_map().size() == 2);
  CHECK(task1->successors().empty());
  CHECK(task1->generated_task_graph() != nullptr);

  // Merge
  CHECK(task_graph->merge_generated_task_graph(task1->id()).ok());
  CHECK(!task_graph->merge_generated_task_graph(100).ok());

  // After merging
  auto tasks_map = task_graph->tasks_map();
  CHECK(tasks_map.size() == 4);
  auto successors = task1->successors();
  CHECK(successors.size() == 1);
  CHECK(task1->generated_task_graph() == nullptr);
  CHECK(tasks_map[0]->id() == 0);
  CHECK(tasks_map[0]->name() == "0");
  CHECK(tasks_map[1]->id() == 1);
  CHECK(tasks_map[1]->name() == "1");
  CHECK(tasks_map[2]->id() == 2);
  CHECK(tasks_map[2]->name() == "gen_1");
  CHECK(tasks_map[2]->generated_by() == 1);
  CHECK(tasks_map[3]->id() == 3);
  CHECK(tasks_map[3]->name() == "gen_2");
  CHECK(tasks_map[3]->generated_by() == 1);
}