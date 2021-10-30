/**
 * @file   task.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file implements class Task.
 */

#include "tiledb/common/task_graph/task.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/task_graph/task_graph.h"

#include <iostream>
#include <limits>

using namespace tiledb::common;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Task::Task() {
  id_ = std::numeric_limits<uint64_t>::max();  // Invalid id
  func_ = nullptr;
  func_tg_ = nullptr;
  generated_tg_ = nullptr;
  generated_by_ = std::numeric_limits<uint64_t>::max();
}

Task::Task(
    uint64_t id,
    const std::function<Status(void)>& func,
    const std::string& name)
    : id_(id)
    , func_(func)
    , func_tg_(nullptr)
    , name_(name)
    , generated_tg_(nullptr)
    , generated_by_(std::numeric_limits<uint64_t>::max()) {
}

Task::Task(
    uint64_t id,
    const std::function<std::pair<Status, tdb_shared_ptr<TaskGraph>>(void)>&
        func,
    const std::string& name)
    : id_(id)
    , func_(nullptr)
    , func_tg_(func)
    , name_(name)
    , generated_tg_(nullptr)
    , generated_by_(std::numeric_limits<uint64_t>::max()) {
}

Task::~Task() {
}

Task::Task(const Task& task)
    : Task() {
  auto clone = task.clone();
  swap(clone);
}

Task::Task(Task&& task)
    : Task() {
  swap(task);
}

Task& Task::operator=(const Task& task) {
  auto clone = task.clone();
  swap(clone);
  return *this;
}

Task& Task::operator=(Task&& task) {
  swap(task);
  return *this;
}

/* ********************************* */
/*                API                */
/* ********************************* */

uint64_t Task::id() const {
  return id_;
}

void Task::set_id(uint64_t id) {
  id_ = id;
}

const std::string& Task::name() const {
  return name_;
}

const std::vector<tdb_shared_ptr<Task>>& Task::predecessors() const {
  return predecessors_;
}

size_t Task::predecessors_num() const {
  return predecessors_.size();
}

const std::vector<tdb_shared_ptr<Task>>& Task::successors() const {
  return successors_;
}

size_t Task::successors_num() const {
  return successors_.size();
}

Status Task::add_successor(const tdb_shared_ptr<Task>& succ_task) {
  // Check if `succ_task` exists in successors
  for (const auto& succ : successors_) {
    if (succ->id() == succ_task->id()) {
      return LOG_STATUS(Status::TaskError(
          "Cannot add task to successors; task is already included in "
          "successors"));
    }
  }

  successors_.push_back(succ_task);

  return Status::Ok();
}

Status Task::add_predecessor(const tdb_shared_ptr<Task>& pred_task) {
  // Check if `pred_task` exists in predecessors
  for (const auto& pred : predecessors_) {
    if (pred->id() == pred_task->id()) {
      return LOG_STATUS(Status::TaskError(
          "Cannot add task to predecessors; task is already included in "
          "predecessors"));
    }
  }

  predecessors_.push_back(pred_task);

  return Status::Ok();
}

Status Task::execute() {
  if (func_ == nullptr && func_tg_ == nullptr) {
    return LOG_STATUS(
        Status::TaskError("Cannot execute task; task function is null"));
  }

  stats_.set_start_time(std::chrono::steady_clock::now());
  status_.set_status(TaskStatus::Status::RUNNING);

  Status st;
  if (func_ != nullptr) {
    st = func_();
  } else {
    assert(func_tg_ != nullptr);
    auto st_tg = func_tg_();
    st = st_tg.first;
    generated_tg_ = st_tg.second;
  }

  status_.set_status(
      st.ok() ? TaskStatus::Status::COMPLETED : TaskStatus::Status::FAILED);
  stats_.set_end_time(std::chrono::steady_clock::now());

  return st;
}

const tdb_shared_ptr<TaskGraph>& Task::generated_task_graph() const {
  return generated_tg_;
}

uint64_t Task::generated_by() const {
  return generated_by_;
}

void Task::set_generated_by(uint64_t task_id) {
  generated_by_ = task_id;
}

void Task::clear_generated_task_graph() {
  generated_tg_ = nullptr;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Task Task::clone() const {
  Task clone;

  clone.id_ = id_;
  clone.name_ = name_;
  clone.func_ = func_;
  clone.func_tg_ = func_tg_;
  clone.predecessors_ = predecessors_;
  clone.successors_ = successors_;
  clone.status_ = status_;
  clone.generated_tg_ = generated_tg_;
  clone.generated_by_ = generated_by_;

  return clone;
}

void Task::swap(Task& task) {
  std::swap(id_, task.id_);
  std::swap(name_, task.name_);
  std::swap(func_, task.func_);
  std::swap(func_tg_, task.func_tg_);
  std::swap(predecessors_, task.predecessors_);
  std::swap(successors_, task.successors_);
  std::swap(status_, task.status_);
  std::swap(generated_tg_, task.generated_tg_);
  std::swap(generated_by_, task.generated_by_);
}