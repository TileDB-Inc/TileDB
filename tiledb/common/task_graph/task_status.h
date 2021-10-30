/**
 * @file   task_status.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 This file defines class TaskStatus.
 */

#ifndef TILEDB_TASK_STATUS_H
#define TILEDB_TASK_STATUS_H

namespace tiledb {
namespace common {

/** Maintains status information about a task. */
class TaskStatus {
 public:
  /* ********************************* */
  /*        PUBLIC DATA TYPES          */
  /* ********************************* */

  enum Status { COMPLETED, RUNNING, NOT_STARTED, FAILED, CANCELED };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  TaskStatus();

  /** Destructor. */
  ~TaskStatus();

  /** Copy constructor. */
  TaskStatus(const TaskStatus&) = default;

  /** Move constructor. */
  TaskStatus(TaskStatus&&) = default;

  /** Copy assignment. */
  TaskStatus& operator=(const TaskStatus&) = default;

  /** Move assignment. */
  TaskStatus& operator=(TaskStatus&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Sets the status. */
  void set_status(const TaskStatus::Status& status);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The status of the task. */
  TaskStatus::Status status_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_TASK_STATUS_H
