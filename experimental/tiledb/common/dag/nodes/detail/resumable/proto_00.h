/**
* @file   proto_00.h
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
* Very first prototype of a resumable node API
 */
#ifndef TILEDB_DAG_NODES_DETAIL_PROTO_00_H
#define TILEDB_DAG_NODES_DETAIL_PROTO_00_H

#include "experimental/tiledb/common/dag/execution/duffs.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"

namespace tiledb::common {

template <class Node>
struct ResumableTask;

template <class Node>
using ResumableDuffsScheduler =
    DuffsSchedulerImpl<ResumableTask<Node>, DuffsSchedulerPolicy>;

// Same as from duffs.h...
//template <class ResumableTask, class Scheduler>
//struct SchedulerTraits<DuffsSchedulerPolicy<ResumableTask, Scheduler>> {
//  using task_handle_type = ResumableTask;
//  using task_type = typename task_handle_type::element_type;
//};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_DETAIL_PROTO_00_H
