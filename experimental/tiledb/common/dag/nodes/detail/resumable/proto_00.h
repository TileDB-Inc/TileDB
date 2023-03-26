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
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"


namespace tiledb::common {

class node_base{
 public:
  using node_type = node_base;
  using node_handle_type = std::shared_ptr<node_base>;
};

template <
    class BlocksIn = std ::tuple<std::tuple<>>,
    class BlocksOut = std::tuple<std::tuple<>>>
struct fn_type;

template <class... BlocksIn, class... BlocksOut>
struct fn_type<std::tuple<BlocksIn...>, std::tuple<BlocksOut...>> {
  using type =
      std::function<std::tuple<BlocksOut...>(const std::tuple<BlocksIn...>&)>;
};

template <class... BlocksOut>
struct fn_type<std::tuple<>, std::tuple<BlocksOut...>> {
  using type = std::function<std::tuple<BlocksOut...>(std::stop_source&)>;
};

template <class... BlocksIn>
struct fn_type<std::tuple<BlocksIn...>, std::tuple<>> {
  using type = std::function<void(const std::tuple<BlocksIn...>&)>;
};

template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class>
    class SourceMover,
    class BlocksOut>
class ProtoNodeImpl;

template <
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class ProtoNodeImpl<
    SinkMover,
    std::tuple<BlocksIn...>,
    SourceMover,
    std::tuple<BlocksOut...>> {
  typename fn_type<std::tuple<BlocksIn...>, std::tuple<BlocksOut...>>::type f_;
  std::tuple<Sink<SinkMover, BlocksIn>...> inputs_;
  std::tuple<Source<SourceMover, BlocksOut>...> outputs_;

 public:
  template <class Function>
  explicit ProtoNodeImpl(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<
              std::tuple<BlocksOut...>,
              Function,
              const std::tuple<BlocksIn...>&>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }
};

template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class ProtoNode
    : public std::shared_ptr<
          ProtoNodeImpl<SinkMover, BlocksIn, SourceMover, BlocksOut>> {
  using PreBase = ProtoNodeImpl<SinkMover, BlocksIn, SourceMover, BlocksOut>;
  using Base = std::shared_ptr<PreBase>;
  using Base::Base;

 public:
  template <class Function>
  explicit ProtoNode(Function&& f)
      : Base{std::make_shared<PreBase>(std::forward<Function>(f))} {
  }
};




template <class Node>
class ResumableTask;

template <class Node>
using ResumableDuffsScheduler =
    DuffsSchedulerImpl<ResumableTask<Node>, DuffsSchedulerPolicy>;

// Same as from duffs.h...
//template <class ResumableTask, class Scheduler>
//struct SchedulerTraits<DuffsSchedulerPolicy<ResumableTask, Scheduler>> {
//  using task_handle_type = ResumableTask;
//  using task_type = typename task_handle_type::element_type;
//};


template <class Node>
class ResumableTaskImpl {
  using NodeBase = Node;
  using scheduler_event_type = SchedulerAction;

  using node_type = node_t<Node>;
  using node_handle_type = node_handle_t<Node>;

  using node_state = typename node_type::node_state;
  static constexpr auto n_inputs = node_type::n_inputs;
  static constexpr auto n_outputs = node_type::n_outputs;

  using task_type = ResumableTaskImpl<Node>;

 public:
  ResumableTaskImpl() = default;
  ResumableTaskImpl(const ResumableTaskImpl&) = default;
  ResumableTaskImpl(ResumableTaskImpl&&) noexcept = default;
  ResumableTaskImpl& operator=(const ResumableTaskImpl&) = default;
  ResumableTaskImpl& operator=(ResumableTaskImpl&&) noexcept = default;
  ~ResumableTaskImpl() = default;

  explicit ResumableTaskImpl(const node_type& node)
      : NodeBase{std::make_shared<node_type>(node)} {}

  auto node() {
    return dynamic_cast<NodeBase*>(this);
  }

  node_handle_type& sink_correspondent() {
    return (*this)->sink_correspondent();
  }

  node_handle_type& source_correspondent() {
    return (*this)->source_correspondent();
  }

  std::array<int, n_inputs> in_state;
  std::array<int, n_outputs> out_state;

  scheduler_event_type resume() {
    // pull_all()
    // Is input ready

    // push_all()
    // Is output ready

    return (*this)->resume();
  }

};

template <class Node>
class ResumableTask : public std::shared_ptr<ResumableTaskImpl<Node>> {
  using Base = std::shared_ptr<ResumableTaskImpl<Node>>;

 public:
  using node_type = node_t<Node>;
  using node_handle_type = node_handle_t<Node>;
  using task_type = ResumableTaskImpl<node_handle_type>;
  using task_handle_type = ResumableTask<Node>;


  ResumableTask() = default;
  ResumableTask(const ResumableTask& rhs) = default;
  ResumableTask(ResumableTask&& rhs) noexcept = default;
  ResumableTask& operator=(const ResumableTask& rhs) = default;
  ResumableTask& operator=(ResumableTask&& rhs) noexcept = default;
  ~ResumableTask() = default;

  explicit ResumableTask(const node_handle_type& n)
      : Base{std::make_shared<ResumableTaskImpl<node_handle_type>>(n)} {
  }

  template <class U = Node>
  explicit ResumableTask(
      const node_type& n,
      std::enable_if_t<!std::is_same_v<node_t<U>, node_handle_t<U>>, void**> =
          nullptr)
      : Base{std::make_shared<ResumableTaskImpl<node_handle_type>>(n)} {
  }

  explicit ResumableTask(node_handle_type&& n)
      : Base{std::make_shared<ResumableTaskImpl<node_handle_type>>(n)} {
  }

  template <class U = Node>
  explicit ResumableTask(
      node_type&& n,
      std::enable_if_t<!std::is_same_v<node_t<U>, node_handle_t<U>>, void**> =
          nullptr)
      : Base{std::make_shared<ResumableTaskImpl<node_handle_type>>(n)} {
  }
};



}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_DETAIL_PROTO_00_H
