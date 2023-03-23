/**
* @file   brainstorm.h
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
* Brainstorming for a resumable node API
*/

#ifndef TILEDB_DAG_NODES_DETAIL_BRAINSTORM_H
#define TILEDB_DAG_NODES_DETAIL_BRAINSTORM_H
#include <array>
#include <optional>
#include <tuple>

namespace tiledb::common {

class base_total_node_state {};

template <unsigned int NumInputs, unsigned int NumOutputs, class T>
struct total_node_state : public base_total_node_state {
  using body_type = T;

  static constexpr unsigned int n_inputs{NumInputs};
  static constexpr unsigned int n_outputs{NumOutputs};
  body_type body_state;

  std::array<int, n_inputs> in;
  std::array<int, n_outputs> out;
  std::tuple<bool, std::optional<T>> run(body_type starting_state);
};

struct node_state {
  int pc;
};

class node_body {
 public:
  using state_type = node_state;
};

enum class progress { ready, blocked, fail };

template <class TNS>  // TNS = Total Node State
class task {
  std::tuple<progress, std::optional<typename TNS::body_type::state_type>> run_body(
      typename TNS::body_type node,
      typename TNS::body_type::state_type old_state) {
    for (unsigned int i = 0; i < TNS::n_inputs; ++i) {
      if (node.in[i] != 0) {
        // We have pending input command, i.e. we tried to pull and failed
      }
    }
    for (unsigned int i = 0; i < TNS::n_outputs; ++i) {
      if (node.out[i] != 0) {
        // We have pending output
      }
    }

    auto&& [success, new_state] = run(old_state);

    if (!success) {
      return {progress::fail, {}};
    }
    for (unsigned int i = 0; i < TNS::n_inputs; ++i) {
      if (node.in[i] != 0) {
        // in_port[i].drain()
        node.in[i] = 0;
      }
    }
    for (unsigned int i = 0; i < TNS::n_outputs; ++i) {
      if (node.out[i] != 0) {
        // out_port[i].fill()
        node.out[i] = 0;
      }
    }
    return {progress::ready, *new_state};
  }
};

class B;



struct Source {
  bool pull();
  bool drain();
};
struct Sink {
  bool push();
  bool fill();
};

template <class N>
class node_traits;

template <>
class node_traits<B> {
 public:
  static constexpr unsigned int n_inputs{1};
  static constexpr unsigned int n_outputs{4};
  using state_type = node_state;
};

template <class body_type>
class TaskServices {
  using Tr = node_traits<body_type>;

 public:
  static constexpr unsigned int n_inputs{Tr::n_inputs};
  static constexpr unsigned int n_outputs{Tr::n_outputs};
  std::array<Source, Tr::n_inputs> in_port;
  std::array<Sink, Tr::n_outputs> out_port;
  std::array<int, Tr::n_inputs> in_state;
  std::array<int, Tr::n_outputs> out_state;

 public:
  std::tuple<bool, std::optional<std::monostate>> get(unsigned int in) {
    if (in_state[in] != 0) {
      throw std::runtime_error("Only one operation per port per activation");
    }
    if (in_port[in].pull()) {
      in_state[in] = 1;
      return {true, {}};
    } else {
      return {false, {}};
    }
  }
  std::tuple<bool, std::monostate> put(unsigned int out) {
    if (out_state[out] != 0) {
      throw std::runtime_error("Only one operation per port per activation");
    }
    out_port[out].push();
    out_state[out] = 1;
  }
};

template <class body_type>
class Task : public body_type {
  using Tr = node_traits<body_type>;
  using body_state = Tr::state_type;
  using services_type = TaskServices<body_type>;

  std::tuple<progress, std::optional<body_state>> run_body(body_state old_state) {
    bool any_success = false;
    for (unsigned int i = 0; i < Tr::n_inputs; ++i) {
      if (services_type::in_state[i] != 0) {
        // We have pending input command, i.e. we tried to pull and failed
        auto port{services_type::in_port[i]};
        auto x{port.pull()};
        if (x) {
          // pull succeeded
          any_success = true;
          services_type::in_state[i] = 1;
        }
      }
    }
    for (unsigned int i = 0; i < TNS::n_outputs; ++i) {
      if (node.out[i] != 0) {
        // We have pending output
      }
    }
    if (!any_success) {
      // No change in I/O state
      return {progress::blocked, {}};
    }
    auto&& [success, new_state] = run(old_state);
    if (!success) {
      return {progress::fail, {}};
    }
    for (unsigned int i = 0; i < TNS::n_inputs; ++i) {
      if (node.in[i] != 0) {
        // in_port[i].drain()
        node.in[i] = 0;
      }
    }
    for (unsigned int i = 0; i < TNS::n_outputs; ++i) {
      if (node.out[i] != 0) {
        // out_port[i].fill()
        node.out[i] = 0;
      }
    }
    return {progress::ready, *new_state};
  }
};

class B : public TaskServices<B> {
 public:

  std::tuple<bool, std::optional<node_state>> run(node_state starting_state);

  bool f() {
    auto&& [got, value]{get(1)};
    if (!got) {
      // No input, do nothing
      return false;
    }
    // Do something, use `value`
    return true;
  }
};

Task<B> x;

}  // namespace detail

#endif  // TILEDB_DAG_NODES_DETAIL_BRAINSTORM_H
