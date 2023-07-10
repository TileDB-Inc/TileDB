//
// Created by EH on 4/27/2023.
//

#ifndef TILEDB_SINGLE_ELEMENT_GENERATOR_H
#define TILEDB_SINGLE_ELEMENT_GENERATOR_H

#include "../../node.h"

namespace tiledb::flow_graph::library {

/*
 * This class is named "single element generator". What's here at present is
 * hard-coded to a monostate, a type that can only be default-constructed. That
 * may be all that's needed for such a test class. It may also be desirable to
 * specify a type and constructor arguments if needed.
 */

template <node_services NS>
class SingleMonostateGenerator {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_body = true;
  } invariants;
  class OutputPort {
  } output;
  void operator()(){};
};

class MonostateOutputPortSpecification {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_output_port_static_specification{true};
  } invariants;
  using flow_type = std::monostate;
};

struct SingleMonostateGeneratorSpecification {
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_static_specification = true;
  } invariants;
  SingleMonostateGeneratorSpecification() = default;
  static constexpr MonostateOutputPortSpecification output;
  static constexpr std::tuple input_ports{};
  static constexpr std::tuple output_ports{output};
  template <node_services NS>
  using node_body_template = SingleMonostateGenerator<NS>;
};

static_assert(
    node_body<SingleMonostateGenerator>,
    "SingleMonostateGenerator is supposed to be an execution node");
static_assert(
    node_static_specification<SingleMonostateGeneratorSpecification>,
    "SingleMonostateGeneratorSpecification is supposed to be a specification "
    "node");

}  // namespace tiledb::flow_graph::library
#endif  // TILEDB_SINGLE_ELEMENT_GENERATOR_H
