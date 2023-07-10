# Flow Graph Testing

## `flow_graph/test`

* `test_graph_type.cc`: Classes that support static graph
  specifications
* `test_graph_specification.cc`:
  * Static graph specifications. Compile as `constexpr`. Have referential
    integrity.
  * Conversion of static graph specifications to dynamic ones. Topology of
    converted specification matches original.
* `test_execution_graph.cc`:

## `flow_graph/<subdirectory>/test`

Tests for individual modules. 