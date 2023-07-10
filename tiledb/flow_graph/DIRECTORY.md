# Flow Graph

## Directories

### `flow_graph`

The root `flow_graph` directory focuses on clean support for the most common use
cases with flow graphs:

* Specifying a node by means of its node body: `flow_graph/node.h`
* Specifying a flow graph: `flow_graph/graph.h`
* Creating and running a flow graph: `flow_graph/system.h`

Other headers are relegated to subdirectories.

### `flow_graph/system`

This directory contains all the elements that define the subsystem boundaries.
Primarily these definitions are in the form of `concept` definitions.

### `flow_graph/library`

This directory contains behind-the-scenes code required to implement the visible
parts.

### `flow_graph/test`

Top level tests of integrated flow graph components.
