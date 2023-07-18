# Flow Graph

## Designated use cases

The `flow_graph` directory focuses on clean support for the most common use
cases with flow graphs:

* Defining a node body.
* Specifying a flow graph.
* Creating a flow graph for execution.
* Running a flow graph.

### Defining a node body

The header `flow_graph/node.h` contains all the concepts needed to define a node
body.

A node body is the middle layer of a node. The bottom layer contains node
services; the top layer contains the node class itself. Each node body contains
a template argument of concept `flow_graph::node_services`. Each node body
instance must satisfy the concept `flow_graph::node_body`. These two concepts
comprise the entirety of the node body interface.

### Specifying a flow graph

The header `flow_graph/graph.h` contains support classes and all the concepts
need to specify a graph, either statically or dynamically.

### Flow graph execution objects

The header `flow_graph/system.h` contains a standard system object that provides
a way of creating an execution graph from a graph specification and for
executing the resulting object.
