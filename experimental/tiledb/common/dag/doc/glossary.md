

## Application Glossary


#### Task Graph

Assumed to be directed and acyclic (DAG).



#### Input

#### Output


#### Port

A port is a binding point between an edge and a node.  A port can be either an input or an output, with respect to a given node.  That is, a node receives data from its input port(s) and transmits data to its output port(s).


#### Edge

An edge connects two nodes.  An edge has two endpoints, an output port of one node and an input port of another node.
Edges are parametric on the number of managed data items (currently only either two or three).


#### Node

A node is a locus for computation in the task graph.
A node may have multiple input ports and multiple output ports.
A node has a contained function.
The expected functionality of a node is to apply its contained function to 
data available on the input port(s) and to return its values to the output port(s).


#### Data Item Type


#### Function Node

A function node is neither a root nor leaf in the task graph.


#### Producer Node

In a task graph, a producer node is a root node in the task graph.  It has only output ports(s) and zero input ports.


#### Consumer Node

In a task graph, a producer node is a leaf node in the task graph.  It has only input port(s) and zero output ports.


#### Segment


#### Segmented Computation


## Implementation Glossary

#### Source

#### Sink

#### Mover
