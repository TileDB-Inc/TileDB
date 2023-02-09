

## Application Glossary


#### Task Graph

A *task graph* is a data structure consisting of *nodes* and *edges*.  Each node applies some 
specified computation by invoking its *contained function*.
Edges represent dependencies between nodes.
A *dataflow* graph (or a *flow* graph) passes data between nodes, with the 
output data of one node supplying data to the input of another node.  A *dependency*
graph specifies the dependencies (the ordering of computation) between nodes, 
but does not pass data between nodes.

A basic task graph is assumed to be a directed acyclic graph (DAG).


#### Input


#### Output


#### Port

A port is a binding point between an edge and a node.
A port can be either an input or an output, with respect to a given node.
That is, a node receives data from its input port(s) and transmits data to its output port(s).



#### Node

A node is a locus for computation in the task graph.  A node may have multiple input ports and multiple output ports.
A node has a contained function.
The expected functionality of a node is to apply its contained function to 
data available on the input port(s) and to return its values to the output port(s).


#### Function Node

A function node is neither a root nor leaf in the task graph.  A function node has a contained function. 
The expected functionality of a function node is to apply its contained function to data available on 
its input port and to return its values to its output port.  The prototype for the contained function is:

```c++
Ret fun (const Type& a);
```

Equivalently, the function must satisfy the following:
```c++
  requires std::is_invocable_r_v<Ret, Fun, const Type&>;
```
(Note that the signature does not need to have `const &`).


#### Producer Node

In a task graph, a producer node is a root node in the task graph.  It has only output ports(s) and zero input ports.
A producer node has a contained function.  
The expected functionality of a producer node is to apply its contained function to an `std::stop_source` object
and to return an output item.  The prototype for the contained function is:

```c++
Ret fun (std::stop_source stop);
```

Equivalently, the function must satisfy the following:
```c++
  requires std::is_invocable_r_v<Ret, Fun, std::stop_source>;
```

#### Consumer Node

In a task graph, a producer node is a leaf node in the task graph.  It has only input port(s) and zero output ports.
A consumer node has a contained function.
The expected functionality of a consumer node is to apply its contained function to an input item, 
returning void.  The prototype for the contained function is:

```c++
void fun (const Type& a);
```

Equivalently, the function must satisfy the following:
```c++
  requires std::is_invocable_r_v<void, Fun, const Type&>;
```


#### Multi-Input Multi-Output (MIMO) Node


#### Edge

An edge connects two nodes.  An edge has two endpoints, an output port of one node and an input port of another node.
Edges are parametric on the number of managed data items (currently only either two or three).




#### Segment

#### Segmented Computation


## Implementation Glossary

#### Data Item Type

#### Source

#### Sink

#### Mover

#### Handles

#### Task

#### Scheduler

#### Coroutine

#### Duff's Device

#### Specification Graph

#### Execution Graph
