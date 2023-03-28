

## Application Glossary


### Essentials

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
That is, in general, a node receives data from its input port(s) and transmits data to its output port(s).


#### Node

A node is a locus for computation in the task graph.  A node may have multiple input
ports and multiple output ports.  A node has a contained discrete coroutine.  The expected
functionality of a node is to apply its contained coroutine to data available on the
input port(s) and to return its values to the output port(s).  

[ ***Important!*** *This is probably not the right specification -- need a spec and an API that 
accounts for multiple "pulls" from the input ports.  And pushes?  Just one push, or multiple?* ] 
Once inputs have been supplied to the enclosed function and its execution has started,
no more inputs will be provided to it. Similarly, once the enclosed function has
completed its execution and supplied its results to the output ports, the function
will not be executed again, and no further output will be produced, until its required
inputs become available.


#### Edge

An edge connects two nodes.  An edge has two endpoints, an output port of one node and an input port of another node.
Edges are parametric on the number of managed data items (currently either two or three).


### Stateless Nodes

#### Function Node

A function node is a single-input single-output node that is
neither a root nor leaf in the task graph.  A function node has a contained function. 
The expected functionality of a function node is to apply its contained function to data available on 
its input port and to return its values to its output port.  
The prototype for the function node is:
```c++
template <
    template <class> class SinkMover, class BlockIn, 
    template <class> class SourceMover, class BlockOut>
class function_node;
```
The prototype for the contained function of a function node is:
```c++
  template <class Function>
  explicit function_node(Function&& f);
```
where the enclosed `Function` must satisfy
```c++
     std::is_invocable_r_v<BlockOut, Function, BlockIn&>
  || std::is_invocable_r_v<BlockOut, Function, const BlockIn&>;
```


#### Producer Node

In a task graph, a producer node is a root node in the task graph.  It has only an output port and zero input ports.
A producer node has a contained function.  
The expected functionality of a producer node is to apply its contained function to an `std::stop_source` object
and to return an output item.  The prototype for the producer node is:
```c++
template <template <class> class Mover, class T>
class producer_node;
```
Its constructor for wrapping the contained function is:
```c++
  template <class Function>
  explicit producer_node(Function&& f);
```
where the enclosed `Function` must satisfy
```c++
  requires std::is_invocable_r_v<T, Function, std::stop_source&>;
```

Calling the `request_stop()` method on the `stop_source` object 
signals to the rest of the task graph that the producer will
not produce any more data.  The value returned by the
producer `request_stop()` has been called will be ignored.

#### Consumer Node

In a task graph, a producer node is a leaf node in the task graph.  It has only one
input port and zero output ports.
A consumer node has a contained function.
The expected functionality of a consumer node is to apply its contained function to an input item, 
returning void.  The prototype for the consumer node is:
```c++
template <template <class> class Mover, class T>
class consumer_node;
```
Its constructor for wrapping the contained function is:
```c++
  template <class Function>
  explicit consumer_node(Function&& f);
```
where the enclosed `Function` must satisfy
```c++
  requires std::is_invocable_r_v<void, Function, const T&>;
```

#### Multi-Input Multi-Output (MIMO) Nodes

In a task graph, a MIMO node is a function node with multiple inputs and multiple outputs.
A MIMO node has a contained function.  The expected functionality of a MIMO node is to apply
its contained function to data available on its inputs port and to return its values to its output port.
The prototype for a MIMO node is:
```c++
template <
    template <class> class SinkMover,   class... BlocksIn,
    template <class> class SourceMover, class... BlocksOut >
class mimo_node;
```

Its constructor wrapping its enclosed function is:
```c++
  template <class Function>
  explicit mimo_node(Function&&);
```
Here, `Function` must meet the requirement
```c++
  std::is_invocable_r_v< std::tuple<BlocksOut...>,
                         Function,
                         const std::tuple<BlocksIn...>&>;
```

In addition to the tuple-based interfaces for the constructors, there are overloads that
allow single arguments (for the input or the output or both) to be passed to the constructor without
the need to wrap them in a tuple, providing
equivalence to the simple nodes above.
For example, constructing a MIMO node with a function meeting the following requirements will
construct a SISO function node:
```c++
     std::is_invocable_r_v< BlocksOut..., Function, const BlocksIn >&>
  && sizeof... (BlocksIn) == 1
  && sizeof... (BlocksOut) == 1;
```
Variants of the constructors for MIMO, NIMO, and MINO nodes have been defined
that allow scalars or tuples to be used in any of the locations for the
`BlocksIn` or `BlocksOut` types.


#### NIMO and MINO Nodes

There are two special cases for the MIMO node, 
the no input, multiple output (NIMO) node and the multiple input, no output (MINO) node.
These are respectively equivalent to a producer node with multiple outputs and a
consumer node with multiple inputs.
These are MIMO nodes but with empty output
type or empty input type, respectively.  They are realized as MIMO nodes using the 
same constructor as for the full MIMO node, shown above, 
but NIMO or MINO are special-cased based on the signature of
the enclosed function.  That is, a MIMO node meeting the following requirements will be
a NIMO node:
```c++
     std::are_same_v<std::tuple<>, BlocksIn...>
  && std::is_invocable_r_v<std::tuple<BlocksOut...>, Function, std::stop_source&>;

```
Note that "no input" is indicated by the input type `BlocksIn` to the function defined as
the empty tuple `std::tuple<>`.
Similarly, a MIMO node meeting the following requirements will be
a MINO node:
```c++
     std::are_same_v<std::tuple<>, BlocksOut...>
  && std::is_invocable_r_v<void, Function, std::tuple<BlocksIn...>&>;
```
Here, "no output" is indicated by the output type `BlocksOut` to the function defined as
the empty tuple `std::tuple<>`.


### MIMO Node Activation 

There are four special cases of MIMO nodes:
#### Gather Nodes
Gather nodes have M inputs and one or zero outputs.
    The inputs are conjunctive.  That is, the enclosed
    function runs only when all inputs are available.

#### Scatter Nodes
Scatter nodes have zero or one input and M outputs.
    The outputs are conjunctive.  That is, when the
    enclosed function is executed, it places the same
    output on every output port.  [***Question:*** *the 
    same output or *an* output?* ]

#### Combiner Nodes
Combiner nodes have M inputs and one output.
    The inputs are disjunctive.  That is, the enclosed
    function runs when any input is available.

#### Splitter Nodes
Splitter nodes have one input and M outputs.
    The outputs are disjunctive.  That is, when the
    enclosed function is executed, it places an output on one of the output ports.
    [***Question:*** *Just one? Which one?*]


### Resumable Nodes

#### State

Resumable nodes are task graph nodes that have a specified *state*.  The state consists
of sufficient information such that the enclosed function can return to the caller at an
intermediate point in its execution and then be resumed at that point.  That is,
the enclosed function must be able to return a state to the caller, and it must be able
to be invoked with a state.  Invoking the enclosed function with *any* state must cause 
the enclosed function to resume its execution at the intermediate execution point 
associated with that state.  Any side-effects of the enclosed function must be captured
in the state.  That is, multiple invocations of the enclosed function with the same state
must always produce the same result (i.e., the same next state or the same final result).  

If the enclosed function is invoked with returned states in any order 
and invoked an arbitrary number of times with any stated, when it finally
reaches the end of its execution, it must return the same final result as if it had been
invoked once without its state being saved and restored.

The state must be able to be saved to stable storage
and then restored to the same state.  That is, the state must be serializable.

*Example.* (https://godbolt.org/z/vsjGo1G39)
```c++ 
#include <iostream>
#include <tuple>
#include <vector>
#include <cassert>

struct state {
  int i{0};
  int sum{0};
};

auto fun(state s) {
  for (size_t i = s.i; i < 10; ++i) {
    s.i = i + 1;
    s.sum += i;
    return std::make_tuple(false, s);
  }
  return std::make_tuple(true, s);
}

int main() {
  std::vector<state> states;
  state s{};
  for (size_t j = 0; j < 10; ++j) {
    auto [done, t] = fun(s);
    if (done)
      break;
    states.push_back(t);
    s = t;
  }

  for (size_t j = 0; j < 10; ++j) {
    for (auto s = states.begin() + j; s != states.end(); ++s) {
      auto [done, t] = fun(*s);
      if (done)
	break;
    }
  }
  auto [done, t] = fun(states[9]);
  assert(done);
  assert(t.sum == 45);

  std::cout << "Final sum is " << t.sum << std::endl;
}
```

#### Segment

A *segment* is the portion of computation performed 
between the invocation of the enclosed function in a resumable node and its return.  


#### Segmented Computation

A model of computation associated with a task graph that
based on the use of resumable nodes is called a *segmented computation*.




## Implementation Glossary

#### Data Item Type

#### Source

#### Sink

#### Mover

#### Handles

#### Task

#### Task Graph

#### Scheduler

#### Coroutine

#### Duff's Device

#### Specification Graph

#### Execution Graph
