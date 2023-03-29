

## Application Glossary

[ ***Question:*** *How are we deciding what is essential vs
what is implementation?* ]


### Essentials

[ ***Comment:*** *Basically there should be 1-1 correspondence between Essentials and the concepts we define for the task graph: task graph, node, edge, function, coroutine.  Those concepts will be parameterized -- are those parameters also Essential?  That may require us to also define a (compute) `State` concept, depending on how we define the `Coroutine` concept.  Our actual edge and node datatypes are highly parameterized, but with things that are deduced from the constructor, i.e., the signature of `Function` or of `Coroutine`.  Those types are not Essential.*]

#### Task Graph

A *task graph* is a data structure consisting of *nodes* and *edges*.  Each node applies some 
specified computation by invoking its *contained function*.
Edges represent dependencies between nodes.
A *dataflow* graph (or a *flow* graph) passes data between nodes, with the 
output data of one node supplying data to the input of another node.  A *dependency*
graph specifies the dependencies (the ordering of computation) between nodes, 
but does not pass data between nodes.

A basic task graph is assumed to be a directed acyclic graph (DAG).

[ ***Important!*** *We need to define the semantics of the task graph 
from the user point of view (or maybe that is the distinction between
essential and implementation?  In that case a minimal API for adding nodes
and edges to a graph would consist of* ]
```c++
// These should be only internally defined concepts since users are not going to define their own types
// (In which case do we need them at all?)  Initially it may be sufficient to just define the API.  It will
// take some time to really get the concepts right.
// For a very abbreviated hack, see https://godbolt.org/z/nGz9jGea8
concept task_graph = requires { ... };
concept function = requires { ... };  // Probably need different name to avoid naming conflicts maybe just a namespace
concept discrete_coroutine = requires { ... };
concept node = requires { ... };  // just node or source and sink?
concept edge = requires { ... };

template <task_graph Graph, function Function>
node auto add_node(Graph& graph, Function&& f);

template <task_graph Graph, discrete_coroutine Coroutine>
node auto add_node(Graph& graph, Coroutine&& f);

template <task_graph Graph, node From, node To>  // just node or source and sink?
edge auto add_edge(Graph& graph, From&& from, To&& to);

template <task_graph Graph>
void sync_wait(Graph& graph);
```
*If we can't get everything packed into one overload of `add_node`, will need to separately
define `initial_node`, `transform_node`, and `terminal_node` functions as well as
potentially mimo vs siso vs function vs discrete coroutine variants*


<!--
#### Input

#### Output

#### Port
[***Comment:*** *We don't have a concept for this and users never see ports.  Move to Implementation?*

A port is a binding point between an edge and a node.
A port can be either an input or an output, with respect to a given node.
That is, in general, a node receives data from its input port(s) and transmits data to its output port(s).
-->

#### Node

A node is a locus for computation in the task graph.  A node may have multiple input
ports and multiple output ports.  A node contains a discrete coroutine (the "enclosed coroutine").
The expected functionality of a node is to apply its enclosed coroutine to data available on the
input port(s) and to return its values to the output port(s).  


#### Edge

An edge connects two nodes.  An edge has two endpoints, an output port of one node and an input port of another node.
Edges are parametric on the number of managed data items (currently either two or three).


#### Discrete Coroutine

[ ***Important!*** *This needs to be made precise. 
And we are going to need a state machine to implement it properly.* ] 
A discrete coroutine is a function that may return to its caller and be 
resumed at the point directly following the return.
A discrete coroutine is
invoked with an input and an initial computation state,
and it produces an output and a final computation state.  
In addition, a discrete coroutine may be in one of four run states:
* *initial run state* -- the coroutine has not yet been invoked
* *active run state* -- the coroutine has been invoked and has not yet completed
* *final run state* -- the coroutine has completed
* *invalid run state* -- the coroutine has been invoked after it has completed

[ ***Need to distinguish states:*** *run state and computation state good enough?
Probably also need coroutine state (yield, done), node state (equivalent to run state?) 
and task state (running, runnable, waiting, created, finished)* ]

In between the initial and final states, when the coroutine is in the active run state,
the coroutine may return to the caller.  
When the coroutine returns to the caller it will
provide its current run state and its current computation state.  
If the returned run state is *active state*, the coroutine may be resumed.  
If the returned run state is *final state*, the coroutine may not be resumed again, it
may only be invoked again (i.e., presented with a new input and initial computation state).
(* Note: we do this so that the defined behavior of the coroutine in mapping inputs
to outputs is preserved.  That is, we don't want the coroutine to get a new round of inputs
unless it has produced its specified outputs. *)

When a coroutine returns to the caller, it may request that the caller provide it with 
additional inputs.  [ ***Note:*** *Providing the inputs is a marker, we can't back up before that.  Maybe we should consider every provision of input as the beginning of an invocation and just allow there to be one or zero outputs on completion of an invocation.* ]


<!--
This is probably not the right specification -- need a spec and an API that 
accounts for multiple "pulls" from the input ports.  And pushes?  Just one push, or multiple?
We need to define the "beginning" of the coroutine as well as the "end".  Those need to be
related to initial and final state.  And related to when the graph computation is complete.

Once inputs have been supplied to the enclosed coroutine and its execution has started,
no more inputs will be provided to it. Similarly, once the enclosed coroutine has
completed its execution and supplied its results to the output ports, the coroutine
may not be executed again, and no further output will be produced, until its required
inputs become available.
-->

#### Function

A degenerate case of a discrete coroutine is simply a function that runs from initiation and 
does not return to its caller until it has completed its execution.  
It does not have a run state nor an execution state.


## Implementation Glossary


### Stateless Nodes

[ ***Question:*** *Do we need to define the stateless nodes that have functions?  
Or can we just define the stateful nodes that have coroutines? 
I suppose a function is a degenerate case of coroutine, though it does not 
have a computation state and does not
need to return any run state or computation state to the caller.  Perhaps we can
special-case or CTAD the general resumable nodes but it might be simpler for now 
to just keep these around, at least implementation wise.  For the glossary we could
just indicate there are these degenerate cases.* ]

[ ***Also*** *With the current task graph, I call these "initial", "transform", and "terminal"
so perhaps we should adopt that terminology for the glossary?*]


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
[ ***Question:*** *Should we just go ahead and use C++20 concepts to specify requirements?* ]

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


### MIMO Node Activations

[ ***Comment:*** *I think we can define / implement this as a shim between the ports and the function.
In fact, I think that is the only way that makes sense, if the number of ports is deduced from the arity
of the function input or return type.*]

There are four special cases of MIMO nodes:
#### Gather Nodes
Gather nodes have M inputs and one or zero outputs.
    The inputs are conjunctive.  That is, the enclosed
    coroutine is invoked  only when all inputs are available.
The (functional) signature of a coroutine enclosed in a gather node would look like:
```c++
   (std::is_invocable_r_v< std::tuple<BlocksOut...>, Function, const std::tuple<BlocksIn...>&> && (sizeof(BlocksOut...) == 1)
||  std::is_invocable_r_v< void, Function, const std::tuple<BlocksIn...>&>
```

#### Scatter Nodes
Scatter nodes have zero or one input and M outputs.
    The outputs are conjunctive.  That is, when the
    enclosed coroutine completes, it places one
    output element on each output port.
The (functional) signature of a coroutine enclosed in a scatter node would look like:
```c++
   (std::is_invocable_r_v< std::tuple<BlocksOut...>, Function, const std::tuple<BlocksIn...>&> && (sizeof(BlocksIn...) == 1)
||  std::is_invocable_r_v< std::tuple<BlocksOut...>, Function, std::stop_source>
||  std::is_invocable_r_v< std::tuple<BlocksOut...>, Function, void>
```

#### Broadcast Nodes
Scatter nodes have zero or one input and M outputs.
The outputs are conjunctive.  That is, when the
enclosed coroutine completes, it places the same
output on every output port.
The (functional) signature of a coroutine enclosed in a scatter node would look like:
```c++
   (sizeof...(BlocksIn) == 1 && sizeof...(BlocksOut) == 1 
    && std::is_invocable_r_v<std::tuple<BlocksOut...>, Function, std::tuple<BlocksIn...>>)
|| (sizeof...(BlocksOut) == 1 
    && std::is_invocable_r_v<std::tuple<BlocksOut...>, Function, std::stop_source>
```
The node enclosing the coroutine is responsible for replicating the result of the coroutine
invocation to each of the output ports.


#### Combiner Nodes
Combiner nodes have M inputs and zero or one output.
    The inputs are disjunctive.  That is, the enclosed
    coroutine is invoked when any input is available.
    [***just one or as many are available?***  *I think it should be just one
    for the predefined case -- we can define more complicated policies later.
    Fairness?* ]
The (functional) signature of a coroutine enclosed in a scatter node would look like:
```c++
     (sizeof...(BlocksOut) == 1 
   && std::is_invocable_r_v<std::tuple<BlocksOut...>, Function, std::tuple<BlocksIn...>>)
|| std::is_invocable_r_v<void, Function, std::tuple<BlocksIn...>>
```
The node enclosing the coroutine is responsible for selecting the input port that provides
data for the coroutine invocation.


#### Splitter Nodes
Splitter nodes have zero or one input and M outputs.
    The outputs are disjunctive.  That is, when the
    enclosed coroutine completes, it places an 
    output on one of the output ports.
    [***Question:*** *Just one? Which one?  Fairness? 
    Random?  Round Robin?  Whatever is available?*]
```c++
     (sizeof...(BlocksIn) == 1 
   && std::is_invocable_r_v<std::tuple<BlocksOut...>, Function, std::tuple<BlocksIn...>>)
|| std::is_invocable_r_v<std::tuple<BlocksOut...>, Function, void>
```
The node enclosing the coroutine is responsible for selecting the output port to
which the result of the coroutine invocation will be provided.



### Resumable Node

Resumable nodes are like the nodes described above, but they contain
coroutines rather than functions.  The interface to the nodes is the same,
but the enclosed functions must satisfy different requirements.

[ ***Important:*** *What should these be? Which of output, 
compute state, run state do we need to return?  Should we
return something else altogether?  A coroutine state?* ]

Transform node:
```c++
     std::is_invocable_r_v<BlockOut, Function, BlockIn&>
```
or
```c++
     std::is_invocable_r_v<std::tuple<counter, BlockOut>, Function, const BlockIn&>;
```
or
```c++
     std::is_invocable_r_v<counter, Function, const BlockIn&, BlockOut&>;
```
or
```c++
     std::is_invocable_r_v<std::tuple<compute_state, BlockOut>, Function, const BlockIn&>;
```
or
```c++
     std::is_invocable_r_v<compute_state, Function, const BlockIn&, BlockOut&>;
```



Initial node:
```c++
     std::is_invocable_r_v<BlockOut, Function, BlockIn&>
  || std::is_invocable_r_v<BlockOut, Function, const BlockIn&>;
```

Terminal node:
```c++
     std::is_invocable_r_v<BlockOut, Function, BlockIn&>
  || std::is_invocable_r_v<BlockOut, Function, const BlockIn&>;
```


#### Computation State

Resumable nodes are task graph nodes that have a specified *computation state*.  
Computation state consists
of sufficient information such that the enclosed coroutine can return to the caller at an
intermediate point in its execution and then be resumed at that point.  That is,
the enclosed function must be able to return a computation state to the caller, 
and it must be able to be invoked with a computation state.  
When the enclosed coroutine with *any* intermediate computation state it must  
resume its execution at the intermediate execution point 
associated with that state.  Any side-effects of the enclosed function must be captured
in the computation state.  That is, multiple invocations of the enclosed function with 
the same computation state
must always produce the same result (i.e., the same next intermediate state).

If the enclosed coroutine is invoked with returned states in any order 
and invoked an arbitrary number of times with any stated, when it finally
reaches the end of its execution, it must return the same final result as if it had been
invoked once without its state being saved and restored (i.e., as if it were a
"normal" function).

Computation state must be able to be saved to stable storage
and then restored.  That is, computation state must be serializable.

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



#### Data Item Type

#### Port

#### Source

#### Sink

#### Mover

#### Task

<!--
#### Task Graph
#### Coroutine
-->

#### Scheduler

#### Duff's Device

#### Specification Graph

#### Execution Graph
