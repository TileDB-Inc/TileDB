
Task Graph Node API
===================

Nodes
-----

A **task graph node** is a computational unit that applies user-provided functions to data flowing through the graph.  Data flows through the graph (and through the nodes) via **task graph ports**.  A **function node** takes input data from its **input port**, applies a user-defined function and creates output data  on its **output port**.  A **producer node** only creates output; a **consumer node** only takes input.  Output ports from one node connect to the input ports of another via an **edge**. A **simple** node takes one piece of data and produces on piece of data for each input.  A **simple MIMO** node may take multiple inputs and produce multiple outputs;  it creates its outputs using one piece of data from each input.  

![simple_node](./simple_node.png)


![mimo_node](./mimo_node.png)

## API


### `class ProducerNode`


```c++
template <template <class> class Mover_T, class Block>
class ProducerNode;
```
  - `Mover_T` the type of data item mover to be used by the `ProducerNode`
  - `Block` the type of data to be produced by the `ProducerNode`


#### Constructors

```c++
  template <class Function>
  ProducerNode(Function&& f);
```

 - `Function` must meet the requirements of `std::function<Block())>`.


```c++
  template <class Function>
  ProducerNode(Function&& f);
```

 - `Function` must meet the requirements of `std::function<Block(std::stop_source))>`.
 - If this constructor is used, a `stop_source` variable is passed to the enclosed function.  If the enclosed function invokes `stop_source::request_stop()`, the `ProducerNode` will enter the stopping state, which will propagate throught its output port to the connected input port.  The connected node will the enter its shutdown state, and so on, thus stopping computation throughout the entire task graph.


 #### Invocation
 ```c++
  void run_once();
```
- Invokes the function enclosed by the `ProducerNode` and sends it to the `Mover`.
- The output port of the `ProducerNode` must be connected by an edge to an input port of another node.

### Example

```c++
    size_t source_function();
    ProducerNode<AsyncMover3, size_t> b{source_function};
```
### `class ConsumerNode`

```c++
template <template <class> class Mover_T, class Block>
class ConsumerNode;
```
  - `Mover_T` the type of data item mover to be used by the `ConsumerNode`
  - `Block` the type of data to be produced by the `ConsumerNode`


#### Constructor

```c++
  template <class Function>
  ProducerNode(Function&& f);
```
  - `Function` must meet the requirements of `std::function<void(const Block&)>`.

#### Invocation
```c++
 void run_once();
```

- Invokes the function enclosed by the `ConsumerNode`, applying it to the `Block` obtained from the `Mover`.
- The input port of the `ConsumerNode` must be connected by an edge to an output port of another node.

### Example

```c++
    void sink_function(const size_t&);
    Consumer Node<AsyncMover3, size_t> b{sink_function};
```

### `class FunctionNode`

```c++
template <template <class> class InMover_T, class InBlock, class <class> OutMover_T = InMover_T, class OutBlock=InBlock>
class FunctionNode;
```
  - `InMover_T` the type of data item mover to be used for input data by the `FunctionNode`
  - `InBlock` the type of data to be consumed on the input of the `FunctionNode`
   - `OutMover_T` the type of data item mover to be used for output data of the `FunctionNode`
  - `OutBlock` the type of data to be created on the output of the `FunctionNode`


#### Constructor

```c++
  template <class Function>
  ProducerNode(Function&& f);
```
  - `Function` must meet the requirements of `std::function<OutBlock(const InBlock&)>`.

#### Invocation
```c++
void run_once();
```
- Applies the function enclosed by the `FunctionNode` to data obtained from the `InMover` and sends the result to the `OutMover`.
- The input port of the `FunctionNode` must be connected to the output port of another node.  
- The output port of the `FunctionNode` must be connected to the input port of another node.

#### Example

```c++
    size_t function(const size_t&);
    FunctionNode<AsyncMover3, size_t> b{function};
```


### `class MIMOFunctionNode`
```c++
template <
    template <class> class SinkMover_T, class... BlocksIn,
    template <class> class SourceMover_T, class... BlocksOut>
class MimoFunctionNode;
```
#### Constructor

```c++
  template <class Function>
  explicit GeneralFunctionNode(Function&& f);
```
  - `f` must meet the requirements of `std::function<std::tuple<BlocksOut...(std::tuple<BlocksIn...>)>;

### `class Edge`

#### Constructor

```c++
template <template <class> class Mover_T, class Block>
class Edge;
```

- `Mover_T` the type of data item mover to be transferred by the `Edge`
- `Block` the type of data to be transferred by the `Edge`

**Note:** When connecting an output port to an input port, the Mover type and the Block type must be the same.

#### Example

```c++
ProducerNode<AsyncMover3, size_t> a{source_function{}};
ConsumerNode<AsyncMover3, size_t> b{sink_function{}};

Edge e{a, b};

ProducerNode<AsyncMover3, double> c{other_source_function{}};
ConsumerNode<AsyncMover3, size_t> d{sink_function{}};

Edge f{c, d};  // This will fail to compile because Block types of Producer and Consumer don't match.
```

### `class Scheduler`

```c++
class Scheduler;
```
- Base class for schedulers.

#### Constructor
```c++
Scheduler(size_t concurrency_level = std::thread::hardware_concurrency();
```

#### Submitting Tasks
template <class... Tasks>
Scheduler::submit(Tasks&&... tasks);
- Submits `tasks` to internal scheduling member function.

## A Simple Graph

```c++

// Nodes can use function objects as well as functions
template <class Block = size_t>
class source_function {
 public:
  Block operator()(std::stop_source& stopper) {
    // When done
    stopper.request_stop();
    return Block{};
  }
};

template <class InBlock = size_t, class OutBlock = InBlock>
class function {
 public:
  OutBlock operator()(const InBlock&) {
    return OutBlock{};
  }
};

template <class Block = size_t>
class sink_function {
 public:
  void operator()(Block) {
  }
};

size_t actual_source_function() {
  return 0UL;
}

// Create nodes with `AsyncMover3`, a 3-stage item mover (i.e., that connects  
// an output port to an input port with a one buffered block in between).
ProducerNode<AsyncMover3, size_t> a{actual_source_function};
FunctionNode<AsyncMover3, size_t> b{function{}};
ConsumerNode<AsyncMover3, size_t> c{sink_function{}};


// Connect a to b
Edge g{a, b};

// Connect b to c
Edge h{b, c};

size_t rounds = 42;

// Task to invoke `ProducerNode a`.  Invoke stop after `rounds` iterations.
auto fun_a = [&]() {
  size_t N = rounds;
  while (N--) {
    a.run_once();
  }
};

// Task to invoke `FunctionNode b`.  Run until stopped.
auto fun_b = [&]() {
  while (true) {
    b.run_once();
    if (b.stopped()) {
      break;
    }
  }
};

// Task to invoke `ConsumerNode c`.  Run until stopped.
auto fun_c = [&]() {
  while (true) {
    c.run_once();
    if (c.stopped()) {
      break;
    }
  }
};

// Emulate a bountiful scheduler
// Run each node as an asynchronous task
auto fut_a = std::async(std::launch::async, fun_a);
auto fut_b = std::async(std::launch::async, fun_b);
auto fut_c = std::async(std::launch::async, fun_c);

// Wait for completion
fut_a.get();
fut_b.get();
fut_c.get();

// Alternatively, the prototype API for schedulers supports the following,
// which essentially provides the functionality of theemulated scheduler above.
BountifulScheduler sched;

// Launch nodes
sched.submit(a, b, c);

// Wait on their completion
sched.sync_wait_all();
```

