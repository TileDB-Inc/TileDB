

## 0.2 TODO

### Now
  - Performance on par with TBB
  - Suspend / resume
    - Explicit state class
  - API documentation

  - Topological sort / lexicographical order / priority queue scheduling
    - Remove unnecessary locking for communication across edges
    - Edge aware scheduling

  - Incorporate state into segmented node
    - Return state from resume
    - Pass back into resume
    - (Passing state to scheduler may not be necessary.)
    - Double buffer state in `Task`
    - Verify atomicity

  - Checkpointing state
    - Implement stub `pickle()` function for `Task`
      - Save `State`, program_counter, segment_counter
    - Implement `checkpoint` function in scheduler
      - Pickle all `Task`s
      - Pickle other scheduler state
    - Suspend on creation (state at granularity of entire node)

  - std::monostate edge type for synchronization
    - To make dependency graph (just synchronization)

  - Node template library
    - Segmented function API
    - Single segment as case of API

  - Examples
    - Pipeline
      - Simple -- compression / decompression
      - Sieve 


### Soon
  - Integrate into Core
    - Define interaction with existing thread pool (postpone until integrating with Core)

  - More extensive benchmarking
    - Required before execution graph
    - Logging / statistics
    - External tool profiling

  - Work-stealing thread pool
    - Go/no-go based on benchmarking / profiling results

  - Specification graph
    - Correlated data

  - Execution graph
    - Wide nodes / wide edges
      - Given by user
      - Automatically generated
          - Is this a large win?
    - Computation scheduling
    - Optimization of computation
    - Optimization of memory use (and tradeoffs with computation)

  - Prototype / implement filter pipeline 
  - Implement external sort / group by
  - Node template library
    - At least one multiport node
    - Ingest / egress policies
    - More multiport nodes

  - Examples
    - Kite


### Later (maybe never)
  - Buffer nodes
  - Reimplement parallel for / parallel sort with taskgraph (?)
    - Not necessarily a win
  - Distributed taskgraph
    - Define interaction with cloud taskgraph
    - Define interaction between layers (dag for query vs dag for core functionality)
  - Backpressure / lazy evaluation
  - Visualization / tuning 

### Not yet categorized


