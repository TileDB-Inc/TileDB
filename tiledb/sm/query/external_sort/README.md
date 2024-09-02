
### Definitions
*   An *input block* is a fixed-size block of memory
*   A *block* contains the sorted data of filled user buffers and is stored in a single temporary array.  The tiles of the array are sized so that a single tile will fit completely into an input block.
*  A *big block* is a set of blocks (arrays) that are sorted with respect to each other and is stored in a temporary group.
*  A *pass* sorts a set of big blocks to create a new big block.
*  An *iteration* comprises multiple passes, each of which sorts a subset of big blocks and produces a new set of big blocks.  An iteration corresponds to the data in one query


### Algorithm:
#### Initialize:
*   Iterate over incomplete queries to create a set of big blocks, each of which corresponds to one incomplete query and each of which consists of one block
#### Process
* Iterator until there is only one big block
  * Partition the big blocks into subsets equal to the number of input blocks in the input block buffer
  * Until all subsets are processed (new pass)
     * Open a new big block
     * Until input blocks are exhausted
         * Merge sort input blocks into user buffers
           * When an input block is depleted, read a new tile from the coresponding big block
           * When user buffers are full, flush to a new block within the new big block

### Directory Structure
The overall temp hierarchical structure from processing a complete query is thus envisioned to look like this:
```
query-results
├── iter_000 (group, set of big blocks, corresponding to one query)
│   ├── pass_000 (group, one big block, set of blocks)
│   │   ├── block_000 (one array, sorted)
│   │   │   ├── attr_0
│   │   │   │   ├── attr_0_data
│   │   │   │   ├── attr_0_offsets
│   │   │   │   └── attr_N_validity
│   │   │   ├── attr_1
│   │   │   ...
│   │   │   └── attr_N
│   │   ├── block_001
│   │   │   ...
│   │   └── block_M
│   ├── pass_001
│   ...
│   └── pass_L
├── iter_001
...
└── iter_K
```

#### Scenario 0:
There is enough memory to that entire query can be loaded into the user buffers.

In this case, only the initialization step is required.  The user buffers are sorted and comprise one big block and one block.
All of the sorted data fits in memory and can be used by the user, or it can be written to a single array that is completely sorted.

(Since there will be no subsequent processing, the array does not have to be tiled.)  In this case, there are is the initial iteration, one pass, one big block, one block, and one (final) array.

#### Scenario 1:
There is not enough memory to load the entire query into the user buffers, there are more input blocks in the input block buffer than there are big blocks produced by the initialization step.

In this case, there will be a sequence of incomplete queries.  In the initialization step, each incomplete query creates a new big block, with one block inside of it.

On the next iteration, all the big blocks can be processed in a single pass to create a single new big block.  All the blocks in the big block will be sorted and will be sorted with respect to each other.  This big block can be consolidated into a single (final) array, or it can fill user buffers directly. 

 #### Scenario 3:
There is not enough memory to load the entire query into the user buffers.  After initialization, there are more temporary big blocks than then are input blocks in the input block buffer.

  In this case, subsets of big blocks are processed at a time to create new big blocks, with each subset creating a new big block.  If there are still more big blocks than there are input blocks in the input block buffer, the process is repeated until only a single output big buffer is created. Thi big block can be consolidated into a single (final) array, or it can be used to fill user buffers directly.


