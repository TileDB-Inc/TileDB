Reading
=======

The read operation returns the values of any subset of attributes inside
a user-supplied subarray. The user specifies the subarray and attributes
upon creating the query. During the query creation, TileDB loads the
metadata of each array fragment from the disk into main memory (which
are generally very small).

The user submits a read query one or multiple times providing his or her
own buffers that will store the results, one per fixed-length attribute,
two per variable-lengths attribute, along with their sizes. TileDB
populates the buffers with the results, following the user-specified
layout within the specified subarray. For variable-sized attributes, the
first buffer contains the starting cell offsets of the variable-sized
cells in the second buffer. Note that, specifically for variable-length
attributes and sparse arrays, the result size may be unpredictable, or
exceed the size of the available main memory. TileDB handles this case
gracefully. If the read results exceed the size of some buffer, TileDB
fills in as much data as it can into the buffer and returns, marking the
query as **incomplete**. The user can then consume the current results,
and resume the process by submitting the read once again (potentially
with different buffers). TileDB can achieve this by properly maintaining
some read state that captures the location where reading stopped. In
addition, to make the memory management in these cases easier on the
user, TileDB offers some auxiliary functions that predict an upper bound
on the memory required for a particular query.

The main challenge of reads is the fact that there may be multiple
fragments in the array. This is because the results must be returned in
some order (the global cell order or another layout, such as
row-/column-major order within the subarray), and read cannot simply
search each fragment individually. The reason is that, if a recent
fragment overwrites a cell of an older fragment, no data should be
retrieved from the old fragment for this cell. TileDB implements a read
algorithm that allows it to skip data that do not qualify for the
result. Independently of the number of fragments in the array, the user
always sees the final array logical view as shown in the examples in the
:doc:`Writing </tiledb101/writing/index>` section.

.. toctree::
    :maxdepth: 2

    reading-dense-arrays
    reading-sparse-arrays
    handling-incomplete-queries
    utility-functions