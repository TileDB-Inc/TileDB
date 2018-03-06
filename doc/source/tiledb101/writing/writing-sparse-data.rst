Writing Sparse Data
===================

Writing sparse data applies to both dense and sparse arrays. There are
two writing modes for sparse data; writing in the the global cell order
(which, similar to the dense case, leads to best performance, but with
the limitation that the user must know the tiling and tile/cell order of
the array), and writing **unordered** data (in which case TileDB will
sort them internally along the global cell order prior to writing them
to the disk). We describe these modes in the next two subsections,
respectively.

.. note:: 
    One important difference between dense and sparse writes is that 
    in the sparse writes the user does not need to specify a subarray 
    in which the write will occur. This is because the user specifies 
    explicitly the coordinates of the cells to be written.
    
Writing in global cell order
----------------------------

As in the dense case, the user provides cell buffers to write, whose
cell values are ordered in the global cell order. There are three
differences with the dense case. First, the user provides values only
for the **non-empty** cells. Second, the user includes an extra buffer
with the **coordinates** of the non-empty cells. The coordinates must be
defined as an extra attribute upon the query creation, with the special
name ``TILEDB_COORDS``. Third, TileDB maintains some extra write state
information for each created data tile (recall that a data tile may be
different from a space tile). Specifically, it counts the number of
cells seen so far and, for every ``c`` cells (where ``c`` is the data
tile capacity specified upon array creation), TileDB stores the minimum
bounding rectangle (MBR) and the bounding coordinates (first and last
coordinates) of the data tile. Note that the MBR initially includes the
first coordinates in the data tile, and expands as more coordinates are
seen. For each data tile, TileDB stores its MBR and bounding coordinates
into the fragment metadata.

:ref:`Figure 11 <figure-11>` illustrates an example, using the same sparse array as :ref:`Figure
7 <figure-7>`, and assuming that the user has specified the coordinates as the last
attribute after ``a1`` and ``a2``. The figure shows the logical view of
the array, as well as the contents of the buffers when the user
populates the array with a single write operation. Similar to the dense
case, the user may populate the array with multiple write API calls, as
shown at the lower part of the figure.

.. _figure-11:

.. figure:: Figure_11.png
    :align: center

    Figure 11: Writing sparse data in global cell order
    
Writing unordered cells
-----------------------

Sparse fragments are typically created when random updates arrive at the
system, and it may be cumbersome for the user to sort the random cells
along the global order. To handle this, TileDB also enables the user to
provide unordered cell buffers to write. TileDB internally sorts the
buffers and then proceeds as explained above for the sorted case. Due to
this internal sorting, each write call in unordered mode creates a
**separate** fragment. Therefore, the write operations in this mode are
not simple append operations as in the ordered cell mode, but they are
rather similar to the sorted runs of the traditional `Merge
Sort <https://en.wikipedia.org/wiki/Merge_sort>`__ algorithm. We will
soon explain that TileDB can effectively merge all the sorted fragments
into a single one.

The upper part of :ref:`Figure 12 <figure-12>` demonstrates the unordered mode of operation
for the same array as in :ref:`Figure 11 <figure-11>`. The figure displays the contents of
the user's buffers. Observe that the cell coordinates in ``buffers[3]``
are not ordered on the array global cell order. Also observe that there
is a one-to-one correspondence between the coordinates and the cell
values in the rest of the buffers. TileDB will internally sort all the
values properly, prior to writing the data to the files. Eventually, the
written data will correspond to the logical view of :ref:`Figure 11 <figure-11>`, and the
physical storage will be identical to that of :ref:`Figure 7 <figure-7>`.

.. _figure-12:

.. figure:: Figure_12.png
    :align: center

    Figure 12: Writing unordered sparse data
    
The unordered mode allows multiple writes as well.
However, there are two differences as compared to the multiple writes in
the ordered cell mode. The lower part of :ref:`Figure 12 <figure-12>` depicts the contents
of the buffers for two writes. The first difference is that the
attribute buffers **must be synchronized**, i.e., there must be a
one-to-one correspondence between the values of each cell across all
attributes. The second difference is that each write will create a **new
fragment**, i.e., a new sub directory under the array directory, as
shown in the figure.

Naturally, the ordered writes are more efficient than unordered. This is
because the latter involve an in-memory sorting. Therefore, if the user
is certain that the data in her buffers follow the global cell order,
she must always use the ordered mode.
