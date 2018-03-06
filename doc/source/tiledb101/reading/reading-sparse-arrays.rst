Reading Sparse Arrays
=====================

Reading in the global cell order
--------------------------------

:ref:`Figure 18 <figure-18>` demonstrates how reads are performed in sparse arrays. 
The figure shows the logical view of the array used in the examples of
:ref:`Figure 14 <figure-14>`  and :ref:`Figure 15 <figure-15>`. 
Once again, from the user's perspective, it does not make a difference if the array is consolidated or not. 
The query subarray is depicted as a blue rectangle. The idea is very similar
to the dense case. The only difference is that the user may specify the
coordinates as an extra attribute called ``TILEDB_COORDS`` if she wishes
to retrieved the coordinates as well (but this is optional). The
resulting buffers after the read are shown in the lower part of the
figure. Observe that TileDB returns only the values of the non-empty
cells inside the subarray. Moreover, assuming that the coordinates are
returned, the values are sorted in the global cell order.

.. _figure-18:

.. figure:: Figure_18.png
    :align: center
    
    Figure 18: Reading from a sparse array in the global cell order
    
Reading in subarray layouts
---------------------------

Similar to the case of dense arrays, the user can read from a sparse
array in a layout other than the global cell order, e.g., she can get
the cell values into her buffers in row- or column-major order within
the specified subarray query. This is illustrated in :ref:`Figure 19 <figure-19>`, which is
the same example as :ref:`Figure 18 <figure-18>`, but now the cell values are returned
sorted in row-major order within subarray ``[3,4], [2,4]``.

.. _figure-19:

.. figure:: Figure_19.png
    :align: center
 
    Figure 19: Reading from a sparse array in row-major subarray layout
