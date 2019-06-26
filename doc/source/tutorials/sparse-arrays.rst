.. _sparse-arrays:

Sparse Arrays
=============

In this tutorial we will learn how to create, read, and write a simple sparse
array in TileDB.

.. table:: Full programs
  :widths: auto

  ====================================  =============================================================
  **Program**                           **Links**
  ------------------------------------  -------------------------------------------------------------
  ``quickstart_sparse``                 |quickstartcpp| |quickstartpy|
  ====================================  =============================================================

.. |quickstartcpp| image:: ../figures/cpp.png
   :align: middle
   :width: 30
   :target: {tiledb_src_root_url}/examples/cpp_api/quickstart_sparse.cc

.. |quickstartpy| image:: ../figures/python.png
   :align: middle
   :width: 25
   :target: {tiledb_py_src_root_url}/examples/quickstart_sparse.py


Basic concepts and definitions
------------------------------

.. toggle-header::
    :header: **Sparse array**

      If the majority of the array cells do not have a value, i.e. many cells
      have “undefined” or “empty” values, we call the array sparse.
      We will soon see that in sparse arrays the empty cells are not
      materialized in physical storage.

.. toggle-header::
    :header: **Coordinates**

      The cell coordinates is an ordered tuple where each element is a domain
      value along some array dimension. The coordinates constitute a unique
      identifier for each cell. As we shall see, in sparse arrays the coordinates
      are materialized in physical storage (contrary to the case of dense
      arrays) and facilitate indexing for quickly locating non-empty cells
      that fall within some query subarray.

Creating a sparse array
-----------------------


.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      The following snippet creates an empty array schema for a sparse array:

      .. code-block:: c++

        Context ctx;
        ArraySchema schema(ctx, TILEDB_SPARSE);

      Next, we define a 2D domain where the coordinates can be integer values
      from 1 to 4 (inclusive) along both dimensions. For now, you can ignore
      the last argument in the dimension constructor (tile extent).

      .. code-block:: c++

        Domain domain(ctx);
        domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
              .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));


      Then, attach the domain to the schema, and configure a few other parameters
      (cell and tile ordering) that are explained in later tutorials:

      .. code-block:: c++

        schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

      Finally, create a single attribute named ``a`` for the array that will hold a single
      integer for each cell:

      .. code-block:: c++

        schema.add_attribute(Attribute::create<int>(ctx, "a"));


      The only difference in this sparse array versus the dense array tutorial is the use of
      ``TILEDB_SPARSE`` in creating the ``ArraySchema`` object. Everything else is the same.

   .. tab-container:: python
      :title: Python

      First we define a 2D domain where the coordinates can be integer values
      from 1 to 4 (inclusive) along both dimensions. For now, you can ignore
      the ``tile`` argument in the dimension constructor (tile extent).

      .. code-block:: python

         # Don't forget to 'import numpy as np'
         dom = tiledb.Domain(tiledb.Dim(name="rows", domain=(1, 4), tile=4, dtype=np.int32),
                             tiledb.Dim(name="cols", domain=(1, 4), tile=4, dtype=np.int32))

      Next we create the schema object, attaching the domain and a single attribute ``a``
      that will hold a single integer for each cell:

      .. code-block:: python

         schema = tiledb.ArraySchema(domain=dom, sparse=True,
                                     attrs=[tiledb.Attr(name="a", dtype=np.int32)])


      The only difference in this sparse array versus the dense array tutorial is the use of
      ``sparse=True`` in creating the ``ArraySchema`` object. Everything else is the same.

.. note::

   The order of the dimensions (as added to the domain) is important later when
   specifying subarrays. For instance, in the above example, subarray
   ``[1,2], [2,4]`` means slice the first two values in the ``rows`` dimension
   domain, and values ``2,3,4`` in the ``cols`` dimension domain.

All that is left to do is create the empty array on disk so that it can be written to.
We specify the name of the array to create, and the schema to use. This command
will essentially persist the array schema we just created on disk.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        std::string array_name("quickstart_sparse_array");
        Array::create(array_name, schema);

   .. tab-container:: python
      :title: Python

      .. code-block:: python

         array_name = "quickstart_sparse"
         tiledb.SparseArray.create(array_name, schema)


Writing to the array
--------------------

We will populate the array by writing some values to its cells, specifically
``1``, ``2``, and ``3`` at cells ``(1,1)``, ``(2,4)`` and  ``(2,3)``,
respectively. Notice that, contrary to the dense case, here we specify
the exact indices where the values will be written, i.e., we provide
the cell coordinates.


.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      To start, prepare the data to be written. Below
      ``coords`` refers to the coordinates, whereas ``data`` to the cell values
      on attribute ``a``. Notice also that there is a one-to-one correspondence
      between a coordinates pair and an attribute value (i.e., cell value ``1``
      corresponds to ``(1,1)``, ``2`` to ``(2,4)`` and ``3`` to ``(2,3)``).

      .. code-block:: c++

        std::vector<int> coords = {1, 1, 2, 4, 2, 3};
        std::vector<int> data = {1, 2, 3};

      Next, open the array for writing, and create a query object:

      .. code-block:: c++

        Context ctx;
        Array array(ctx, array_name, TILEDB_WRITE);
        Query query(ctx, array, TILEDB_WRITE);

      Then, set up the query. We set the buffers for attribute ``a`` and coordinates,
      and also set the layout of the cells in the buffer to "unordered". Although
      the cell layout is
      covered thoroughly in later tutorials, here what you should know is that
      you are telling TileDB that the cell values and coordinates in your buffers
      do not follow a particular order (so that TileDB can do its magic to sort
      and index those cells appropriately).

      .. code-block:: c++

        query.set_layout(TILEDB_UNORDERED)
             .set_buffer("a", data);
             .set_coordinates(coords);

      Finally, submit the query and close the array.

      .. code-block:: c++

        query.submit();
        array.close();


   .. tab-container:: python
      :title: Python

      To start, prepare the data to be written.

      .. code-block:: python

         data = np.array(([1, 2, 3]))

      Next, prepare the coordinates of the cells to be written. Below, ``I`` refers
      to coordinates in the ``rows`` dimension and ``J`` to coordinates in the ``cols``
      dimension. Notice also that there is a one-to-one correspondence
      between a coordinates pair and an attribute value (i.e., cell value ``1``
      corresponds to ``(1,1)``, ``2`` to ``(2,4)`` and ``3`` to ``(2,3)``).

      .. code-block:: python

         I, J = [1, 2, 2], [1, 4, 3]

      Finally, open the array for writing and write the data to the array.

      .. code-block:: python

         with tiledb.SparseArray(array_name, mode='w') as A:
             A[I, J] = data

The array data is now stored on disk.
The resulting array is depicted in the figure below.

.. figure:: ../figures/quickstart_sparse.png
   :align: center
   :scale: 40 %

Reading from the array
----------------------

We will next explain how to read the cell values in subarray
``[1,2], [2,4]``, i.e., in the blue rectangle shown in the figure above.
The result values should be ``3 2``, reading in row-major order.


.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      Reading happens in much the same way as writing, except we must provide
      buffers sufficient to hold the data being read. First, open the array for
      reading:

      .. code-block:: c++

        Context ctx;
        Array array(ctx, array_name, TILEDB_READ);

      Next, specify the subarray in terms of ``(min, max)`` values on each
      dimension. One of the most challenging issues is estimating how large
      the result of a read query on a sparse array is, so that you
      know how much space to allocate for your buffers, and how
      to parse the result (this was not an issue in the dense case).
      For now, just notice that function ``max_buffer_elements`` facilitates
      allocating appropriate space that will certainly hold the result
      of the specified subarray in buffers ``data`` and ``coords``. Memory
      allocation for reads is covered thoroughly in later tutorials.

      .. code-block:: c++

          const std::vector<int> subarray = {1, 2, 2, 4};
          auto max_el = array.max_buffer_elements(subarray);
          std::vector<int> data(max_el["a"].second);
          std::vector<int> coords(max_el[TILEDB_COORDS].second);

      Then, we set up and submit a query object, and close the array, similarly to writes.

      .. code-block:: c++

        Query query(ctx, array);
        query.set_subarray(subarray)
             .set_layout(TILEDB_ROW_MAJOR)
             .set_buffer("a", data);
             .set_coordinates(coords);
        query.submit();
        array.close();

      Now ``data`` holds the result **non-empty** cell values on attribute ``a``,
      with their corresponding coordinates being stored in ``coords`` (there is
      always a one-to-one correspondence).

   .. tab-container:: python
      :title: Python

      Reading happens in much the same way as writing, simply specifying a different
      mode when opening the array:

      .. code-block:: python

         with tiledb.SparseArray(array_name, mode='r') as A:
             # Slice only rows 1, 2 and cols 2, 3, 4.
             data = A[1:3, 2:5]

      Now ``data["a"]`` holds the result **non-empty** cell values on attribute ``a``,
      with their corresponding coordinates being stored in ``data["coords"]`` (there is
      always a one-to-one correspondence). Again by default the Python API issues
      the read query in row-major layout.

The row-major layout here means that the cells will be returned in row-major order
**within the subarray** ``[1,2], [2,4]`` (more information on cell layouts
is covered in later tutorials).

If you compile and run this tutorial example as shown below, you should see
the following output:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: bash

         $ g++ -std=c++11 quickstart_sparse.cc -o quickstart_sparse -ltiledb
         $ ./quickstart_sparse
         Cell (2, 3) has data 3
         Cell (2, 4) has data 2

   .. tab-container:: python
      :title: Python

      .. code-block:: bash

         $ python quickstart_sparse.py
         Cell (2, 3) has data 3
         Cell (2, 4) has data 2

On-disk structure
-----------------

A TileDB array is stored on disk as a directory with the name given at the time of array creation.
If we look into the array on disk after it has been written to, we will see something like the following

.. code-block:: bash

    $ ls -l quickstart_sparse_array/
    total 8
    drwx------  5 stavros  staff  160 Jun 25 15:22 __1561490578769_1561490578769_9e429a59930b4a9c83baa57eb2fb41a8
    -rwx------  1 stavros  staff  153 Jun 25 15:22 __array_schema.tdb
    -rwx------  1 stavros  staff    0 Jun 25 15:22 __lock.tdb

The array directory and files ``__array_schema.tdb`` and ``__lock.tdb`` were written upon
array creation, whereas subdirectory 
``__1561490578769_1561490578769_9e429a59930b4a9c83baa57eb2fb41a8`` was
created after array writting. This subdirectory, called **fragment**, contains the written
cell values for attribute ``a`` in file ``a.tdb`` and the corresponding coordinates in
a **separate** file ``__coords.tdb``, along with associated metadata:

.. code-block:: bash

    $ ls -l quickstart_sparse_array/__1561490578769_1561490578769_9e429a59930b4a9c83baa57eb2fb41a8/
    total 24
    -rwx------  1 stavros  staff  106 Jun 25 15:22 __coords.tdb
    -rwx------  1 stavros  staff  611 Jun 25 15:22 __fragment_metadata.tdb
    -rwx------  1 stavros  staff   32 Jun 25 15:22 a.tdb

The TileDB array hierarchy on disk and more details about fragments are discussed in
later tutorials.
