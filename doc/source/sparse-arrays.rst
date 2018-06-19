Sparse Arrays
=============

In this tutorial we will learn how to create, read, and write a simple sparse
array in TileDB.

.. toggle-header::
    :header: **Example Code Listing**

    .. content-tabs::

       .. tab-container:: cpp
          :title: C++

          .. literalinclude:: {source_examples_path}/cpp_api/quickstart_sparse.cc
             :language: c++
             :linenos:


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

The following snippet creates an empty array schema for a sparse array:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Context ctx;
        ArraySchema schema(ctx, TILEDB_SPARSE);

Next, we define a 2D domain where the coordinates can be integer values
from 1 to 4 (inclusive) along both dimensions. For now, you can ignore
the last argument when adding a dimension (tile extent).

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Domain domain(ctx);
        domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
              .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 4}}, 4));

.. note::

   The order of the dimensions (as added to the domain) is important later when
   specifying subarrays. For instance, in the above example, subarray
   ``[1,2], [2,4]`` means slice the first two values in the ``rows`` dimension
   domain, and values ``2,3,4`` in the ``cols`` dimension domain.

Then, attach the domain to the schema, and configure a few other parameters
(cell and tile ordering) that are explained in later tutorials:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

Finally, create a single attribute named ``a`` for the array that will hold a single
integer for each cell:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        schema.add_attribute(Attribute::create<int>(ctx, "a"));

All that is left to do is create the empty array on disk so that it can be written to.
We specify the name of the array to create, and the schema to use. This command
will essentially persist the array schema we just created on disk.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        std::string array_name("quickstart_sparse");
        Array::create(array_name, schema);

The only difference in this sparse array versus the dense array tutorial is the use of
``TILEDB_SPARSE`` in creating the ``ArraySchema`` object. Everything else is the same.

Writing to the array
--------------------

We will populate the array by writing some values to its cells, specifically
``1``, ``2``, and ``3`` at cells ``(1,1)``, ``(2,4)`` and  ``(2,3)``,
respectively. Notice that, contrary to the dense case, here we specify
the exact indices where the values will be written, i.e., we provide
the cell coordinates. To start, prepare the data to be written. Below
``coords`` refers to the coordinates, whereas ``data`` to the cell values
on attribute ``a``. Notice also that there is a one-to-one correspondence
between a coordinates pair and an attribute value (i.e., cell value ``1``
corresponds to ``(1,1)``, ``2`` to ``(2,4)`` and ``3`` to ``(2,3)``).

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        std::vector<int> coords = {1, 1, 2, 4, 2, 3};
        std::vector<int> data = {1, 2, 3};

Next, open the array for writing, and create a query object:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        Context ctx;
        Array array(ctx, array_name, TILEDB_WRITE);
        Query query(ctx, array);

Then, set up the query. We set the buffers for attribute ``a`` and coordinates,
and also set the layout of the cells in the buffer to "unordered". Although
the cell layout is
covered thoroughly in later tutorials, here what you should know is that
you are telling TileDB that the cell values and coordinates in your buffers
do not follow a particular order (so that TileDB can do its magic to sort
and index those cells appropriately).

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        query.set_layout(TILEDB_UNORDERED)
             .set_buffer("a", data);
             .set_coordinates(coords);

Finally, submit the query and close the array.

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

        query.submit();
        array.close();

The array data is now stored on disk.
The resulting array is depicted in the figure below.

.. figure:: figures/quickstart_sparse.png
   :align: center
   :scale: 40 %

Reading from the array
----------------------

We will next explain how to read the cell values in subarray
``[1,2], [2,4]``, i.e., in the blue rectangle shown in the figure above.
The result values should be ``3 2``, reading in row-major order.

Reading happens in much the same way as writing, except we must provide
buffers sufficient to hold the data being read. First, open the array for
reading:

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

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

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

      .. code-block:: c++

          const std::vector<int> subarray = {1, 2, 2, 4};
          auto max_el = array.max_buffer_elements(subarray);
          std::vector<int> data(max_el["a"].second);
          std::vector<int> coords(max_el[TILEDB_COORDS].second);

Then, we set up and submit a query object, and close the array, similarly to writes.
The row-major layout here means that the cells will be returned in row-major order
**within the subarray** ``[1,2], [2,4]`` (more information on cell layouts
is covered in later tutorials).

.. content-tabs::

   .. tab-container:: cpp
      :title: C++

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
If you compile and run this tutorial example as shown below, you should see
the following output:

.. code-block:: bash

   $ g++ -std=c++11 quickstart_sparse.cc -o quickstart_sparse -ltiledb
   $ ./quickstart_sparse
   Cell (2, 3) has data 3
   Cell (2, 4) has data 2


On-disk structure
-----------------

A TileDB array is stored on disk as a directory with the name given at the time of array creation.
If we look into the array on disk after it has been written to, we will see something like the following

.. code-block:: bash

    $ ls -l my_array/
    total 8
    drwx------  5 tyler  staff  170 Jun 12 10:32 __a71ac7b88bd84bd8897d156397eef603_1528813977859
    -rwx------  1 tyler  staff  164 Jun 12 10:32 __array_schema.tdb
    -rwx------  1 tyler  staff    0 Jun 12 10:32 __lock.tdb

The array directory and files ``__array_schema.tdb`` and ``__lock.tdb`` were written upon
array creation, whereas subdirectory ``__a71ac7b88bd84bd8897d156397eef603_1528813977859`` was
created after array writting. This subdirectory, called **fragment**, contains the written
cell values for attribute ``a`` in file ``a.tdb`` and the corresponding coordinates in
a **separate** file ``__coords.tdb``, along with associated metadata:

.. code-block:: bash

    $ ls -l my_array/__a71ac7b88bd84bd8897d156397eef603_1528813977859/
    total 24
    -rwx------  1 tyler  staff  112 Jun 12 10:32 __coords.tdb
    -rwx------  1 tyler  staff  124 Jun 12 10:32 __fragment_metadata.tdb
    -rwx------  1 tyler  staff    4 Jun 12 10:32 a1.tdb

The TileDB array hierarchy on disk and more details about fragments are discussed in
later tutorials.
