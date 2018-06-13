Tutorial: Sparse arrays
=======================

In this tutorial we will learn how to create, read, and write sparse arrays in TileDB. Sparse arrays share many of the same concepts as dense arrays in TileDB, so this tutorial closely resembles the dense array tutorial.


Basic concepts
--------------

**Sparse** arrays represent data where not every cell in the array has an associated value. Apart from this conceptual difference, you interact with sparse arrays in TileDB using the same concepts and the same API as dense arrays.


Creating a sparse array
-----------------------

The following snippet creates an empty array schema for a sparse array::

    tiledb::Context ctx;
    tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);

Next, we define an example 2D domain where the coordinates can be integer values from 0 to 3 (inclusive)::

    tiledb::Domain domain(ctx);
    domain.add_dimension(tiledb::Dimension::create<int>(ctx, "x", {{0, 3}}, 2))
          .add_dimension(tiledb::Dimension::create<int>(ctx, "y", {{0, 3}}, 2));

Then, attach the domain to the schema, and configure a few other parameters (cell and tile ordering) that are explained in later tutorials::

    schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
          .set_domain(domain);

Finally, we’ll create a single attribute named ``a1`` for the array that will hold a single character for each cell::

    schema.add_attribute(tiledb::Attribute::create<char>(ctx, "a1"));

All that’s left to do is create the empty array on disk so that it can be written to. We specify the name of the array to create, and the schema to use::

    tiledb::Array::create("my_array", schema);

The only difference in this sparse array versus the dense array tutorial is the use of ``TILEDB_SPARSE`` in creating the ``ArraySchema`` object. Everything else is the same.

Writing to the array
--------------------

Next we’ll write some data to a few cells of the array. Specifically, we’ll populate cells (0,0), (1,0), (1,1) and (2,3) with the characters ``a``, ``b``, ``c`` and ``d`` respectively.

To start, load a buffer with the coordinates of the cells to be written. We'll use a random ordering of coordinates to illustrate that the ordering of the coordinates does not matter. Note that the type (``int``) of the coordinates must be the same as the type used for the ``Dimension`` objects when creating the array schema::

    std::vector<int> coords = {2, 3, 1, 0, 0, 0, 1, 1};

Next, populate a buffer with the data to be written, corresponding to the ordering of the cell coordinates in the ``coords`` buffer::

    std::vector<char> data = {'d', 'b', 'a', 'c'};

Next, open the array for writing, and create a query object::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_WRITE);
    tiledb::Query query(ctx, array);

Then, set up and submit the query. Instead of setting a subarray as in the dense example, here we configure the query with the exact coordinates of the cells to write to. Correspondingly, we use the ``TILEDB_UNORDERED`` query layout::

    query.set_layout(TILEDB_UNORDERED)
        .set_coordinates(coords)
        .set_buffer("a1", data);
    query.submit();
    array.close();

The array on disk now stores the written data.


Reading from the array
----------------------

Reading happens in much the same way as writing, except we must provide buffers sufficient to hold the data being read. First, open the array for reading::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_READ);

Next, we set up a subarray to read all the data that was written. We'll set the subarray to the entire domain of the array, but because the array is sparse, only the cells actually populated in the array will be read. We use the utility function ``max_buffer_elements()`` to allocate a buffer large enough::

    std::vector<int> subarray = {0, 3, 0, 3};
    auto max_sizes = array.max_buffer_elements(subarray);
    std::vector<char> data(max_sizes["a1"].second);
    std::vector<int> coords(max_sizes[TILEDB_COORDS].second);

We allocate a buffer for the coordinates as well so that the corresponding coordinate values for the cells are also read. Then, we set up and submit a query object the same way as for the write::

    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_coordinates(coords)
        .set_buffer("a1", data);
    query.submit();
    query.finalize();
    array.close();

The vector ``data`` now contains the resulting cell data for attribute ``a1``, and the vector ``coords`` now contains the coordinates for each cell read. We can print the results using another utility function ``result_buffer_elements()`` to determine the number of cells actually read::

    int num_cells_read = query.result_buffer_elements()["a1"].second;
    for (int i = 0; i < num_cells_read; i++) {
      int x = coords[2 * i], y = coords[2 * i + 1];
      char a = data[i];
      std::cout << "Cell (" << x << "," << y << ") has data '" << a << "'"
                << std::endl;
    }


On-disk structure
-----------------

A TileDB array is stored on disk in a directory-like object with the name given at the time of array creation. If we look into the array on disk after it has been written to, we would see something like the following::

    $ ls -l my_array/
    total 8
    drwx------  5 tyler  staff  170 Jun 12 10:32 __a71ac7b88bd84bd8897d156397eef603_1528813977859
    -rwx------  1 tyler  staff  164 Jun 12 10:32 __array_schema.tdb
    -rwx------  1 tyler  staff    0 Jun 12 10:32 __lock.tdb

These files are the associated array metadata that TileDB uses to index the data. The directory ``__a71ac7b88bd84bd8897d156397eef603_1528813977859`` (an array **fragment**) contains the written data for attribute ``a1`` (including the coordinates of the written cells) and the associated metadata::

    $ ls -l my_array/__a71ac7b88bd84bd8897d156397eef603_1528813977859/
    total 24
    -rwx------  1 tyler  staff  112 Jun 12 10:32 __coords.tdb
    -rwx------  1 tyler  staff  124 Jun 12 10:32 __fragment_metadata.tdb
    -rwx------  1 tyler  staff    4 Jun 12 10:32 a1.tdb

The TileDB array hierarchy on disk and more details about fragments are discussed in later tutorials.
