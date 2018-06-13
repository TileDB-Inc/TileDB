Tutorial: Dense arrays
======================

In this tutorial we will learn how to create, read, and write dense arrays in TileDB.


Basic concepts
--------------

An array in TileDB is an n-dimensional collection of **cells**, where each cell is uniquely identified by a coordinate tuple equal to the dimensionality of the array.  For example, every cell in a 2D array is represented by a coordinate pair (x, y).  A 3D array by a coordinate triple, (x, y, z).  A TileDB array has a set of named **attributes**, meaning every cell in the array has one value for every attribute. Each dimension in the array has an associated **domain** which defines the data type and extent (min, max) of coordinate values for that dimension.  The ordered set of dimensions comprise the **array domain**.  All domain extents are inclusive.

In TileDB, you define the structure of an array with an **array schema** that specifies the number, type and name of the dimensions, the domain for each dimension, the type and name of the attributes, and several other parameters. For example, basic image data might be represented by a 2D array where each cell is identified by its pixel coordinates ``(x,y)``, and has an ``rgb`` attribute containing the RGB value for that pixel. The domains for x and y could be the width and height of the image, respectively. Alternatively, image data might be represented as a 3D array where each cell is identified by a triple ``(x,y,c)`` where ``c`` is a dimension with domain ``[0,2]``, indexing the red, green and blue channels independently. The two schemas represents the same data, but with a different organization.

If every cell in the array has an associated value, such as the image example, we call the array **dense**. If not every cell has a associated value, i.e. some cells are "empty" or "undefined", we call the array **sparse**.


Creating a dense array
----------------------

The following snippet creates an empty array schema for a dense array::

    tiledb::Context ctx;
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);

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

Note: the array name here can actually be a full URI, for example a path like ``file:///home/username/my_array`` or an S3 URI like ``s3://bucket-name/array-name``.


Writing to the array
--------------------

Next we’ll write some data to the upper left corner of the array. Specifically, we’ll populate cells (0,0), (1,0), (0,1) and (1,1) with the characters ``a``, ``b``, ``c`` and ``d`` respectively.

To start, load the data to be written::

    std::vector<char> data = {'a', 'b', 'c', 'd'};

Next, open the array for writing, and create a query object::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_WRITE);
    tiledb::Query query(ctx, array);

Then, set up the query. We set the buffer for attribute ``a1``, and also set the **subarray** for the write, which specifies the bounding box for the cells that we want to write::

    query.set_layout(TILEDB_ROW_MAJOR)
        .set_subarray<int>({0, 1, 0, 1})
        .set_buffer("a1", data);
    query.submit();
    array.close();

The array on disk now stores the written data.


Reading from the array
----------------------

Reading happens in much the same way as writing, except we must provide buffers sufficient to hold the data being read. First, open the array for reading::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_READ);

Next, we set up the same subarray to read all the data that was written, and use the utility function ``max_buffer_elements()`` to allocate a buffer large enough::

    std::vector<int> subarray = {0, 1, 0, 1};
    auto max_sizes = array.max_buffer_elements(subarray);
    std::vector<char> data(max_sizes["a1"].second);

Then, we set up and submit a query object the same way as for the write::

    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a1", data);
    query.submit();
    query.finalize();
    array.close();

The vector ``data`` now contains the resulting cell data for attribute ``a1``. We can print the results using another utility function ``result_buffer_elements()`` to determine the number of cells actually read::

    int num_cells_read = query.result_buffer_elements()["a1"].second;
    for (int i = 0; i < num_cells_read; i++) {
      char a = data[i];
      std::cout << a << std::endl;
    }


On-disk structure
-----------------

A TileDB array is stored on disk in a directory-like object with the name given at the time of array creation. If we look into the array on disk after it has been written to, we would see something like the following::

    $ ls -l my_array/
    total 8
    drwx------  4 tyler  staff  136 Jun 11 18:30 __0c4739ed957b4f5eaf0b2738cb1bec1c_1528756214526
    -rwx------  1 tyler  staff  164 Jun 11 18:30 __array_schema.tdb
    -rwx------  1 tyler  staff    0 Jun 11 18:30 __lock.tdb

These files are the associated array metadata that TileDB uses to index the data. The directory ``__0c4739ed957b4f5eaf0b2738cb1bec1c_1528756214526`` (an array **fragment**) contains the written data for attribute `a1` and the associated metadata::

    $ ls -l my_array/__0c4739ed957b4f5eaf0b2738cb1bec1c_1528756214526/
    total 16
    -rwx------  1 tyler  staff  117 Jun 11 18:30 __fragment_metadata.tdb
    -rwx------  1 tyler  staff    4 Jun 11 18:30 a1.tdb

The TileDB array hierarchy on disk and more details about fragments are discussed in later tutorials.
