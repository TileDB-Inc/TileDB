Tutorial: Multi-attribute arrays
================================

In this tutorial we will learn how to add multiple attributes to TileDB arrays. It's recommended to read the tutorial on dense arrays first.


Basic concepts
--------------

The tutorials so far have considered arrays where each cell stored a single named value. In TileDB terminology, the named value is called an **attribute**. The attributes are defined in the array schema, meaning conceptually every cell in the same array stores values for the same set of attributes (named values).

TileDB supports an arbitrary number of attributes per array, allowing flexibility in designing your array schema.


Creating a multi-attribute array
--------------------------------

Attributes of arrays are defined when creating the array schema. We'll start with the following 2D dense schema, as we have seen in previous tutorials::

    tiledb::Context ctx;
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    tiledb::Domain domain(ctx);
    domain.add_dimension(tiledb::Dimension::create<int>(ctx, "x", {{0, 3}}, 2))
          .add_dimension(tiledb::Dimension::create<int>(ctx, "y", {{0, 3}}, 2));
    schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
          .set_domain(domain);

As before we’ll create an attribute named ``a1`` for the array that will hold a single character for each cell. We'll also now add a second attribute ``a2`` that will hold a floating-point value for each cell::

    schema.add_attribute(tiledb::Attribute::create<char>(ctx, "a1"));
    schema.add_attribute(tiledb::Attribute::create<float>(ctx, "a2"));

Each cell ``(x,y)`` in the array will contain two values, a character in ``a1`` and a floating point value in ``a2``.

All that’s left to do is create the empty array on disk so that it can be written to::

    tiledb::Array::create("my_array", schema);

Note: in the current version of TileDB, once an array has been created, you cannot modify the array schema. This means that it is not currently possible to add or remove attributes to an already existing array.

Writing to the array
--------------------

Next we’ll write some data to the upper left corner of the array. Specifically, we’ll populate cells (0,0), (1,0), (0,1) and (1,1) with the characters ``a``, ``b``, ``c`` and ``d`` respectively in attribute ``a1`` and the values ``1.3``, ``-1.2``, ``4.0`` and ``5.1`` respectively in attribute ``a2``.

To start, load the data to be written::

    std::vector<char> data_a1 = {'a', 'b', 'c', 'd'};
    std::vector<float> data_a2 = {1.3f, -1.2f, 4.0f, 5.1f};

Next, open the array for writing, and create a query object::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_WRITE);
    tiledb::Query query(ctx, array);

Then, set up the query. We set the buffer for attributes ``a1`` and ``a2``, and also set the subarray for the write, which specifies the bounding box for the cells that we want to write::

    query.set_layout(TILEDB_ROW_MAJOR)
        .set_subarray<int>({0, 1, 0, 1})
        .set_buffer("a1", data_a1)
        .set_buffer("a2", data_a2);
    query.submit();
    array.close();

The array on disk now stores the written data.

**Note**: During writing, you must provide a value for all attributes for the cells being written, otherwise an error will be thrown.


Reading from the array
----------------------

Reading happens in much the same way as writing, except we must provide buffers sufficient to hold the data being read. First, open the array for reading::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_READ);

Next, we set up the same subarray to read all the data that was written, and use the utility function ``max_buffer_elements()`` to allocate buffers large enough::

    std::vector<int> subarray = {0, 1, 0, 1};
    auto max_sizes = array.max_buffer_elements(subarray);
    std::vector<char> data_a1(max_sizes["a1"].second);
    std::vector<float> data_a2(max_sizes["a2"].second);

Then, we set up and submit a query object the same way as for the write::

    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a1", data_a1)
        .set_buffer("a2", data_a2);
    query.submit();
    query.finalize();
    array.close();

The vector ``data_a1`` now contains the resulting cell data for attribute ``a1``, and ``data_a2`` for ``a2``. We can print the results using another utility function ``result_buffer_elements()`` to determine the number of cells actually read::

    int num_cells_read = query.result_buffer_elements()["a1"].second;
    for (int i = 0; i < num_cells_read; i++) {
      char a1 = data_a1[i];
      float a2 = data_a2[i];
      std::cout << "Cell " << i << " has a1 = '" << a1 << "' and a2 = " << a2
                << std::endl;
    }

Subselecting on attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~

While you must provide buffers (values) for all attributes during writes, the same is not true during reads. If you submit a read query with buffers only for some of the attributes of an array, only those attributes will be read from disk. For example, if we modify the read query above to the following::

    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a2", data_a2);
    query.submit();
    query.finalize();
    array.close();

Then only the attribute ``a2`` will be read into the buffer ``data_a2``.

TileDB represents attributes of an array in a **column-oriented** manner. This means that when reading only some of the attributes, the other attributes can be completely ignored by TileDB, which can greatly improve performance during reads.


Image data example
------------------

Let's take the example of 2D RGB image data to examine how multiple attributes can be used effectively. This data can be represented naturally as a dense 2D array, where the dimensions correspond to the rows and columns of pixels, and each cell corresponds to an individual pixel. The question is how to store the RGB values for each pixel (cell).

One option would be to store the RGB value as a single ``uint32_t`` attribute named ``rgb``, e.g.::

    tiledb::Context ctx;
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    tiledb::Domain domain(ctx);
    domain.add_dimension(
              tiledb::Dimension::create<int>(ctx, "x", {{0, image_width - 1}}, 2))
          .add_dimension(
              tiledb::Dimension::create<int>(ctx, "y", {{0, image_height - 1}}, 2));
    schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}}).set_domain(domain);
    schema.add_attribute(tiledb::Attribute::create<uint32_t>(ctx, "rgb"));
    tiledb::Array::create("my_array", schema);

In designing array schemas, one must also consider how the data will be accessed on reads. If the application always needs the full RGB value for each pixel, then the above schema makes sense. However, if the application will treat the color channels separately, accesses to the pixel data may often only be interested in only the red value, and not the green or blue values. In that case, a different schema may make more sense. In particular, we can simply use three attributes instead of one, e.g.::

    schema.add_attribute(tiledb::Attribute::create<uint8_t>(ctx, "red"))
          .add_attribute(tiledb::Attribute::create<uint8_t>(ctx, "green"))
          .add_attribute(tiledb::Attribute::create<uint8_t>(ctx, "blue"));

This will allow the application to sub-select on the different color attributes, instead of always having to read the entire RGB value.