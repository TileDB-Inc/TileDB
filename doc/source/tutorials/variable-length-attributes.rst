Tutorial: Variable-length attributes
====================================

In this tutorial we will learn how to add fixed- and variable-length attributes to TileDB arrays. It's recommended to read the tutorial on dense arrays first.


Basic concepts
--------------

Previous tutorials have considered arrays where each cell stored a single named value. In TileDB terminology, the named value is called an **attribute**. The attributes are defined in the array schema, meaning conceptually every cell in the same array stores values for the same set of attributes (named values).

TileDB also supports attributes of fixed- or variable-length, which means a single named value can contain multiple components.

A **fixed-length attribute** allows each cell to store a tuple of values of the same type. For example, each cell could store a pair of integers, or a 10-tuple of floating point values. Fixed-length attributes model data where a single named value contains multiple components.

A **variable-length attribute** allows each cell to store a variable-length list of values of the same type. For example, each cell could store a variable-length list of characters (a string value).


Creating the array
------------------

Attributes of arrays are defined when creating the array schema. We'll start with the following 2D dense schema, as we have seen in previous tutorials::

    tiledb::Context ctx;
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    tiledb::Domain domain(ctx);
    domain.add_dimension(tiledb::Dimension::create<int>(ctx, "x", {{0, 3}}, 2))
          .add_dimension(tiledb::Dimension::create<int>(ctx, "y", {{0, 3}}, 2));
    schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
          .set_domain(domain);

We'll add two attributes: ``a1`` which will store a pair of integers, and ``a2`` which will store a variable-length list of characters (i.e. a string value)::

    schema.add_attribute(tiledb::Attribute::create<int[2]>(ctx, "a1"))
          .add_attribute(tiledb::Attribute::create<std::vector<char>>(ctx, "a2"));

Each cell ``(x,y)`` in the array will contain two named values, a pair of integers in ``a1`` and a variable-length string in ``a2``.

All that’s left to do is create the empty array on disk so that it can be written to::

    tiledb::Array::create("my_array", schema);

Note: in the current version of TileDB, once an array has been created, you cannot modify the array schema. This means that it is not currently possible to add or remove attributes to an already existing array.

Writing to the array
--------------------

Next we’ll write some data to the upper left corner of the array. Specifically, we’ll populate cells (0,0), (1,0), (0,1) and (1,1) with the integer pairs ``(1,2)``, ``(-8,-9)``, ``(2,0)`` and ``(5,10)`` respectively in attribute ``a1`` and the strings ``aaa``, ``b``, ``cc`` and ``dddddd`` respectively in attribute ``a2``.

To start, load the data to be written::

    std::vector<int> data_a1 = {1, 2, -8, -9, 2, 0, 5, 10};
    std::string data_a2 = "aaabccdddddd";
    std::vector<uint64_t> offsets_a2 = {0, 3, 4, 6};

The buffer for the fixed-length attribute ``a1`` is packed such that the data belonging to each cell is directly adjacent in the buffer.

The variable-length attribute ``a2`` is slightly more complicated. The data itself is packed the same way as before in ``data_a2``, but additionally a second vector ``offsets_a2`` stores the beginning offset for each cell's data. The first cell to be written will have data ``aaa``, beginning at offset 0 in ``data_a2``. The second cell will have data ``b`` beginning at offset 3 in ``data_a2``, which is just after the first cell's data. We will see when reading from the array how the offsets will be used to compute the length of a cell's variable-length attribute.

Next, open the array for writing, and create a query object::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_WRITE);
    tiledb::Query query(ctx, array);

Then, set up the query. We set the buffer for attributes ``a1`` and ``a2``, and also set the subarray for the write, which specifies the bounding box for the cells that we want to write. Note that the attribute ``a2`` uses both the offset buffer and the data buffer::

    query.set_layout(TILEDB_ROW_MAJOR)
        .set_subarray<int>({0, 1, 0, 1})
        .set_buffer("a1", data_a1)
        .set_buffer("a2", offsets_a2, data_a2);
    query.submit();
    array.close();

The array on disk now stores the written data.


Reading from the array
----------------------

Reading happens in much the same way as writing, except we must provide buffers sufficient to hold the data being read. First, open the array for reading::

    tiledb::Context ctx;
    tiledb::Array array(ctx, "my_array", TILEDB_READ);

Next, we set up the same subarray to read all the data that was written, and use the utility function ``max_buffer_elements()`` to allocate buffers large enough::

    std::vector<int> subarray = {0, 1, 0, 1};
    auto max_sizes = array.max_buffer_elements(subarray);
    std::vector<int> data_a1(max_sizes["a1"].second);
    std::vector<uint64_t> offsets_a2(max_sizes["a2"].first);
    std::vector<char> data_a2(max_sizes["a2"].second);

The ``max_buffer_elements()`` returns a pair of values for each attribute. The ``first`` value is the maximum number of offsets for that attribute that can be read with the specified subarray. For fixed-length attributes, this value is irrelevant, as there are no offsets read. The ``second`` value is the maximum number of data elements that can be read with the specified subarray. In this case for attribute ``a2`` that is the maximum number of characters of ``a2`` data that could be read.

Then, we set up and submit a query object the same way as for the write::

    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a1", data_a1)
        .set_buffer("a2", offsets_a2, data_a2);
    query.submit();
    query.finalize();
    array.close();

The vector ``data_a1`` now contains the resulting cell data for attribute ``a1``, and ``data_a2`` for ``a2``. Additionally, the ``offsets_a2`` vector contains the offsets of the data in ``data_a2`` for each of the cells that were read.

We can get the total number of cells and total number of bytes of variable-length data that were actually read as follows::

    auto results = query.result_buffer_elements();
    int num_cells_read = results["a1"].second / 2;
    uint64_t total_a2_bytes_read = results["a2"].second;

We divide by two to get the number of cells because ``a1`` stores two values per cell.

To print the results, we simply must reconstruct the attribute values from the buffers that were read::

    for (int i = 0; i < num_cells_read; i++) {
      int a1_a = data_a1[2 * i], a1_b = data_a1[2 * i + 1];

      uint64_t a2_start = offsets_a2[i], a2_end;
      if (i < num_cells_read - 1)
          a2_end = offsets_a2[i + 1];
      else
          a2_end = total_a2_bytes_read;
      uint64_t a2_length = a2_end - a2_start;
      std::string a2(&data_a2[a2_start], a2_length);

      std::cout << "Cell " << i << " has a1 = (" << a1_a << ", " << a1_b
                << ") and a2 = '" << a2 << "'" << std::endl;
    }

For the values of ``a1`` we just use the two adjacent values in ``data_a1``. For the value of the variable-length ``a2`` string, we first compute the length ``a2_end - a2_start`` of the variable data for the current cell, and then construct a string using that information. This will print the following, as expected::

    Cell 0 has a1 = (1, 2) and a2 = 'aaa'
    Cell 1 has a1 = (-8, -9) and a2 = 'b'
    Cell 2 has a1 = (2, 0) and a2 = 'cc'
    Cell 3 has a1 = (5, 10) and a2 = 'dddddd'


Image data example
------------------

Let's take the example of 2D RGB image data to examine how fixed-length attributes can be used effectively. Image data can be represented naturally as a dense 2D array, where the dimensions correspond to the rows and columns of pixels, and each cell corresponds to an individual pixel. The question is how to store the RGB values for each pixel (cell).

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

One could also store the components separately in the ``rgb`` attribute as three separate ``uint8_t`` values::

    schema.add_attribute(tiledb::Attribute::create<uint8_t[3]>(ctx, "rgb"));

Now only 24 bits (uncompressed) of attribute data are stored for the RGB value, saving 1 byte per cell in storage space.

In general, which approach is preferable depends on the particular application and use of the array data.