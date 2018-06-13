Tutorial: Dense data ingestion
==============================

In this tutorial we will walk through an example of ingesting dense data into a TileDB array. It's recommended to first read the tutorials on dense arrays and attributes.


Project setup
-------------

For this tutorial we'll ingest dense image data into a TileDB array and perform some basic slices on it. We'll use the widely-available `libpng <https://sourceforge.net/projects/libpng/>`__ library for reading pixel data from png files. The tutorial will also assume you've already installed a TileDB release on your system (see the Installation documentation page for instructions on how to do that) as well as ``libpng``.

To get started, it will be easiest to use the `example CMake project <https://github.com/TileDB-Inc/TileDB/tree/dev/examples/cmake_project>`__ as a template that we will fill in, including linking the ingestor program with ``libpng``.

Clone the TileDB repository and copy the ``examples/cmake_project`` directory to where you want to store this ingestor project::

    $ git clone https://github.com/TileDB-Inc/TileDB.git
    $ mkdir ~/tiledb_projects
    $ cd TileDB
    $ cp -R examples/cmake_project ~/tiledb_projects/png_ingestion
    $ cd ~/tiledb_projects/png_ingestion

Adding ``libpng``
-----------------

First, edit the ``src/main.cc`` file and add the new include at the top::

    #include <png.h>
    // Note: on some macOS platforms with a brew-installed libpng,
    // include instead:
    // #include <libpng16/png.h>

Next, edit ``CMakeLists.txt`` and add the commands to link the executable against libpng::

    # Find and link with libpng.
    find_package(PNG REQUIRED)
    target_link_libraries(ExampleExe "${PNG_LIBRARIES}")
    target_include_directories(ExampleExe PRIVATE "${PNG_INCLUDE_DIRS}")
    target_compile_definitions(ExampleExe PRIVATE "${PNG_DEFINITIONS}")

You should now be able to build and run the example program::

    $ mkdir build
    $ cd build
    $ cmake .. && make
    $ ./ExampleExe


The array schema
----------------

Before ingesting data, we need to design an array schema to hold the data. In this case, the image data is two-dimensional and dense, so we'll ingest the data into a 2D dense array.

PNG pixel data has four component values for each pixel in the image: red, green, blue, and alpha (RGBA). We have several choices on how to store this data.

The most obvious would be to have each cell in the array (corresponding to each pixel in the image) hold a single ``uint32_t`` array with the RGBA value. This would correspond to an array schema with a single attribute named ``rgba`` of type ``uint32_t``.

Because the RGBA value is fundamentally made of four components, we can also store the components separately, where each cell has a separate red, green, blue and alpha value. This would correspond to an array schema with four attributes: ``red``, ``green``, ``blue``, and ``alpha``, all of type ``uint8_t``.

The choice of array schema depends on the type of read queries that will be issued to the array, and whether separate access to the RGBA components will be a common task. For the sake of example, this tutorial will use the second schema, with four attributes.


Reading PNG data
----------------

First we need to interface with ``libpng`` to read pixel data into memory from a ``.png`` file on disk. Insert the following function at the top of ``src/main.cc``. This will read PNG pixel data from a given path, and normalize it such that there are always values for all four RGBA components::

    /**
     * Reads a .png file at the given path and returns a vector of pointers to
     * the pixel data in each row. The caller must free the row pointers.
     *
     * This is a modified version of: https://gist.github.com/niw/5963798
     * "How to read and write PNG file using libpng"
     * (C) 2002-2010 Guillaume Cottenceau
     * Redistributed under the X11 license.
     *
     * @param path Path of .png file
     * @param width Set to the width of the png image
     * @param height Set to the height of the png image
     * @return Vector of pointers to the pixel data in each row.
     */
    std::vector<uint8_t*> read_png(const std::string& path, unsigned* width, unsigned* height) {
      std::vector<uint8_t*> row_pointers;
    
      // Get the image info.
      auto fp = fopen(path.c_str(), "rb");
      png_structp png = png_create_read_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
      png_infop info = png_create_info_struct(png);
      setjmp(png_jmpbuf(png));
      png_init_io(png, fp);
      png_read_info(png, info);

      *width = png_get_image_width(png, info);
      *height = png_get_image_height(png, info);
      uint8_t color_type = png_get_color_type(png, info),
              bit_depth = png_get_bit_depth(png, info);
    
      // Read any color_type into 8bit depth, RGBA format.
      // See http://www.libpng.org/pub/png/libpng-manual.txt
      if (bit_depth == 16)
        png_set_strip_16(png);
    
      if (color_type == PNG_COLOR_TYPE_PALETTE)
        png_set_palette_to_rgb(png);
    
      // PNG_COLOR_TYPE_GRAY_ALPHA is always 8 or 16bit depth.
      if (color_type == PNG_COLOR_TYPE_GRAY && bit_depth < 8)
        png_set_expand_gray_1_2_4_to_8(png);
    
      if (png_get_valid(png, info, PNG_INFO_tRNS))
        png_set_tRNS_to_alpha(png);
    
      // These color_type don't have an alpha channel then fill it with 0xff.
      if (color_type == PNG_COLOR_TYPE_RGB || color_type == PNG_COLOR_TYPE_GRAY ||
          color_type == PNG_COLOR_TYPE_PALETTE)
        png_set_filler(png, 0xFF, PNG_FILLER_AFTER);
    
      if (color_type == PNG_COLOR_TYPE_GRAY ||
          color_type == PNG_COLOR_TYPE_GRAY_ALPHA)
        png_set_gray_to_rgb(png);
    
      png_read_update_info(png, info);
    
      // Set up buffers to hold rows of pixel data.
      for (int y = 0; y < *height; y++) {
        auto row = reinterpret_cast<uint8_t*>(std::malloc(png_get_rowbytes(png, info)));
        row_pointers.push_back(row);
      }
    
      // Read the pixel data.
      png_read_image(png, row_pointers.data());
      fclose(fp);
    
      return row_pointers;
    }

Next, we'll write the function to create a 2D dense array with the schema we designed that will hold the ingested pixel data::

    /**
     * Create a dense array suitable for holding pixel data.
     *
     * @param width Width of array
     * @param height Height of array
     * @param array_name Path of array to create
     */
    void create_array(unsigned width, unsigned height, const std::string& array_name) {
      tiledb::Context ctx;
      tiledb::Domain domain(ctx);
      domain.add_dimension(tiledb::Dimension::create<unsigned>(ctx, "x", {{0, width - 1}}, 100))
          .add_dimension(tiledb::Dimension::create<unsigned>( ctx, "y", {{0, height - 1}}, 100));
    
      tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
      schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}}).set_domain(domain);
    
      schema.add_attribute(tiledb::Attribute::create<uint8_t>(ctx, "red"))
          .add_attribute(tiledb::Attribute::create<uint8_t>(ctx, "green"))
          .add_attribute(tiledb::Attribute::create<uint8_t>(ctx, "blue"))
          .add_attribute(tiledb::Attribute::create<uint8_t>(ctx, "alpha"));
    
      // Create the (empty) array on disk.
      tiledb::Array::create(array_name, schema);
    }

The array schema specifies that the domain of the array will be ``[0, width)`` and ``[0, height)`` in the x and y dimensions, respectively. We've also configured the array such that the red, green, blue, and alpha components will each be stored in separate ``uint8_t`` attributes. We've also chosen a relatively small tile extent of 100x100; for very large (e.g. gigapixel) images it would make sense to increase this to 1000x1000 or even higher.

Next, we'll write the function that, given the RGBA pixel data, splits it into separate attribute buffers and issues a write query to TileDB::

    /**
     * Ingest in-memory PNG pixel data into a dense TileDB array.
     *
     * @param row_pointers PNG data
     * @param width Width of PNG image
     * @param height Height of PNG image
     * @param array_name Path of TileDB array to create.
     */
    void ingest_png_data(
        const std::vector<uint8_t*>& row_pointers,
        unsigned width,
        unsigned height,
        const std::string& array_name) {
      create_array(width, height, array_name);
    
      // Unpack the pixel data into the four attributes.
      std::vector<uint8_t> red, green, blue, alpha;
      for (int y = 0; y < height; y++) {
        auto row = row_pointers[y];
        for (int x = 0; x < width; x++) {
          auto rgba = &row[4 * x];
          uint8_t r = rgba[0], g = rgba[1], b = rgba[2], a = rgba[3];
          red.push_back(r);
          green.push_back(g);
          blue.push_back(b);
          alpha.push_back(a);
        }
      }
    
      // Write the pixel data into the array.
      tiledb::Context ctx;
      tiledb::Array array(ctx, array_name, TILEDB_WRITE);
      tiledb::Query query(ctx, array);
      query.set_layout(TILEDB_ROW_MAJOR)
          .set_buffer("red", red)
          .set_buffer("green", green)
          .set_buffer("blue", blue)
          .set_buffer("alpha", alpha);
      query.submit();
      query.finalize();
      array.close();
    }

All that's left to do is modify the ``main()`` function to call these functions with command-line arguments that specify the path of the input .png file and the output TileDB array::

    int main(int argc, char** argv) {
      std::string input_png(argv[1]), output_array(argv[2]);
      unsigned width, height;
      std::vector<uint8_t*> row_pointers = read_png(input_png, &width, &height);
    
      ingest_png_data(row_pointers, width, height, output_array);

      for (int y = 0; y < height; y++)
        std::free(row_pointers[y]);
    
      return 0;
    }

We can build and run the program to ingest a .png file::

    $ make
    $ ./ExampleExe input.png my_array_name

This will read the file ``input.png``, create a new array in the current directory named ``my_array_name``, and write the pixel data into it.