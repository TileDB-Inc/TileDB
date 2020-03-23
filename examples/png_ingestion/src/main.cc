/**
 * @file   main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This is a simple ingestor program for TileDB that ingests PNG data into an
 * array, slices from the array and creates a new PNG image of the slice.
 */

#include <cassert>
#include <cstdio>

#include <png.h>
// Note: on some macOS platforms with a brew-installed libpng, use this instead:
//#include <libpng16/png.h>

// Include the TileDB C++ API headers
#include <tiledb/tiledb>

using namespace tiledb;

/**
 * Reads a .png file at the given path and returns a vector of pointers to
 * the pixel data in each row. The caller must free the row pointers.
 *
 * This is a modified version of: https://gist.github.com/niw/5963798
 * "How to read and write PNG file using libpng"
 * (C) 2002-2010 Guillaume Cottenceau
 * Redistributed under the X11 license.
 */
std::vector<uint8_t*> read_png(
    const std::string& path, unsigned* width, unsigned* height) {
  std::vector<uint8_t*> row_pointers;

  // Get the image info.
  auto fp = fopen(path.c_str(), "rb");
  png_structp png =
      png_create_read_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
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
    auto row = (uint8_t*)(std::malloc(png_get_rowbytes(png, info)));
    row_pointers.push_back(row);
  }

  // Read the pixel data.
  png_read_image(png, row_pointers.data());
  fclose(fp);

  return row_pointers;
}

/**
 * Writes a .png file at the given path using a vector of pointers to
 * the pixel data in each row. The caller must free the row pointers.
 *
 * This is a modified version of: https://gist.github.com/niw/5963798
 * "How to read and write PNG file using libpng"
 * (C) 2002-2010 Guillaume Cottenceau
 * Redistributed under the X11 license.
 */
void write_png(
    std::vector<uint8_t*>& row_pointers,
    unsigned width,
    unsigned height,
    const std::string& path) {
  FILE* fp = fopen(path.c_str(), "wb");
  if (!fp)
    abort();

  png_structp png =
      png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
  if (!png)
    abort();

  png_infop info = png_create_info_struct(png);
  if (!info)
    abort();

  if (setjmp(png_jmpbuf(png)))
    abort();

  png_init_io(png, fp);

  // Output is 8bit depth, RGBA format.
  png_set_IHDR(
      png,
      info,
      width,
      height,
      8,
      PNG_COLOR_TYPE_RGBA,
      PNG_INTERLACE_NONE,
      PNG_COMPRESSION_TYPE_DEFAULT,
      PNG_FILTER_TYPE_DEFAULT);
  png_write_info(png, info);

  // To remove the alpha channel for PNG_COLOR_TYPE_RGB format,
  // Use png_set_filler().
  // png_set_filler(png, 0, PNG_FILLER_AFTER);

  png_write_image(png, row_pointers.data());
  png_write_end(png, NULL);

  fclose(fp);
}

/**
 * Create a TileDB array suitable for storing pixel data.
 *
 * @param width Number of columns in array domain
 * @param height Number of rows in array domain
 * @param array_path Path to array to create
 */
void create_array(
    unsigned width, unsigned height, const std::string& array_path) {
  Context ctx;
  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create<unsigned>(ctx, "y", {{0, height - 1}}, 100))
      .add_dimension(
          Dimension::create<unsigned>(ctx, "x", {{0, width - 1}}, 100));

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}}).set_domain(domain);
  schema.add_attribute(Attribute::create<uint8_t>(ctx, "red"))
      .add_attribute(Attribute::create<uint8_t>(ctx, "green"))
      .add_attribute(Attribute::create<uint8_t>(ctx, "blue"))
      .add_attribute(Attribute::create<uint8_t>(ctx, "alpha"));

  // Create the (empty) array on disk.
  Array::create(array_path, schema);
}

/**
 * Ingest the pixel data from the given .png image into a TileDB array.
 *
 * @param input_png Path of .png image to ingest.
 * @param array_path Path of array to create.
 */
void ingest_png(const std::string& input_png, const std::string& array_path) {
  // Read the png file into memory
  unsigned width, height;
  std::vector<uint8_t*> row_pointers = read_png(input_png, &width, &height);

  // Create the empty array.
  create_array(width, height, array_path);

  // Unpack the row-major pixel data into four attribute buffers.
  std::vector<uint8_t> red, green, blue, alpha;
  for (unsigned y = 0; y < height; y++) {
    auto row = row_pointers[y];
    for (unsigned x = 0; x < width; x++) {
      auto rgba = &row[4 * x];
      uint8_t r = rgba[0], g = rgba[1], b = rgba[2], a = rgba[3];
      red.push_back(r);
      green.push_back(g);
      blue.push_back(b);
      alpha.push_back(a);
    }
  }

  // Clean up.
  for (int y = 0; y < height; y++)
    std::free(row_pointers[y]);

  // Write the pixel data into the array.
  Context ctx;
  Array array(ctx, array_path, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("red", red)
      .set_buffer("green", green)
      .set_buffer("blue", blue)
      .set_buffer("alpha", alpha);
  query.submit();
  query.finalize();
  array.close();
}

/**
 * Reads a slice of image data from a TileDB array, converts it to greyscale,
 * and writes a new image with the resulting image data.
 *
 * @param array_path Path of array to read from.
 * @param output_png Path of .png image to create.
 */
void slice_and_desaturate(
    const std::string& array_path, const std::string& output_png) {
  Context ctx;
  Array array(ctx, array_path, TILEDB_READ);

  // Get the array non-empty domain, which corresponds to the original image
  // width and height.
  auto non_empty = array.non_empty_domain<unsigned>();
  auto array_y_min = non_empty[0].second.first,
       array_y_max = non_empty[0].second.second,
       array_x_min = non_empty[1].second.first,
       array_x_max = non_empty[1].second.second;
  auto array_height = array_y_max - array_y_min + 1,
       array_width = array_x_max - array_x_min + 1;

  // Read ("slice") the lower left quarter of the image.
  std::vector<unsigned> subarray = {
      array_height / 2, array_height - 1, 0, array_width / 2};
  auto output_height = subarray[1] - subarray[0] + 1,
       output_width = subarray[3] - subarray[2] + 1;

  // Allocate buffers to read into.
  auto max_elements = array.max_buffer_elements(subarray);
  std::vector<uint8_t> red(max_elements["red"].second),
      green(max_elements["green"].second), blue(max_elements["blue"].second),
      alpha(max_elements["alpha"].second);

  // Read from the array.
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_subarray(subarray)
      .set_buffer("red", red)
      .set_buffer("green", green)
      .set_buffer("blue", blue)
      .set_buffer("alpha", alpha);
  query.submit();
  query.finalize();
  array.close();

  // Allocate a buffer suitable for passing to libpng.
  std::vector<uint8_t*> desaturated;
  for (unsigned y = 0; y < output_height; y++)
    desaturated.push_back(
        (uint8_t*)std::malloc(output_width * 4 * sizeof(uint8_t)));

  // Compute and store the desaturated pixel values.
  for (unsigned y = 0; y < output_height; y++) {
    uint8_t* row = desaturated[y];
    for (unsigned x = 0; x < output_width; x++) {
      unsigned i = y * output_width + x;
      auto rgba = &row[4 * x];
      auto grey = (uint8_t)((red[i] + green[i] + blue[i]) / 3.0f);
      rgba[0] = rgba[1] = rgba[2] = grey;
      rgba[3] = alpha[i];
    }
  }

  // Write the image.
  write_png(desaturated, output_width, output_height, output_png);

  // Clean up.
  for (unsigned i = 0; i < output_height; i++)
    std::free(desaturated[i]);
}

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cout << "USAGE: " << argv[0]
              << " <input.png> <array-name> <output.png>" << std::endl
              << std::endl
              << "Ingests `input.png` into a new array `my_array_name`, slices "
                 "and produces a new output image `output.png`."
              << std::endl;
    return 1;
  }

  std::string input_png(argv[1]), array_path(argv[2]), output_png(argv[3]);

  // Ingest the .png data to a new TileDB array.
  ingest_png(input_png, array_path);

  // Read a slice from the array and write it to a new .png image.
  slice_and_desaturate(array_path, output_png);

  return 0;
}
