/**
 * @file   png_ingestion_webp.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
 * array using WebP filter
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
  for (unsigned y = 0; y < *height; y++) {
    auto row = (uint8_t*)(std::malloc(png_get_rowbytes(png, info)));
    row_pointers.push_back(row);
  }

  // Read the pixel data.
  png_read_image(png, row_pointers.data());
  fclose(fp);

  png_destroy_read_struct(&png, &info, NULL);
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
  png_destroy_write_struct(&png, &info);
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
  VFS vfs(ctx);
  if (vfs.is_dir(array_path)) {
    vfs.remove_dir(array_path);
  }
  Domain domain(ctx);
  // We use `width * 4` for X dimension to allow for RGBA (4) elements per-pixel
  domain
      .add_dimension(
          Dimension::create<unsigned>(ctx, "x", {{1, width * 4}}, width))
      .add_dimension(
          Dimension::create<unsigned>(ctx, "y", {{1, height}}, height));

  // To compress using webp we need RGBA in a single buffer
  ArraySchema schema(ctx, TILEDB_DENSE);
  Attribute rgba = Attribute::create<uint8_t>(ctx, "rgba");

  // Create WebP filter and set options
  Filter webp(ctx, TILEDB_FILTER_WEBP);
  auto fmt = TILEDB_WEBP_RGBA;
  webp.set_option(TILEDB_WEBP_INPUT_FORMAT, &fmt);
  float quality = 50.0f;
  webp.set_option(TILEDB_WEBP_QUALITY, &quality);

  // Add to FilterList and set attribute filters
  FilterList filterList(ctx);
  filterList.add_filter(webp);
  rgba.set_filter_list(filterList);

  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}}).set_domain(domain);
  schema.add_attribute(rgba);

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
  std::vector<uint8_t> rgba;
  for (unsigned y = 0; y < height; y++) {
    auto row = row_pointers[y];
    for (unsigned x = 0; x < width; x++) {
      auto rgba_temp = &row[4 * x];
      uint8_t r = rgba_temp[0], g = rgba_temp[1], b = rgba_temp[2],
              a = rgba_temp[3];
      rgba.push_back(r);
      rgba.push_back(g);
      rgba.push_back(b);
      rgba.push_back(a);
    }
  }

  // Clean up.
  for (unsigned y = 0; y < height; y++)
    std::free(row_pointers[y]);

  // Write the pixel data into the array.
  Context ctx;
  Array array(ctx, array_path, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("rgba", rgba);
  query.submit();
  query.finalize();
  array.close();
}

/**
 * Reads image data from a TileDB array using WebP filter
 * and writes a new image with the resulting image data.
 *
 * @param array_path Path of array to read from.
 * @param output_png Path of .png image to create.
 */
void read_png_array(
    const std::string& array_path, const std::string& output_png) {
  Context ctx;
  Array array(ctx, array_path, TILEDB_READ);

  // Get the array non-empty domain, which corresponds to the original image
  // width and height.
  auto non_empty = array.non_empty_domain<unsigned>();
  auto array_width = non_empty[0].second.second,
       array_height = non_empty[1].second.second;

  std::vector<unsigned> subarray = {1, array_width, 1, array_height};
  auto output_width = subarray[1], output_height = subarray[3];

  // Allocate query and set subarray
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR).set_subarray(subarray);

  // Allocate buffer to read into.
  std::vector<uint8_t> rgba(output_height * output_width);

  // Set buffer
  query.set_data_buffer("rgba", rgba);

  // Read from the array.
  query.submit();
  query.finalize();
  array.close();
  assert(Query::Status::COMPLETE == query.query_status());

  // Allocate a buffer suitable for passing to libpng.
  std::vector<uint8_t*> png_buffer;
  for (unsigned y = 0; y < output_height; y++)
    png_buffer.push_back((uint8_t*)std::malloc(output_width * sizeof(uint8_t)));

  for (unsigned y = 0; y < output_height; y++) {
    uint8_t* row = png_buffer[y];
    for (unsigned x = 0; x < output_width; x++) {
      row[x] = rgba[(y * output_width) + x];
    }
  }

  // Write the image.
  write_png(png_buffer, output_width / 4, output_height, output_png);

  // Clean up.
  for (unsigned i = 0; i < output_height; i++)
    std::free(png_buffer[i]);
}

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cout << "USAGE: " << argv[0]
              << " <input.png> <array-name> <output.png>" << std::endl
              << std::endl
              << "Ingests `input.png` into a new array `my_array_name` and "
                 "produces a new output image `output.png`."
              << std::endl;
    return 1;
  }

  std::string input_png(argv[1]), array_path(argv[2]), output_png(argv[3]);

  // Ingest the .png data to a new TileDB array.
  ingest_png(input_png, array_path);

  // Read from the array and write it to a new .png image.
  read_png_array(array_path, output_png);

  return 0;
}
