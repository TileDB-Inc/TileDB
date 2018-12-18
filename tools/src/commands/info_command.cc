/**
 * @file  info_command.cc
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
 * This file defines the info command.
 */

#include <fstream>
#include <sstream>

#include "commands/info_command.h"
#include "misc/common.h"

#include "tiledb/sm/storage_manager/storage_manager.h"

namespace tiledb {
namespace cli {

using namespace tiledb::sm;

clipp::group InfoCommand::get_cli() {
  using namespace clipp;
  auto array_arg =
      ((option("-a", "--array").required(true) & value("uri", array_uri_)) %
       "URI of TileDB array");

  auto schema_info =
      "array-schema: Prints basic information about the array's schema." %
      (command("array-schema").set(type_, InfoType::ArraySchema), array_arg);

  auto tile_sizes =
      "tile-sizes: Prints statistics about tile sizes in the array." %
      (command("tile-sizes").set(type_, InfoType::TileSizes), array_arg);

  auto svg_mbrs =
      "svg-mbrs: Produces an SVG visualizing the MBRs (2D arrays only)" %
      (command("svg-mbrs").set(type_, InfoType::SVGMBRs),
       array_arg,
       option("-o", "--output").doc("Path to write output SVG") &
           value("path", output_path_),
       option("-w", "--width").doc("Width of output SVG") &
           value("N", svg_width_),
       option("-h", "--height").doc("Height of output SVG") &
           value("N", svg_height_));

  auto dump_mbrs =
      "dump-mbrs: Dumps the MBRs in the array to text output." %
      (command("dump-mbrs").set(type_, InfoType::DumpMBRs),
       array_arg,
       option("-o", "--output").doc("Path to write output text file") &
           value("path", output_path_));

  auto cli = schema_info | tile_sizes | dump_mbrs | svg_mbrs;
  return cli;
}

void InfoCommand::run() {
  switch (type_) {
    case InfoType::None:
      break;
    case InfoType::TileSizes:
      print_tile_sizes();
      break;
    case InfoType::SVGMBRs:
      write_svg_mbrs();
      break;
    case InfoType::DumpMBRs:
      write_text_mbrs();
      break;
    case InfoType::ArraySchema:
      print_schema_info();
      break;
  }
}

void InfoCommand::print_tile_sizes() const {
  StorageManager sm;
  THROW_NOT_OK(sm.init(nullptr));

  // Open the array
  URI uri(array_uri_);
  Array array(uri, &sm);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  // Compute and report mean persisted tile sizes over all attributes.
  const auto* schema = array.array_schema();
  auto fragment_metadata = array.fragment_metadata();
  auto attributes = schema->attributes();
  uint64_t total_persisted_size = 0, total_in_memory_size = 0;

  // Helper function for processing each attribute.
  auto process_attr = [&](const std::string& name, bool var_size) {
    uint64_t persisted_tile_size = 0, in_memory_tile_size = 0;
    uint64_t num_tiles = 0;
    for (const auto& f : fragment_metadata) {
      uint64_t tile_num = f->tile_num();
      for (uint64_t tile_idx = 0; tile_idx < tile_num; tile_idx++) {
        persisted_tile_size += f->persisted_tile_size(name, tile_idx);
        in_memory_tile_size += f->tile_size(name, tile_idx);
        num_tiles++;
        if (var_size) {
          persisted_tile_size += f->persisted_tile_var_size(name, tile_idx);
          in_memory_tile_size += f->tile_var_size(name, tile_idx);
          num_tiles++;
        }
      }
    }
    total_persisted_size += persisted_tile_size;
    total_in_memory_size += in_memory_tile_size;

    std::cout << "- " << name << " (" << num_tiles << " tiles):" << std::endl;
    std::cout << "  Total persisted tile size: " << persisted_tile_size
              << " bytes." << std::endl;
    std::cout << "  Total in-memory tile size: " << in_memory_tile_size
              << " bytes." << std::endl;
  };

  // Print header
  std::cout << "Array URI: " << uri.to_string() << std::endl;
  std::cout << "Tile stats (per attribute):" << std::endl;

  // Dump info about coords for sparse arrays.
  if (!schema->dense())
    process_attr(constants::coords, false);

  // Dump info about the rest of the attributes
  for (const auto* attr : attributes)
    process_attr(attr->name(), attr->var_size());

  std::cout << "Sum of attribute persisted size: " << total_persisted_size
            << " bytes." << std::endl;
  std::cout << "Sum of attribute in-memory size: " << total_in_memory_size
            << " bytes." << std::endl;

  // Close the array.
  THROW_NOT_OK(array.close());
}

void InfoCommand::print_schema_info() const {
  StorageManager sm;
  THROW_NOT_OK(sm.init(nullptr));

  // Open the array
  URI uri(array_uri_);
  Array array(uri, &sm);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  array.array_schema()->dump(stdout);

  // Close the array.
  THROW_NOT_OK(array.close());
}

void InfoCommand::write_svg_mbrs() const {
  StorageManager sm;
  THROW_NOT_OK(sm.init(nullptr));

  // Open the array
  URI uri(array_uri_);
  Array array(uri, &sm);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  const auto* schema = array.array_schema();
  auto dim_num = schema->dim_num();
  if (dim_num < 2) {
    THROW_NOT_OK(array.close());
    throw std::runtime_error("SVG MBRs only supported for >1D arrays.");
  }

  std::vector<std::tuple<double, double, double, double>> mbr_rects;
  double min_x = std::numeric_limits<double>::max(),
         max_x = std::numeric_limits<double>::min(),
         min_y = std::numeric_limits<double>::max(),
         max_y = std::numeric_limits<double>::min();
  auto fragment_metadata = array.fragment_metadata();
  for (const auto& f : fragment_metadata) {
    auto mbrs = f->mbrs();
    for (void* mbr : mbrs) {
      auto tup = get_mbr(mbr, schema->coords_type());
      min_x = std::min(min_x, std::get<0>(tup));
      min_y = std::min(min_y, std::get<1>(tup));
      max_x = std::max(max_x, std::get<0>(tup) + std::get<2>(tup));
      max_y = std::max(max_y, std::get<1>(tup) + std::get<3>(tup));
      mbr_rects.push_back(tup);
    }
  }

  const double coord_width = max_x - min_x + 1;
  const double coord_height = max_y - min_y + 1;
  const double scale_x = svg_width_ / coord_width;
  const double scale_y = svg_height_ / coord_height;
  std::stringstream svg;
  svg << "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
      << "<svg version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\" "
         "xmlns:xlink=\"http://www.w3.org/1999/xlink\" width=\""
      << (svg_width_) << "px\" height=\"" << (svg_height_) << "px\" >\n";
  svg << "<g>\n";
  const uint16_t g_inc = std::max<uint16_t>(1, 0xff / mbr_rects.size());
  uint32_t r = 0, g = 0, b = 0xff;
  for (const auto& tup : mbr_rects) {
    double x = scale_x * (std::get<0>(tup) - min_x);
    double y = scale_y * (std::get<1>(tup) - min_y);
    double width = scale_x * std::get<2>(tup);
    double height = scale_y * std::get<3>(tup);
    svg << "  <rect x=\"" << x << "\" y=\"" << y << "\" width=\"" << width
        << "\" height=\"" << height << "\" "
        << "style=\"fill:rgb(" << r << ", " << g << ", " << b
        << ");stroke:none;fill-opacity:0.5\" "
           "/>\n";
    g = (g + g_inc) % 0xff;
  }
  svg << "</g>\n";
  svg << "</svg>";

  if (output_path_.empty()) {
    std::cout << svg.str() << std::endl;
  } else {
    std::ofstream os(output_path_, std::ios::out | std::ios::trunc);
    os << svg.str() << std::endl;
  }

  // Close the array.
  THROW_NOT_OK(array.close());
}

void InfoCommand::write_text_mbrs() const {
  StorageManager sm;
  THROW_NOT_OK(sm.init(nullptr));

  // Open the array
  URI uri(array_uri_);
  Array array(uri, &sm);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  const auto* schema = array.array_schema();
  auto dim_num = schema->dim_num();
  auto fragment_metadata = array.fragment_metadata();
  auto coords_type = schema->coords_type();
  std::stringstream text;
  for (const auto& f : fragment_metadata) {
    auto mbrs = f->mbrs();
    for (void* mbr : mbrs) {
      auto str_mbr = mbr_to_string(mbr, coords_type, dim_num);
      for (unsigned i = 0; i < dim_num; i++) {
        text << str_mbr[2 * i + 0] << "," << str_mbr[2 * i + 1];
        if (i < dim_num - 1)
          text << "\t";
      }
      text << std::endl;
    }
  }

  if (output_path_.empty()) {
    std::cout << text.str() << std::endl;
  } else {
    std::ofstream os(output_path_, std::ios::out | std::ios::trunc);
    os << text.str() << std::endl;
  }

  // Close the array.
  THROW_NOT_OK(array.close());
}

std::tuple<double, double, double, double> InfoCommand::get_mbr(
    const void* mbr, tiledb::sm::Datatype datatype) const {
  switch (datatype) {
    case Datatype::INT8:
      return get_mbr<int8_t>(mbr);
    case Datatype::UINT8:
      return get_mbr<uint8_t>(mbr);
    case Datatype::INT16:
      return get_mbr<int16_t>(mbr);
    case Datatype::UINT16:
      return get_mbr<uint16_t>(mbr);
    case Datatype::INT32:
      return get_mbr<int>(mbr);
    case Datatype::UINT32:
      return get_mbr<unsigned>(mbr);
    case Datatype::INT64:
      return get_mbr<int64_t>(mbr);
    case Datatype::UINT64:
      return get_mbr<uint64_t>(mbr);
    case Datatype::FLOAT32:
      return get_mbr<float>(mbr);
    case Datatype::FLOAT64:
      return get_mbr<double>(mbr);
    default:
      throw std::invalid_argument(
          "Cannot get MBR; Unsupported coordinates type");
  }
}

template <typename T>
std::tuple<double, double, double, double> InfoCommand::get_mbr(
    const void* mbr) const {
  T x, y, width, height;
  y = static_cast<const T*>(mbr)[0];
  height = static_cast<const T*>(mbr)[1] - y + 1;
  x = static_cast<const T*>(mbr)[2];
  width = static_cast<const T*>(mbr)[3] - x + 1;
  return std::make_tuple(x, y, width, height);
}

std::vector<std::string> InfoCommand::mbr_to_string(
    const void* mbr, Datatype coords_type, unsigned dim_num) const {
  std::vector<std::string> result;
  for (unsigned i = 0; i < dim_num; i++) {
    switch (coords_type) {
      case Datatype::INT8:
        result.push_back(
            std::to_string(static_cast<const uint8_t*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const uint8_t*>(mbr)[2 * i + 1]));
        break;
      case Datatype::UINT8:
        result.push_back(
            std::to_string(static_cast<const uint8_t*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const uint8_t*>(mbr)[2 * i + 1]));
        break;
      case Datatype::INT16:
        result.push_back(
            std::to_string(static_cast<const int16_t*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const int16_t*>(mbr)[2 * i + 1]));
        break;
      case Datatype::UINT16:
        result.push_back(
            std::to_string(static_cast<const uint16_t*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const uint16_t*>(mbr)[2 * i + 1]));
        break;
      case Datatype::INT32:
        result.push_back(
            std::to_string(static_cast<const int*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const int*>(mbr)[2 * i + 1]));
        break;
      case Datatype::UINT32:
        result.push_back(
            std::to_string(static_cast<const unsigned*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const unsigned*>(mbr)[2 * i + 1]));
        break;
      case Datatype::INT64:
        result.push_back(
            std::to_string(static_cast<const int64_t*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const int64_t*>(mbr)[2 * i + 1]));
        break;
      case Datatype::UINT64:
        result.push_back(
            std::to_string(static_cast<const uint64_t*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const uint64_t*>(mbr)[2 * i + 1]));
        break;
      case Datatype::FLOAT32:
        result.push_back(
            std::to_string(static_cast<const float*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const float*>(mbr)[2 * i + 1]));
        break;
      case Datatype::FLOAT64:
        result.push_back(
            std::to_string(static_cast<const double*>(mbr)[2 * i + 0]));
        result.push_back(
            std::to_string(static_cast<const double*>(mbr)[2 * i + 1]));
        break;
      default:
        throw std::invalid_argument(
            "Cannot get MBR; Unsupported coordinates type");
    }
  }

  return result;
}

// Explicit template instantiations
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<int8_t>(const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<uint8_t>(const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<int16_t>(const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<uint16_t>(const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<int32_t>(const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<uint32_t>(const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<int64_t>(const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<uint64_t>(const void* mbr) const;
template std::tuple<double, double, double, double> InfoCommand::get_mbr<float>(
    const void* mbr) const;
template std::tuple<double, double, double, double>
InfoCommand::get_mbr<double>(const void* mbr) const;

}  // namespace cli
}  // namespace tiledb