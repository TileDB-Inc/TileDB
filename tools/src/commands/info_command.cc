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
  auto tile_sizes =
      "tile-sizes: Prints statistics about tile sizes in the array." %
      (command("tile-sizes").set(type_, InfoType::TileSizes), array_arg);
  auto svg_mbrs =
      "svg-mbrs: Produces an SVG visualizing the MBRs (2D arrays only)" %
      (command("svg-mbrs").set(type_, InfoType::SVGMBRs),
       array_arg,
       option("-o", "--output").doc("Path to write output SVG") &
           value("path", svg_path_),
       option("-w", "--width").doc("Width of output SVG") &
           value("N", svg_width_),
       option("-h", "--height").doc("Height of output SVG") &
           value("N", svg_height_));
  auto cli = tile_sizes | svg_mbrs;
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
  std::cout << "Array URI: " << uri.to_string() << std::endl;
  std::cout << "Mean persisted tile sizes: " << std::endl;
  for (const auto& attr : attributes) {
    uint64_t persisted_tile_size = 0;
    uint64_t num_tiles = 0;
    for (const auto& f : fragment_metadata) {
      uint64_t tile_num = f->tile_num();
      num_tiles += tile_num;
      for (uint64_t tile_idx = 0; tile_idx < tile_num; tile_idx++) {
        persisted_tile_size += f->persisted_tile_size(attr->name(), tile_idx);
        if (attr->var_size())
          persisted_tile_size +=
              f->persisted_tile_var_size(attr->name(), tile_idx);
      }
    }

    double mean_persisted_tile_size = persisted_tile_size / double(num_tiles);
    std::cout << "  - " << attr->name() << ": " << mean_persisted_tile_size
              << " bytes." << std::endl;
  }

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

  if (svg_path_.empty()) {
    std::cout << svg.str() << std::endl;
  } else {
    std::ofstream os(svg_path_, std::ios::out | std::ios::trunc);
    os << svg.str() << std::endl;
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