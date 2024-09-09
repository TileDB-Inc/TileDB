/**
 * @file  info_command.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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

#include "commands/info_command.h"
#include "misc/common.h"

#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"

#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>

namespace tiledb::cli {

using namespace tiledb::sm;

/** The thread pool for compute-bound tasks. */
ThreadPool compute_tp_{std::thread::hardware_concurrency()};

/** The thread pool for io-bound tasks. */
ThreadPool io_tp_{std::thread::hardware_concurrency()};

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
  Config config;
  auto logger = make_shared<Logger>(HERE(), "");
  ContextResources resources(config, logger, 1, 1, "");

  // Open the array
  URI uri(array_uri_);
  Array array(resources, uri);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));
  EncryptionKey enc_key;

  // Compute and report mean persisted tile sizes over all attributes.
  const auto& schema = array.array_schema_latest();
  auto fragment_metadata = array.fragment_metadata();
  auto& attributes = schema.attributes();
  uint64_t total_persisted_size = 0, total_in_memory_size = 0;

  // Helper function for processing each attribute.
  auto process_attr = [&](const std::string& name, bool var_size) {
    uint64_t persisted_tile_size = 0, in_memory_tile_size = 0;
    uint64_t num_tiles = 0;
    for (const auto& f : fragment_metadata) {
      uint64_t tile_num = f->tile_num();
      std::vector<std::string> names;
      names.push_back(name);
      f->loaded_metadata()->load_tile_offsets(enc_key, names);
      f->loaded_metadata()->load_tile_var_sizes(enc_key, name);
      for (uint64_t tile_idx = 0; tile_idx < tile_num; tile_idx++) {
        persisted_tile_size +=
            f->loaded_metadata()->persisted_tile_size(name, tile_idx);
        in_memory_tile_size += f->tile_size(name, tile_idx);
        num_tiles++;
        if (var_size) {
          persisted_tile_size +=
              f->loaded_metadata()->persisted_tile_var_size(name, tile_idx);
          in_memory_tile_size +=
              f->loaded_metadata()->tile_var_size(name, tile_idx);
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
  if (!schema.dense())
    process_attr(constants::coords, false);

  // Dump info about the rest of the attributes
  for (const auto& attr : attributes)
    process_attr(attr->name(), attr->var_size());

  std::cout << "Sum of attribute persisted size: " << total_persisted_size
            << " bytes." << std::endl;
  std::cout << "Sum of attribute in-memory size: " << total_in_memory_size
            << " bytes." << std::endl;

  // Close the array.
  THROW_NOT_OK(array.close());
}

void InfoCommand::print_schema_info() const {
  Config config;
  auto logger = make_shared<Logger>(HERE(), "");
  ContextResources resources(config, logger, 1, 1, "");

  // Open the array
  URI uri(array_uri_);
  Array array(resources, uri);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  std::cout << array.array_schema_latest() << std::endl;

  // Close the array.
  THROW_NOT_OK(array.close());
}

void InfoCommand::write_svg_mbrs() const {
  Config config;
  auto logger = make_shared<Logger>(HERE(), "");
  ContextResources resources(config, logger, 1, 1, "");

  // Open the array
  URI uri(array_uri_);
  Array array(resources, uri);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  const auto& schema = array.array_schema_latest();
  auto dim_num = schema.dim_num();
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
    const auto& mbrs = f->loaded_metadata()->mbrs();
    for (const auto& mbr : mbrs) {
      auto tup = get_mbr(mbr, schema.domain());
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
  const uint16_t g_inc = std::max<uint16_t>(
      1, static_cast<uint16_t>((size_t)0xff / mbr_rects.size()));
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
  Config config;
  auto logger = make_shared<Logger>(HERE(), "");
  ContextResources resources(config, logger, 1, 1, "");

  // Open the array
  URI uri(array_uri_);
  Array array(resources, uri);
  THROW_NOT_OK(
      array.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0));

  auto encryption_key = array.encryption_key();
  const auto& schema = array.array_schema_latest();
  auto dim_num = schema.dim_num();
  auto fragment_metadata = array.fragment_metadata();
  std::stringstream text;
  for (const auto& f : fragment_metadata) {
    f->loaded_metadata()->load_rtree(*encryption_key);
    const auto& mbrs = f->loaded_metadata()->mbrs();
    for (const auto& mbr : mbrs) {
      auto str_mbr = mbr_to_string(mbr, schema.domain());
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
    const NDRange& mbr, const tiledb::sm::Domain& domain) const {
  assert(domain.dim_num() == 2);
  double x, y, width, height;

  // First dimension
  auto d1_type = domain.dimension_ptr(0)->type();
  switch (d1_type) {
    case Datatype::INT8:
      y = static_cast<const int8_t*>(mbr[0].data())[0];
      height = static_cast<const int8_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::UINT8:
      y = static_cast<const uint8_t*>(mbr[0].data())[0];
      height = static_cast<const uint8_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::INT16:
      y = static_cast<const int16_t*>(mbr[0].data())[0];
      height = static_cast<const int16_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::UINT16:
      y = static_cast<const uint16_t*>(mbr[0].data())[0];
      height = static_cast<const uint16_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::INT32:
      y = static_cast<const int32_t*>(mbr[0].data())[0];
      height = static_cast<const int32_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::UINT32:
      y = static_cast<const uint32_t*>(mbr[0].data())[0];
      height = static_cast<const uint32_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::INT64:
      y = static_cast<const int64_t*>(mbr[0].data())[0];
      height = static_cast<const int64_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::UINT64:
      y = static_cast<const uint64_t*>(mbr[0].data())[0];
      height = static_cast<const uint64_t*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::FLOAT32:
      y = static_cast<const float*>(mbr[0].data())[0];
      height = static_cast<const float*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::FLOAT64:
      y = static_cast<const double*>(mbr[0].data())[0];
      height = static_cast<const double*>(mbr[0].data())[1] - y + 1;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      y = static_cast<const int64_t*>(mbr[0].data())[0];
      height = static_cast<const int64_t*>(mbr[0].data())[1] - y + 1;
      break;
    default:
      throw std::invalid_argument(
          "Cannot get MBR; Unsupported coordinates type");
  }

  // Second dimension
  auto d2_type = domain.dimension_ptr(1)->type();
  switch (d2_type) {
    case Datatype::INT8:
      x = static_cast<const int8_t*>(mbr[1].data())[0];
      width = static_cast<const int8_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::UINT8:
      x = static_cast<const uint8_t*>(mbr[1].data())[0];
      width = static_cast<const uint8_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::INT16:
      x = static_cast<const int16_t*>(mbr[1].data())[0];
      width = static_cast<const int16_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::UINT16:
      x = static_cast<const uint16_t*>(mbr[1].data())[0];
      width = static_cast<const uint16_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::INT32:
      x = static_cast<const int32_t*>(mbr[1].data())[0];
      width = static_cast<const int32_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::UINT32:
      x = static_cast<const uint32_t*>(mbr[1].data())[0];
      width = static_cast<const uint32_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::INT64:
      x = static_cast<const int64_t*>(mbr[1].data())[0];
      width = static_cast<const int64_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::UINT64:
      x = static_cast<const uint64_t*>(mbr[1].data())[0];
      width = static_cast<const uint64_t*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::FLOAT32:
      x = static_cast<const float*>(mbr[1].data())[0];
      width = static_cast<const float*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::FLOAT64:
      x = static_cast<const double*>(mbr[1].data())[0];
      width = static_cast<const double*>(mbr[1].data())[1] - x + 1;
      break;
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      x = static_cast<const int64_t*>(mbr[1].data())[0];
      width = static_cast<const int64_t*>(mbr[1].data())[1] - x + 1;
      break;
    default:
      throw std::invalid_argument(
          "Cannot get MBR; Unsupported coordinates type");
  }

  return std::make_tuple(x, y, width, height);
}

// Works only for fixed-sized coordinates
std::vector<std::string> InfoCommand::mbr_to_string(
    const NDRange& mbr, const tiledb::sm::Domain& domain) const {
  std::vector<std::string> result;
  const int8_t* r8;
  const uint8_t* ru8;
  const int16_t* r16;
  const uint16_t* ru16;
  const int32_t* r32;
  const uint32_t* ru32;
  const int64_t* r64;
  const uint64_t* ru64;
  const float* rf32;
  const double* rf64;
  auto dim_num = domain.dim_num();
  for (unsigned d = 0; d < dim_num; d++) {
    auto type = domain.dimension_ptr(d)->type();
    switch (type) {
      case sm::Datatype::STRING_ASCII:
        result.push_back(std::string(mbr[d].start_str()));
        result.push_back(std::string(mbr[d].end_str()));
        break;
      case Datatype::INT8:
        r8 = (const int8_t*)mbr[d].data();
        result.push_back(std::to_string(r8[0]));
        result.push_back(std::to_string(r8[1]));
        break;
      case Datatype::UINT8:
        ru8 = (const uint8_t*)mbr[d].data();
        result.push_back(std::to_string(ru8[0]));
        result.push_back(std::to_string(ru8[1]));
        break;
      case Datatype::INT16:
        r16 = (const int16_t*)mbr[d].data();
        result.push_back(std::to_string(r16[0]));
        result.push_back(std::to_string(r16[1]));
        break;
      case Datatype::UINT16:
        ru16 = (const uint16_t*)mbr[d].data();
        result.push_back(std::to_string(ru16[0]));
        result.push_back(std::to_string(ru16[1]));
        break;
      case Datatype::INT32:
        r32 = (const int32_t*)mbr[d].data();
        result.push_back(std::to_string(r32[0]));
        result.push_back(std::to_string(r32[1]));
        break;
      case Datatype::UINT32:
        ru32 = (const uint32_t*)mbr[d].data();
        result.push_back(std::to_string(ru32[0]));
        result.push_back(std::to_string(ru32[1]));
        break;
      case Datatype::INT64:
        r64 = (const int64_t*)mbr[d].data();
        result.push_back(std::to_string(r64[0]));
        result.push_back(std::to_string(r64[1]));
        break;
      case Datatype::UINT64:
        ru64 = (const uint64_t*)mbr[d].data();
        result.push_back(std::to_string(ru64[0]));
        result.push_back(std::to_string(ru64[1]));
        break;
      case Datatype::FLOAT32:
        rf32 = (const float*)mbr[d].data();
        result.push_back(std::to_string(rf32[0]));
        result.push_back(std::to_string(rf32[1]));
        break;
      case Datatype::FLOAT64:
        rf64 = (const double*)mbr[d].data();
        result.push_back(std::to_string(rf64[0]));
        result.push_back(std::to_string(rf64[1]));
        break;
      default:
        throw std::invalid_argument(
            "Cannot get MBR; Unsupported coordinates type");
    }
  }

  return result;
}

}  // namespace tiledb::cli
