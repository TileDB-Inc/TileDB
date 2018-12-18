/**
 * @file  info_command.h
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
 * This file declares the info command.
 */

#ifndef TILEDB_CLI_INFO_COMMAND_H
#define TILEDB_CLI_INFO_COMMAND_H

#include "commands/command.h"

#include "tiledb/sm/enums/datatype.h"

namespace tiledb {
namespace cli {

/**
 * Command that can display information about TileDB arrays.
 */
class InfoCommand : public Command {
 public:
  /** Get the CLI for this command instance. */
  clipp::group get_cli();

  /** Runs this info command. */
  void run();

 private:
  /** Types of information that can be displayed. */
  enum class InfoType { None, TileSizes, SVGMBRs, DumpMBRs, ArraySchema };

  /** Type of information to display. */
  InfoType type_ = InfoType::None;

  /** Array to print info for. */
  std::string array_uri_;

  /** Path to write any output. */
  std::string output_path_ = "";

  /** Width of output SVG. */
  unsigned svg_width_ = 600;

  /** Height of output SVG. */
  unsigned svg_height_ = 600;

  /** Prints information about the array's tile sizes. */
  void print_tile_sizes() const;

  /** Prints basic information about the array schema. */
  void print_schema_info() const;

  /** Dumps array MBRs to SVG. */
  void write_svg_mbrs() const;

  /** Dumps array MBRs to text. */
  void write_text_mbrs() const;

  /** Converts an opaque MBR to a 2D (double) rectangle. */
  std::tuple<double, double, double, double> get_mbr(
      const void* mbr, tiledb::sm::Datatype datatype) const;

  /** Converts an opaque MBR to a 2D (double) rectangle. */
  template <typename T>
  std::tuple<double, double, double, double> get_mbr(const void* mbr) const;

  /**
   * Converts an opaque MBR to a string vector. The vector contents are strings:
   * [dim0_min, dim0_max, dim1_min, dim1_max, ...]
   *
   * @param mbr MBR to convert
   * @param coords_type Datatype of MBR values
   * @param dim_num Number of dimensions in MBR
   * @return String vector of MBR.
   */
  std::vector<std::string> mbr_to_string(
      const void* mbr,
      tiledb::sm::Datatype coords_type,
      unsigned dim_num) const;
};

}  // namespace cli
}  // namespace tiledb

#endif