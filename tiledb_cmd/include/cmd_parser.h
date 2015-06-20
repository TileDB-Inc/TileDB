/**
 * @file   cmd_parser.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines class CmdParser.
 */

#ifndef CMD_PARSER_H
#define CMD_PARSER_H

#include "array_schema.h"
#include <inttypes.h>

/** 
 * Objects of this class properly parse TileDB queries given from a command
 * line. 
 */
class CmdParser {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  CmdParser();
  /** Destructor. */
  ~CmdParser();

  // PARSING METHODS
  /** Parse command line for query 'clear_array'. */
  int parse_clear_array(
      int argc, char** argv, std::string& array_name, 
      std::string& workspace, std::string& err_msg) const;
  /** Parse command line for query 'define_array' and return the schema. */
  int parse_define_array(
      int argc, char** argv, const ArraySchema*& array_schema,
      std::string& workspace, std::string& err_msg) const;
  /** Parse command line for query 'delete_array'. */
  int parse_delete_array( 
      int argc, char** argv, std::string& array_name, 
      std::string& workspace, std::string& err_msg) const;
  /** Parse command line for query 'export_to_csv'. */
  int parse_export_to_csv(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& filename, std::vector<std::string>& dim_names,
      std::vector<std::string>& attribute_names,
      bool& reverse, std::string& err_msg) const;
  /** Parse command line for query 'generate_synthetic_data'. */
  int parse_generate_synthetic_data(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& filename, std::string& file_type, 
      unsigned& seed, std::string& distribution, int64_t& cell_num, 
      size_t& file_size, std::string& err_msg) const;
  /** Parse command line for query 'load_bin'. */
  int parse_load_bin(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& filename, std::string& err_msg) const;
  /** Parse command line for query 'load_csv'. */
  int parse_load_csv(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& filename, std::string& err_msg) const;
  /** Parse command line for query 'load_sorted_bin'. */
  int parse_load_sorted_bin(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& path, std::string& err_msg) const;
  /** Parse command line for query 'show_array_schema'. */
  int parse_show_array_schema(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& err_msg) const;
  /** Parse command line for query 'subarray'. */
  int parse_subarray(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& result_name, std::vector<double>& range,
      std::vector<std::string>& attribute_names,
      bool& reverse, std::string& err_msg) const;
  /** Parse command line for query 'update_csv'. */
  int parse_update_csv(
      int argc, char** argv, std::string& array_name, std::string& workspace, 
      std::string& filename, std::string& err_msg) const;

 private:
  // PRIVATE METHODS
  /** Retrieves the attribute names. */
  int get_attribute_names(
      const std::string& str_attribute_names, 
      std::vector<std::string>& attribute_names, std::string& err_msg) const;
  /** Gets the capacity. */ 
  int get_capacity(
      const std::string& str_capacity, 
      int64_t& capacity, std::string& err_msg) const;
  /** Gets the cell order. */
  int get_cell_order(
      const std::string& str_cell_order, ArraySchema::CellOrder& cell_order,
      std::string& err_msg) const;
  /** Gets the number of cells. */
  int get_cell_num(
      const std::string& str_cell_num, int64_t& cell_num,
      std::string& err_msg) const;
  /** Gets the consolidation step. */ 
  int get_consolidation_step(
      const std::string& str_consolidation_step,
      int& consolidation_step, std::string& err_msg) const;
  /** Gets the dimension domains. */ 
  int get_dim_domains(
      const std::string& str_dim_domains, int dim_num,
      std::vector<std::pair<double, double> >& dim_domains,
      std::string& err_msg) const;
  /** Gets the dimension names. */ 
  int get_dim_names(
      const std::string& str_dim_names,
      const std::vector<std::string>& attribute_names,
      std::vector<std::string>& dim_names,
      std::string& err_msg) const;
  /** Gets the file size. */
  int get_file_size(
      const std::string& str_file_size, size_t& file_size,
      std::string& err_msg) const;
  /** Gets the range. */
  int get_range(
      const std::string& str_range, std::vector<double>& range,
      std::string& err_msg) const;
  /** Gets the mode (reverse or not). */
  int get_reverse(
      const std::string& mode, bool& reverse, std::string& err_msg) const;
  /** Gets the seed. */
  int get_seed(
      const std::string& str_seed, unsigned& seed,
      std::string& err_msg) const;
  /** Gets the tile extents. */
  int get_tile_extents(
      const std::string& str_tile_extents, int dim_num,
      const std::vector<std::pair<double, double> >& dim_domains,
      std::vector<double>& tile_extents, std::string& err_msg) const;
  /** Gets the tile order. */
  int get_tile_order(
      const std::string& str_tile_order, ArraySchema::TileOrder& tile_order,
      std::string& err_msg) const;
  /** Gets the types and number of values per attribute. */
  int get_types(
      const std::string& str_types, int attribute_num,
      std::vector<const std::type_info*>& types, 
      std::vector<int>& val_num, std::string& err_msg) const;
};

#endif
