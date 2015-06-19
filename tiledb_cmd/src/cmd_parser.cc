/**
 * @file   cmd_parser.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the CmdParser class.
 */

#include "cmd_parser.h"
#include "utils.h"
#include "csv_file.h"
#include "special_values.h"
#include <chrono>
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <algorithm>
#include <bitset>

/******************************************************
************** CONSTRUCTORS & DESTRUCTORS *************
******************************************************/

CmdParser::CmdParser() {
}

CmdParser::~CmdParser() {
}

/******************************************************
******************* PARSING METHODS *******************
******************************************************/

int CmdParser::parse_clear_array(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  err_msg = "";

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }

  return 0;
}

int CmdParser::parse_define_array(
    int argc, char** argv, const ArraySchema*& array_schema,
    std::string& workspace, std::string& err_msg) const {
  // Initialization
  workspace = "";
  err_msg = "";
  std::string str_attribute_names("");
  std::string array_name("");
  std::string str_capacity("");
  std::string str_dim_names("");
  std::string str_dim_domains("");
  std::string str_tile_extents("");
  std::string str_cell_order("");
  std::string str_tile_order("");
  std::string str_consolidation_step("");
  std::string str_types("");

  // Varibles to be used in the creation of the array schema
  std::vector<std::string> attribute_names;
  std::vector<std::string> dim_names;
  std::vector<std::pair<double, double> > dim_domains;
  std::vector<const std::type_info*> types;
  std::vector<int> val_num;
  ArraySchema::CellOrder cell_order;
  ArraySchema::TileOrder tile_order;
  int64_t capacity;
  int consolidation_step;
  std::vector<double> tile_extents;

  // Error handling
  int err;

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"attribute-names",1,0,'a'},
    {"array-name",1,0,'A'},
    {"capacity",1,0,'c'},
    {"dim-names",1,0,'d'},
    {"dim-domains",1,0,'D'},
    {"tile-extents",1,0,'e'},
    {"cell-order",1,0,'o'},
    {"tile-order",1,0,'O'},
    {"consolidation-step",1,0,'s'},
    {"types",1,0,'t'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "a:A:c:d:D:e:o:O:s:t:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'a':
        if(str_attribute_names != "") {
          err_msg = "More than one attribute name lists provided.";
          return -1;
        }
        str_attribute_names = optarg;
        break;
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'c':
        if(str_capacity != "") {
          err_msg = "More than one capacities provided.";
          return -1;
        }
        str_capacity = optarg;
        break;
      case 'd':
        if(str_dim_names != "") {
          err_msg = "More than one dimension name lists provided.";
          return -1;
        }
        str_dim_names = optarg;
        break;
      case 'D':
        if(str_dim_domains != "") {
          err_msg = "More than one dimension domain lists provided.";
          return -1;
        }
        str_dim_domains = optarg;
        break;
      case 'e':
        if(str_tile_extents != "") {
          err_msg = "More than one tile extent lists provided.";
          return -1;
        }
        str_tile_extents = optarg;
        break;
      case 'o':
        if(str_cell_order != "") {
          err_msg = "More than one cell orders provided.";
          return -1;
        }
        str_cell_order = optarg;
        break;
      case 'O':
        if(str_tile_order != "") {
          err_msg = "More than one tile orders provided.";
          return -1;
        }
        str_tile_order = optarg;
        break;
      case 's':
        if(str_consolidation_step != "") {
          err_msg = "More than one consolidation steps provided.";
          return -1;
        }
        str_consolidation_step = optarg;
        break;
      case 't':
        if(str_types != "") {
          err_msg = "More than one type lists provided.";
          return -1;
        }
        str_types = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(str_attribute_names == "") {
    err_msg = "Attribute names not provided.";
    return -1;
  }
  if(str_dim_names == "") {
    err_msg = "Dimension names not provided.";
    return -1;
  }
  if(str_dim_domains == "") {
    err_msg = "Dimension domains not provided.";
    return -1;
  }
  if(str_types == "") {
    err_msg = "Types not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  // Check array name
  if(!is_valid_name(array_name.c_str())) {
    err_msg = std::string("'") + array_name + "' is not a valid array name."
              " The array name can contain only alphanumerics and '_'.";
    return -1;
  }
  // Get attribute names
  err = get_attribute_names(str_attribute_names, attribute_names, err_msg);
  if(err == -1) 
    return -1;
  // Check dimension names
  err = get_dim_names(str_dim_names, attribute_names, dim_names, err_msg);
  if(err == -1) 
    return -1;
  // Get dimension domains
  err = get_dim_domains(str_dim_domains, dim_names.size(), 
                        dim_domains, err_msg);
  if(err == -1) 
    return -1;
  // Get types and val num
  err = get_types(str_types, attribute_names.size(), types, val_num, err_msg);
  if(err == -1) 
    return -1;
  // Get cell order
  err = get_cell_order(str_cell_order, cell_order, err_msg);
  if(err == -1) 
    return -1;
  // Get tile order
  err = get_tile_order(str_tile_order, tile_order, err_msg);
  if(err == -1) 
    return -1;
  // Get capacity
  err = get_capacity(str_capacity, capacity, err_msg);
  if(err == -1) 
    return -1;
  // Get consolidation step
  err = get_consolidation_step(str_consolidation_step, consolidation_step,
                               err_msg);
  if(err == -1) 
    return -1;
  // Get tile extents
  err = get_tile_extents(str_tile_extents, dim_names.size(), dim_domains,
                         tile_extents, err_msg); 
  if(err == -1) 
    return -1;

  // The capacity cannot be set for regular tiles
  if(str_tile_extents != "" && str_capacity != "") {
    err_msg = "Capacity is meaningless in the case of regular tiles.";
    return -1;
  }
  // Tile order must not be given if the tiles are irregular
  if(str_tile_extents == "" && str_tile_order != "") {
    err_msg = "Tile order is meaningless in the case of irregular tiles.";
    return -1;
  }

  // Assign default values
  if(consolidation_step == -1) 
    consolidation_step = AS_CONSOLIDATION_STEP;
  if(capacity == -1) 
    capacity = AS_CAPACITY;
  if(cell_order == ArraySchema::CO_NONE) 
    cell_order = AS_CELL_ORDER;
  if(tile_order == ArraySchema::TO_NONE)
    tile_order = AS_TILE_ORDER;

  // Create array schema
  if(tile_extents.size() == 0) { // Irregular tiles
    array_schema = new ArraySchema(
        array_name, attribute_names, dim_names, 
        dim_domains, types, val_num, 
        cell_order, capacity, consolidation_step);
  } else {                       // Regular tiles 
    array_schema = new ArraySchema(
        array_name, attribute_names, dim_names,
        dim_domains, types, val_num, tile_extents,
        cell_order, tile_order, consolidation_step);
  }

  return 0;
}

int CmdParser::parse_delete_array(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  err_msg = "";

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }

  return 0;
}

int CmdParser::parse_export_to_csv(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& filename,
    std::vector<std::string>& dim_names,
    std::vector<std::string>& attribute_names,
    bool& reverse, std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  filename = "";
  err_msg = "";
  std::string str_attribute_names("");
  std::string str_dim_names("");
  std::string mode("");

  // Error handling
  int err;

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"attribute-names",1,0,'a'},
    {"array-name",1,0,'A'},
    {"dim-names",1,0,'d'},
    {"filename",1,0,'f'},
    {"mode",1,0,'m'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "a:A:d:f:m:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'a':
        if(str_attribute_names != "") {
          err_msg = "More than one attribute name lists provided.";
          return -1;
        }
        str_attribute_names = optarg;
        break;
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'd':
        if(str_dim_names != "") {
          err_msg = "More than one dimension name lists provided.";
          return -1;
        }
        str_dim_names = optarg;
        break;
      case 'f':
        if(filename != "") {
          err_msg = "More than one file names provided.";
          return -1;
        }
        filename = optarg;
        break;
      case 'm':
        if(mode != "") {
          err_msg = "More than one modes provided.";
          return -1;
        }
        mode = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  if(filename == "") {
    err_msg = "File name not provided.";
    return -1;
  }

  // Get attribute names
  CSVLine csv_line(str_attribute_names);
  attribute_names = csv_line.values();
  // Get dimension names
  csv_line = str_dim_names;
  dim_names = csv_line.values();
  // Get mode
  err = get_reverse(mode, reverse, err_msg);
  if(err == -1) 
    return -1;

  return 0;
}

int CmdParser::parse_generate_synthetic_data(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& filename,
    std::string& file_type, unsigned& seed,
    std::string& distribution, int64_t& cell_num,
    size_t& file_size, std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  filename = "";
  err_msg = "";
  distribution = "";
  file_type = "";
  std::string str_file_types("");
  std::string str_cell_num("");
  std::string str_seed("");
  std::string str_file_size("");

  // Error handling
  int err;

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"distribution",1,0,'d'},
    {"filename",1,0,'f'},
    {"cell-number",1,0,'n'},
    {"seed",1,0,'s'},
    {"file-size",1,0,'S'},
    {"file-type",1,0,'t'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:d:f:n:s:S:t:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'd':
        if(distribution != "") {
          err_msg = "More than one distributions provided.";
          return -1;
        }
        distribution = optarg;
        break;
      case 'f':
        if(filename != "") {
          err_msg = "More than one file names provided.";
          return -1;
        }
        filename = optarg;
        break;
      case 'n':
        if(str_cell_num != "") {
          err_msg = "More than one cell numbers provided.";
          return -1;
        }
        str_cell_num = optarg;
        break;
      case 's':
        if(str_seed != "") {
          err_msg = "More than one seeds provided.";
          return -1;
        }
        str_seed = optarg;
        break;
      case 'S':
        if(str_file_size != "") {
          err_msg = "More than one file sizes provided.";
          return -1;
        }
        str_file_size = optarg;
        break;
      case 't':
        if(file_type != "") {
          err_msg = "More than one file types provided.";
          return -1;
        }
        file_type = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  if(filename == "") {
    err_msg = "File name not provided.";
    return -1;
  }

  // Default file type
  if(file_type == "") 
    file_type = "csv";
  // Check file type
  if(file_type != "csv" && file_type != "sorted_csv" &&
     file_type != "bin" && file_type != "sorted_bin") {
    err_msg = std::string("Unknown file type '") + file_type + "'.";
    return -1;
  }

  // Default distribution
  if(distribution == "") 
    distribution = "uniform";
    // Check distribution
  if(distribution != "uniform") {
    err_msg = std::string("Unknown distribution '") + distribution + "'.";
    return -1;
  }

  // Get number of cells
  err = get_cell_num(str_cell_num, cell_num, err_msg); 
  if(err == -1)
    return -1;
  // Get file size
  err = get_file_size(str_file_size, file_size, err_msg); 
  if(err == -1)
    return -1;
  // Get seed
  err = get_seed(str_seed, seed, err_msg); 
  if(err == -1)
    return -1;

  // Either the number of cells or the file size must be given
  if(str_cell_num == "" && str_file_size == "") {
    err_msg = "Either the number of cells of the file size must be given.";
    return -1;
  }  

  return 0;
}

int CmdParser::parse_load_csv(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& filename,
    std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  filename = "";
  err_msg = "";

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"filename",1,0,'f'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:f:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'f':
        if(filename != "") {
          err_msg = "More than one file names provided.";
          return -1;
        }
        filename = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  if(filename == "") {
    err_msg = "File name not provided.";
    return -1;
  }

  // Check if file exists
  if(!is_file(filename)) {
    err_msg = std::string("File '") + filename + "' does not exist.";
    return -1;
  }

  return 0;
}

int CmdParser::parse_load_sorted_bin(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& dirname,
    std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  dirname = "";
  err_msg = "";

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"directory",1,0,'d'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:d:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'd':
        if(dirname != "") {
          err_msg = "More than one directories provided.";
          return -1;
        }
        dirname = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  if(dirname == "") {
    err_msg = "Directory not provided.";
    return -1;
  }

  // Check if directory exists
  if(!is_dir(dirname)) {
    err_msg = std::string("Directory '") + dirname + "' does not exist.";
    return -1;
  }

  return 0;
}

int CmdParser::parse_show_array_schema(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  err_msg = "";

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch.";
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  return 0;
}

int CmdParser::parse_subarray(
    int argc, char** argv, std::string& array_name, std::string& workspace,
    std::string& result_name, std::vector<double>& range,
    std::vector<std::string>& attribute_names,
    bool& reverse, std::string& err_msg) const {
   // Initialization
  array_name = "";
  workspace = "";
  result_name = "";
  range.clear();
  attribute_names.clear();
  err_msg = "";
  std::string str_attribute_names("");
  std::string mode("");
  std::string str_range("");

  // Error handling
  int err;

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"attribute-names",1,0,'a'},
    {"array-name",1,0,'A'},
    {"mode",1,0,'m'},
    {"range",1,0,'r'},
    {"result_name",1,0,'R'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "a:A:m:r:R:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'a':
        if(str_attribute_names != "") {
          err_msg = "More than one attribute name lists provided.";
          return -1;
        }
        str_attribute_names = optarg;
        break;
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'm':
        if(mode != "") {
          err_msg = "More than one modes provided.";
          return -1;
        }
        mode = optarg;
        break;
      case 'r':
        if(str_range != "") {
          err_msg = "More than one ranges provided.";
          return -1;
        }
        str_range = optarg;
        break;
      case 'R':
        if(result_name != "") {
          err_msg = "More than one result names provided.";
          return -1;
        }
        result_name = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  if(result_name == "") {
    err_msg = "Result name not provided.";
    return -1;
  }
  if(str_range == "") {
    err_msg = "Range not provided.";
    return -1;
  }

  // Get mode
  err = get_reverse(mode, reverse, err_msg);
  if(err == -1) 
    return -1;
  // Get attribute names
  CSVLine csv_line(str_attribute_names);
  attribute_names = csv_line.values();
  if(duplicates(attribute_names)) {
    err_msg = "Duplicate attribute names provided.";
    return -1;
  }
  // Get range
  err = get_range(str_range, range, err_msg); 
  if(err == -1) 
    return -1;

  return 0;    
}

int CmdParser::parse_update_csv(
    int argc, char** argv, std::string& array_name,
    std::string& workspace, std::string& filename,
    std::string& err_msg) const {
  // Initialization
  array_name = "";
  workspace = "";
  filename = "";
  err_msg = "";

  // --- Parse command line --- //
  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"filename",1,0,'f'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:f:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          err_msg = "More than one array names provided.";
          return -1;
        }
        array_name = optarg;
        break;
      case 'f':
        if(filename != "") {
          err_msg = "More than one file names provided.";
          return -1;
        }
        filename = optarg;
        break;
      case 'w':
        if(workspace != "") {
          err_msg = "More than one workspaces provided.";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        err_msg = "Invalid option.";
        return -1;
    }
  }

  // --- Check correctness --- //
  // Check number of arguments
  if((argc-2) != 2*option_num) {
    err_msg = "Arguments-options mismatch."; 
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    err_msg = "Array name not provided.";
    return -1;
  }
  if(workspace == "") {
    err_msg = "Workspace not provided.";
    return -1;
  }
  if(filename == "") {
    err_msg = "File name not provided.";
    return -1;
  }

  // Check if file exists
  if(!is_file(filename)) {
    err_msg = std::string("File '") + filename + "' does not exist.";
    return -1;
  }

  return 0;
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

int CmdParser::get_attribute_names(
    const std::string& str_attribute_names,
    std::vector<std::string>& attribute_names, std::string& err_msg) const {
  // Initialization
  attribute_names.clear();
  err_msg = "";

  // Create a CSV line with the attribute names
  CSVLine csv_line(str_attribute_names);

  // Check attribute name validity
  std::string attribute_name;
  while(csv_line >> attribute_name) {
    if(!is_valid_name(attribute_name.c_str())) {
      err_msg = std::string("'") + attribute_name + 
                "' is not a valid attribute name."
                " An attribute name can contain only alphanumerics and '_'.";
      return -1;
    }
    attribute_names.push_back(attribute_name);
  }

  // Check for duplicate attribute names
  if(duplicates(attribute_names)) {
    err_msg = "Duplicate attribute names provided.";
    return -1;
  }

  return 0;
}

int CmdParser::get_capacity(
    const std::string& str_capacity,
    int64_t& capacity, std::string& err_msg) const {
  // Capacity not provided, do nothing
  if(str_capacity == "") { 
    capacity = -1;
    return 0;
  }

  // Capacity must be integer
  if(!is_non_negative_integer(str_capacity.c_str())) {
    err_msg = "The capacity must be a positive integer."; 
    return -1;
  }

  sscanf(str_capacity.c_str(), "%lld", &capacity); 

  // Capacity cannot be zero
  if(capacity == 0) {
    err_msg = "The capacity cannot be zero."; 
    return -1;
  }
  
  return 0;
}

int CmdParser::get_cell_order(
    const std::string& str_cell_order,
    ArraySchema::CellOrder& cell_order, std::string& err_msg) const {
  // Initialization
  err_msg = "";

  // Retrieve cell order
  if(str_cell_order == "") {
    cell_order = ArraySchema::CO_NONE;
  } else if(str_cell_order == "row-major") {
    cell_order = ArraySchema::CO_ROW_MAJOR;
  } else if(str_cell_order == "column-major") {
    cell_order = ArraySchema::CO_COLUMN_MAJOR;
  } else if(str_cell_order == "hilbert") {
    cell_order = ArraySchema::CO_HILBERT;
  } else {
    err_msg = std::string("Unknown cell order '(") + str_cell_order + "'.";
    return -1;
  }

  return 0;
}

int CmdParser::get_cell_num(
    const std::string& str_cell_num, int64_t& cell_num,
    std::string& err_msg) const {
  // Number of cells not provided, do nothing
  if(str_cell_num == "") {
    cell_num = -1;
    return 0;
  }

  // Number of cells must be integer
  if(!is_non_negative_integer(str_cell_num.c_str())) {
    err_msg = "The number of cells must be a positive integer.";  
    return -1;
  }

  sscanf(str_cell_num.c_str(), "%lld", &cell_num); 

  // Number of cells must be positive
  if(cell_num == 0) {
    err_msg = "The number of cells cannot be zero."; 
    return -1;
  }

  return 0;
}

int CmdParser::get_consolidation_step(
    const std::string& str_consolidation_step,
    int& consolidation_step, std::string& err_msg) const {
  // Consolidation step not provided, do nothing
  if(str_consolidation_step == "") {
    consolidation_step = -1;
    return 0;
  }

  // Consolidation step must be integer
  if(!is_non_negative_integer(str_consolidation_step.c_str())) {
    err_msg = "The consolidation step must be a positive integer.";  
    return -1;
  }

  sscanf(str_consolidation_step.c_str(), "%d", &consolidation_step); 

  // Consolidation step must be positive
  if(consolidation_step == 0) {
    err_msg = "The consolidation step cannot be zero."; 
    return -1;
  }

  return 0;
}

int CmdParser::get_dim_domains(
   const std::string& str_dim_domains, int dim_num,
   std::vector<std::pair<double, double> >& dim_domains,
   std::string& err_msg) const {
  // Initialization
  dim_domains.clear();
  err_msg = "";

  // Create a CSV line with all the dimension domains
  CSVLine csv_line(str_dim_domains);

  // Check the number of domain bounds
  if(csv_line.values().size() != 2*dim_num) { 
    err_msg = "The number of domain bounds does not agree with the number of " 
              "dimensions. There should be a lower and an upper bound "
              "per dimension.";
    return -1;
  }

  // Check if the domain bounds are real numbers, and if the lower bound
  // is smaller than or equal to the upper.
  double lower, upper; 
  std::string str_lower, str_upper;
  for(int i=0; i<dim_num; ++i) {
    csv_line >> str_lower;
    csv_line >> str_upper;
    if(!is_real(str_lower.c_str()) || !is_real(str_upper.c_str())) {
      err_msg = "The domain bounds must be real numbers.";
      return -1;
    } else {
      sscanf(str_lower.c_str(), "%lf", &lower);
      sscanf(str_upper.c_str(), "%lf", &upper);
      if(lower > upper) {
        err_msg = "A lower domain bound cannot be larger than its "
                  "corresponding upper.";
        return -1;
      }
      dim_domains.push_back(std::pair<double, double>(lower, upper));
    }
  }

  return 0;
}

int CmdParser::get_dim_names(
    const std::string& str_dim_names,
    const std::vector<std::string>& attribute_names,
    std::vector<std::string>& dim_names,
    std::string& err_msg) const {
  // Initialization
  dim_names.clear();
  err_msg = "";

  // Create a CSV line with all dimension names
  CSVLine csv_line(str_dim_names);

  // Check dimension name validity
  std::string dim_name;
  while(csv_line >> dim_name) {
    if(!is_valid_name(dim_name.c_str())) {
      err_msg = std::string("'") + dim_name + 
                "' is not a valid dimension name."
                " A dimension name can contain only alphanumerics and '_'.";
      return -1;
    }
    dim_names.push_back(dim_name);
  }

  // Check for duplicate dimension names
  std::set<std::string> set_dim_names(dim_names.begin(), 
                                      dim_names.end());
  if(set_dim_names.size() != dim_names.size()) {
    err_msg = "Duplicate dimension names are not allowed.";
    return -1;
  }

  // Check if any dimension name is the same as an attribute name
  std::set<std::string> set_attribute_names(attribute_names.begin(), 
                                            attribute_names.end());
  std::vector<std::string> intersect; 
  std::set_intersection(set_attribute_names.begin(), 
                        set_attribute_names.end(),
                        set_dim_names.begin(),
                        set_dim_names.end(),
                        std::back_inserter(intersect));

  if(intersect.size()) {
    err_msg = std::string("A dimension cannot have the same name ") + 
              "as an attribute (e.g., '" + intersect[0] + "').";
    return -1;
  }

  return 0;
}

int CmdParser::get_file_size(
    const std::string& str_file_size, size_t& file_size,
    std::string& err_msg) const {
  // File size not provided, do nothing
  if(str_file_size == "") {
    file_size = 0;
    return 0;
  }

  // File size must be integer
  if(!is_non_negative_integer(str_file_size.c_str())) {
    err_msg = "The file size must be a positive integer.";  
    return -1;
  }

  sscanf(str_file_size.c_str(), "%zu", &file_size); 

  // Number of cells must be positive
  if(file_size == 0) {
    err_msg = "The file size cannot be zero."; 
    return -1;
  }

  return 0;
}

int CmdParser::get_range(
    const std::string& str_range, std::vector<double>& range,
    std::string& err_msg) const {
  // Create a CSV line with all the range bounds
  CSVLine csv_line(str_range); 

  // Check the number of domain bounds
  if(csv_line.values().size() % 2 != 0) { 
    err_msg = "The number of range bounds must be even.";
    return -1;
  }

  // Check if the range bounds are real numbers, and if the lower is bound
  // is smaller than or equal to the upper.
  int dim_num = csv_line.values().size()/2;
  range.reserve(2*dim_num);
  double lower, upper; 
  std::string str_lower, str_upper;
  for(int i=0; i<dim_num; ++i) {
    csv_line >> str_lower;
    csv_line >> str_upper;
    if(!is_real(str_lower.c_str()) || !is_real(str_upper.c_str())) {
      err_msg = "The range bounds must be real numbers.";
      return -1;
    } else {
      sscanf(str_lower.c_str(), "%lf", &lower);
      sscanf(str_upper.c_str(), "%lf", &upper);
      if(lower > upper) {
        err_msg = "A lower range bound cannot be larger than its "
                  "corresponding upper.";
        return -1;
      }
      range.push_back(lower);
      range.push_back(upper);
    }
  }

  return 0;
}

int CmdParser::get_reverse(
    const std::string& mode, bool& reverse, std::string& err_msg) const {
  // By default, the mode is "normal"
  if(mode == "" || mode == "normal") { 
    reverse = false;
  } else if(mode == "reverse") {
    reverse = true;
  } else { // Error
    err_msg = std::string("Unknown mode '")+ mode + "'.";
    return -1;
  }

  return 0;
}

int CmdParser::get_seed(
    const std::string& str_seed, unsigned& seed,
    std::string& err_msg) const {
  // Seed not provided, get default seed
  if(str_seed == "") {
    seed = std::chrono::system_clock::now().time_since_epoch().count();
    return 0;
  }

  // Seed must be a non-negative integer
  if(!is_non_negative_integer(str_seed.c_str())) {
    err_msg = "The seed must be a non-negative integer.";  
    return -1;
  }

  sscanf(str_seed.c_str(), "%u", &seed); 

  return 0;
}

int CmdParser::get_tile_extents(
    const std::string& str_tile_extents, int dim_num,
    const std::vector<std::pair<double, double> >& dim_domains,
    std::vector<double>& tile_extents, std::string& err_msg) const {
  // Intialization
  tile_extents.clear();
  err_msg = "";

  // No extents = irregular tiles
  if(str_tile_extents == "")
    return 0;

  // Create a CSV line with all the tile extents
  CSVLine csv_line(str_tile_extents); 
   
  // Check if the number of tile extents is the same as the number of dimensions
  if(csv_line.values().size() != dim_num) {
    err_msg = "The number of tile extents must be the same as the number "
              "of dimensions.";
    return -1;
  }

  // Check if the tile extents are real numbers
  double tile_extent;
  std::string str_tile_extent;
  while(csv_line >> str_tile_extent) {
    if(!is_real(str_tile_extent.c_str())) {
      err_msg = "The tile extents must be real numbers.";
      return -1;
    }
    sscanf(str_tile_extent.c_str(), "%lf", &tile_extent); 
    tile_extents.push_back(tile_extent);
  }

  // Check if each tile extent is smaller than the corresponding domain range
  for(int i=0; i<tile_extents.size(); ++i) {
    if(tile_extents[i] > dim_domains[i].second - dim_domains[i].first + 1) {
      err_msg = "The tile extents must not exceed their corresponding "
                "domain ranges.";
      return-1;
    }
  }

  return 0;
}

int CmdParser::get_tile_order(
    const std::string& str_tile_order,
    ArraySchema::TileOrder& tile_order, std::string& err_msg) const {
  // Initialization
  err_msg = "";

  // Retrieve tile order
  if(str_tile_order == "") {
    tile_order = ArraySchema::TO_NONE;
  } else if(str_tile_order == "row-major") {
    tile_order = ArraySchema::TO_ROW_MAJOR;
  } else if(str_tile_order == "column-major") {
    tile_order = ArraySchema::TO_COLUMN_MAJOR;
  } else if(str_tile_order == "hilbert") {
    tile_order = ArraySchema::TO_HILBERT;
  } else {
    err_msg = std::string("Unknown tile order '(") + str_tile_order + "'.";
    return -1;
  }

  return 0;
}

int CmdParser::get_types(
  const std::string& str_types, int attribute_num,
  std::vector<const std::type_info*>& types,
  std::vector<int>& val_num, std::string& err_msg) const {
  // Initialization
  types.clear();
  val_num.clear();
  err_msg = "";

  // Create a CSV line with all the types
  CSVLine csv_line(str_types); 

  // Check number of types
  if(csv_line.values().size() != attribute_num + 1) {
    err_msg = "The number of types should be equal to the number of "
              "attributes plus 1 (the last corresponds to the coordinates).";
    return -1;
  }

  // Check soundness of attribute types
  std::string type_val_num;  // Example format: 'int' or 'int:4'
  std::string type;
  int num;
  for(int i=0; i<csv_line.values().size()-1; ++i) {
    csv_line >> type_val_num;
    char* token = strtok(&type_val_num[0], ":"); // Type extraction
    type = token;    
    token = strtok(NULL, ":"); // Number of attribute values per cell extraction
    if(token == NULL) { // Missing number of attribute values per cell
      val_num.push_back(1);
    } else {
      // Process next token
      if(!strcmp(token, "var")) { // Variable-sized cell
        val_num.push_back(VAR_SIZE);
      } else if(!is_non_negative_integer(token)) { 
        // Invalid number of attribute values
        err_msg = "The number of attribute values per cell must be a "
                  "positive integer.";
        return -1;
      } else { // Valid number of attribute values per cell
        sscanf(token, "%d", &num);
        if(num == 0) {
          err_msg = "The number of attribute values per cell cannot be zero.";
          return -1;
        }
        val_num.push_back(num);
      }

      // No other token should follow
      token = strtok(NULL, ":");
      if(token != NULL) {
        err_msg = "Redundant arguments in definition of cell type.";
        return -1;
      }
    }
    
    if(type == "char") {
      types.push_back(&typeid(char));
    } else if(type == "int") {
      types.push_back(&typeid(int));
    } else if(type == "int64_t") {
      types.push_back(&typeid(int64_t));
    } else if(type == "float") {
      types.push_back(&typeid(float));
    } else if(type == "double") {
      types.push_back(&typeid(double));
    } else {
      err_msg = std::string("Invalid attribute type '") + type + "'.";
      return -1;
    }
  } 

  // Check specifically the coordinate type (it cannot be char)
  csv_line >> type;
  if(type == "int") {
    types.push_back(&typeid(int));
  } else if(type == "int64_t") {
    types.push_back(&typeid(int64_t));
  } else if(type == "float") {
    types.push_back(&typeid(float));
  } else if(type == "double") {
    types.push_back(&typeid(double));
  } else {
    err_msg = std::string("Invalid coordinates type '") + type + "'.";
    return -1;
  }
 
  return 0;
}

