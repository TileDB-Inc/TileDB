/**
 * @file   tiledb_define_array.cc
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
 * Implements command "tiledb_define_array".
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
#include <getopt.h>
#include <iostream>
#include <string>

/** 
 * Parses the command options. It sets the workspace, as well as the array
 * serialized in a CSV string. The format of the final string (stored
 * in array_schema_str) is the following (single line, no '\n' characters 
 * involved):
 * 
 * array_name,attribute_num,attribute_1,...,attribute_{attribute_num},
 * dim_num,dim_1,...,dim_{dim_num},
 * dim_domain_1_low,dim_domain_1_high,...,
 * dim_domain_{dim_num}_low,dim_domain_{dim_num}_high
 * type_1,...,type_{attribute_num+1}
 * tile_extents_1,...,tile_extents_{dim_num},
 * cell_order,tile_order,capacity,consolidation_step
 *
 * If one of the items is omitted (e.g., tile_order), then this CSV field will
 * contain character '*' (e.g., it should be ...,cell_order,*,capacity,...). 
 * 
 * It returns 0 on success. On error, it prints a message on stderr and
 * returns -1. 
 */                                                            
int parse_options(
    int argc, 
    char** argv, 
    std::string& array_schema_str,
    std::string& workspace) {

  // Initialization
  workspace = "";
  std::string array_name("");
  std::string attribute_names_str("");
  std::string dim_names_str("");
  std::string dim_domains_str("");
  std::string types_str("");
  std::string tile_extents_str("");
  std::string cell_order_str("");
  std::string tile_order_str("");
  std::string capacity_str("");
  std::string consolidation_step_str("");

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
        if(attribute_names_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    <<  "More than one attribute name lists provided.\n";
          return -1;
        }
        attribute_names_str = optarg;
        break;
      case 'A':
        if(array_name != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one array names provided.\n";
          return -1;
        }
        array_name = optarg;
        break;
      case 'c':
        if(capacity_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one capacities provided.\n";
          return -1;
        }
        capacity_str = optarg;
        break;
      case 'd':
        if(dim_names_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one dimension name lists provided.\n";
          return -1;
        }
        dim_names_str = optarg;
        break;
      case 'D':
        if(dim_domains_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one dimension domain lists provided.\n";
          return -1;
        }
        dim_domains_str = optarg;
        break;
      case 'e':
        if(tile_extents_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one tile extent lists provided.\n";
          return -1;
        }
        tile_extents_str = optarg;
        break;
      case 'o':
        if(cell_order_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one cell orders provided.\n";
          return -1;
        }
        cell_order_str = optarg;
        break;
      case 'O':
        if(tile_order_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one tile orders provided.\n";
          return -1;
        }
        tile_order_str = optarg;
        break;
      case 's':
        if(consolidation_step_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one consolidation steps provided.\n";
          return -1;
        }
        consolidation_step_str = optarg;
        break;
      case 't':
        if(types_str != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one type lists provided.\n";
          return -1;
        }
        types_str = optarg;
        break;
      case 'w':
        if(workspace != "") {
          std::cerr << ERROR_MSG_HEADER
                    << " More than one workspaces provided.\n";
          return -1;
        }
        workspace = optarg;
        break;
      default:
        return -1;
    }
  }

  // Auxiliary variables
  CSVLine temp, array_schema_csv;

  // *********************************************************
  // Check correctness
  // *********************************************************
  // ----- Check number of arguments
  if((argc-1) != 2*option_num) {
    std::cerr << ERROR_MSG_HEADER << " Arguments-options mismatch.\n"; 
    return -1;
  }
  // ----- Check if correct arguments have been ginen
  if(array_name == "") {
    std::cerr << ERROR_MSG_HEADER << " Array name not provided.\n";
    return -1;
  }
  if(attribute_names_str == "") {
    std::cerr << ERROR_MSG_HEADER << " Attribute names not provided.\n";
    return -1;
  }
  if(dim_names_str == "") {
    std::cerr << ERROR_MSG_HEADER << " Dimension names not provided.\n";
    return -1;
  }
  if(dim_domains_str == "") {
    std::cerr << ERROR_MSG_HEADER << " Dimension domains not provided.\n";
    return -1;
  }
  if(types_str == "") {
    std::cerr << ERROR_MSG_HEADER << " Types not provided.\n";
    return -1;
  }
  if(workspace == "") {
    std::cerr << ERROR_MSG_HEADER << " Workspace not provided.\n";
    return -1;
  }

  // ----- The capacity cannot be set for regular tiles
  if(tile_extents_str != "" && capacity_str != "") {
    std::cerr << ERROR_MSG_HEADER 
              << " Capacity is meaningless in the case of regular tiles.\n";
    return -1;
  }
  // ----- Tile order must not be given if the tiles are irregular
  if(tile_extents_str == "" && tile_order_str != "") {
    std::cerr << ERROR_MSG_HEADER 
              << " Tile order is meaningless in the case of irregular tiles.\n";
    return -1;
  }

  // ----- Check number of workspaces workspace
  temp << workspace;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one workspaces provided.\n";
    return -1;
  }

  // *********************************************************
  // Serialize all array schema items into a single CSV string
  // and perform necessary checks
  // *********************************************************
  // ----- array name
  temp.clear();
  temp << array_name;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one array names provided.\n";
    return -1;
  }
  array_schema_csv << temp;
  // ----- attribute_num and attributes
  temp.clear();
  temp << attribute_names_str;
  int attribute_num = temp.val_num();
  array_schema_csv << attribute_num;
  array_schema_csv << temp;
  // ----- dim_num and dimensions
  temp.clear();
  temp << dim_names_str;
  int dim_num = temp.val_num();
  array_schema_csv << dim_num;
  array_schema_csv << temp;
  // ----- dimension domains
  temp.clear();
  temp << dim_domains_str;
  if(temp.val_num() != 2*dim_num) {
    std::cerr << ERROR_MSG_HEADER  
              << " The number of domain bounds does not match the"
              << " provided number of dimensions.\n";
    return -1;
  }
  array_schema_csv << temp;
  // ----- types
  temp.clear();
  temp << types_str;

  if(temp.val_num() != attribute_num + 1) {
    std::cerr << ERROR_MSG_HEADER  
              << " The number of types does not match the number of"
              << " attributes.\n";
    return -1;
  }
  array_schema_csv << temp;
  // ----- tile extents
  temp.clear();
  if(tile_extents_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << tile_extents_str;
    if(temp.val_num() != dim_num) {
      std::cerr << ERROR_MSG_HEADER  
                << " The number of tile extents does not match the number of"
                << " dimensions.\n";
      return -1;
    }
    array_schema_csv << temp;
  }
  // ----- cell order
  temp.clear();
  if(cell_order_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << cell_order_str;
    if(temp.val_num() > 1) {
      std::cerr << ERROR_MSG_HEADER  
                << " More than one cell orders provided.\n";
      return -1;
    }
  array_schema_csv << temp;
  }
  // ----- tile order
  temp.clear();
  if(tile_order_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << tile_order_str;
    if(temp.val_num() > 1) {
      std::cerr << ERROR_MSG_HEADER  
                << " More than one tile orders provided.\n";
      return -1;
    }
    array_schema_csv << temp;
  }
  // ----- capacity
  temp.clear();
  if(capacity_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << capacity_str;
    if(temp.val_num() > 1) {
      std::cerr << ERROR_MSG_HEADER  
                << " More than one capacities provided.\n";
      return -1;
    }
    array_schema_csv << temp;
  }
  // ----- consolidation step
  temp.clear();
  if(consolidation_step_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << consolidation_step_str;
    if(temp.val_num() > 1) {
      std::cerr << ERROR_MSG_HEADER  
                << " More than one consolidation steps provided.\n";
      return -1;
    }
    array_schema_csv << temp;
  }

  // *********************************************************
  // Set the array schema (CSV) string
  // *********************************************************
  array_schema_str = array_schema_csv.c_str();

  return 0;
}

int main(int argc, char** argv) {
  // Arguments
  std::string array_schema_str; // Holds the schema serialized in a CSV string
  std::string workspace;

  // Stores the return code of the various functions below
  int rc; 
 
  // Parse command line
  rc = parse_options(argc, argv, array_schema_str, workspace);
  if(rc) {
    std::cerr << ERROR_MSG_HEADER << " Failed to parse the command line.\n";
    return TILEDB_EPARSE;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  rc = tiledb_ctx_init(workspace.c_str(), tiledb_ctx);
  if(rc) {
    std::cerr << ERROR_MSG_HEADER << " Failed to initialize TileDB.\n";
    return TILEDB_EINIT;
  }

  // Show the array schema
  rc = tiledb_define_array(tiledb_ctx, array_schema_str.c_str());

  if(rc) 
    return rc;

  // Finalize TileDB
  rc = tiledb_ctx_finalize(tiledb_ctx);
  if(rc) {
    std::cerr << ERROR_MSG_HEADER << " Failed to finalize TileDB.\n";
    return TILEDB_EFIN;
  }

  std::cout << MSG_HEADER << " Program executed successfully!\n";
  
  return 0;
}
