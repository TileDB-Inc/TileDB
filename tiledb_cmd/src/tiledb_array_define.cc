/**
 * @file   tiledb_array_define.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * Implements command "tiledb_array_define", which enables the user to define
 * an array, specifying its array schema.
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
#include <getopt.h>
#include <iostream>
#include <string>

#define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n"
#define PRINT(x) std::cout << "[TileDB] " << x << ".\n"

/* 
 * Parses the command options. It returns the array workspace, group, and 
 * array schema. The array schema is serialized in CSV string array_schema_str.
 * The format of this string is the following (single line, no '\n' characters
 * involved):
 * 
 * array_name,attribute_num,attribute_1,...,attribute_{attribute_num},
 * dim_num,dim_1,...,dim_{dim_num},
 * dim_domain_1_low,dim_domain_1_high,...,
 * dim_domain_{dim_num}_low,dim_domain_{dim_num}_high
 * type_1,...,type_{attribute_num+1}
 * tile_extents_1,...,tile_extents_{dim_num},
 * cell_order,tile_order,capacity,consolidation_step,
 * compression_type_1,...,compression_type_{attribute_num+1}
 *
 * If one of the items is omitted (e.g., tile_order), then this CSV field will
 * contain character '*' (e.g., it should be ...,cell_order,*,capacity,...). 
 * 
 * It returns 0 on success. On error, it prints a message on stderr if compiled
 * in VERBOSE mode and returns -1.
 */                                                            
int parse_options(
    int argc, 
    char** argv, 
    std::string& workspace,
    std::string& group,
    std::string& array_schema_str) {

  // Initialization
  workspace = "";
  group = "";
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
  std::string compression_str("");

  // Auxiliary variables
  CSVLine temp, array_schema_csv;

  // *************** //
  // Parse options * //
  // *************** // 

  // Define long options
  static struct option long_options[] = 
  {
    {"attribute-names",1,0,'a'},
    {"array-name",1,0,'A'},
    {"capacity",1,0,'c'},
    {"dim-names",1,0,'d'},
    {"dim-domains",1,0,'D'},
    {"tile-extents",1,0,'e'},
    {"group",1,0,'g'},
    {"cell-order",1,0,'o'},
    {"tile-order",1,0,'O'},
    {"consolidation-step",1,0,'s'},
    {"types",1,0,'t'},
    {"workspace",1,0,'w'},
    {"compression",1,0,'z'},
    {0,0,0,0},
  };

  // Define short options
  const char* short_options = "a:A:c:d:D:e:g:o:O:s:t:w:z:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'a':
        if(attribute_names_str != "") {
          PRINT_ERROR("More than one attribute name lists provided");
          return -1;
        }
        attribute_names_str = optarg;
        break;
      case 'A':
        if(array_name != "") {
          PRINT_ERROR("More than one array names provided");
          return -1;
        }
        array_name = optarg;
        break;
      case 'c':
        if(capacity_str != "") {
          PRINT_ERROR("More than one capacities provided");
          return -1;
        }
        capacity_str = optarg;
        break;
      case 'd':
        if(dim_names_str != "") {
          PRINT_ERROR("More than one dimension name lists provided");
          return -1;
        }
        dim_names_str = optarg;
        break;
      case 'D':
        if(dim_domains_str != "") {
          PRINT_ERROR("More than one dimension domain lists provided");
          return -1;
        }
        dim_domains_str = optarg;
        break;
      case 'e':
        if(tile_extents_str != "") {
          PRINT_ERROR("More than one tile extent lists provided");
          return -1;
        }
        tile_extents_str = optarg;
        break;
      case 'g':
        if(group != "") {
          PRINT_ERROR("More than one groups provided");
          return -1;
        }
        group = optarg;
        break;
      case 'o':
        if(cell_order_str != "") {
          PRINT_ERROR("More than one cell orders provided");
          return -1;
        }
        cell_order_str = optarg;
        break;
      case 'O':
        if(tile_order_str != "") {
          PRINT_ERROR("More than one tile orders provided");
          return -1;
        }
        tile_order_str = optarg;
        break;
      case 's':
        if(consolidation_step_str != "") {
          PRINT_ERROR("More than one consolidation steps provided");
          return -1;
        }
        consolidation_step_str = optarg;
        break;
      case 't':
        if(types_str != "") {
          PRINT_ERROR("More than one type lists provided");
          return -1;
        }
        types_str = optarg;
        break;
      case 'w':
        if(workspace != "") {
          PRINT_ERROR("More than one workspaces provided");
          return -1;
        }
        workspace = optarg;
        break;
      case 'z':
        if(compression_str != "") {
          PRINT_ERROR("More than one compression type lists provided");
          return -1;
        }
        compression_str = optarg;
        break;
      default:
        return -1;
    }
  }

  // ******************* //
  // Check correctness * //
  // ******************* //

  // ----- Check number of arguments -----
  if((argc-1) != 2*option_num) {
    PRINT_ERROR("Arguments-options mismatch"); 
    return -1;
  }
  // ----- Check if correct arguments have been given ----- //
  if(array_name == "") {
    PRINT_ERROR("Array name not provided");
    return -1;
  }
  if(attribute_names_str == "") {
    PRINT_ERROR("Attribute names not provided");
    return -1;
  }
  if(dim_names_str == "") {
    PRINT_ERROR("Dimension names not provided");
    return -1;
  }
  if(dim_domains_str == "") {
    PRINT_ERROR("Dimension domains not provided");
    return -1;
  }
  if(types_str == "") {
    PRINT_ERROR("Types not provided");
    return -1;
  }
  // ----- The capacity cannot be set for regular tiles ----- //
  if(tile_extents_str != "" && capacity_str != "") {
    PRINT_ERROR("Capacity is meaningless in the case of regular tiles");
    return -1;
  }
  // ----- Tile order must not be given if the tiles are irregular ----- //
  if(tile_extents_str == "" && tile_order_str != "") {
    PRINT_ERROR("Tile order is meaningless in the case of irregular tiles");
    return -1;
  } 
  // ----- Tile order must not be given together with capacity ----- //
  if(capacity_str != "" && tile_order_str != "") {
    PRINT_ERROR("It is meaningless to provide both tile order and capacity");
    return -1;
  } 

  // ----- Check workspace and group multiplicities ----- //
  temp.clear();
  temp << workspace;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one workspaces provided");
    return -1;
  }
  temp.clear();
  temp << group;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one groups provided");
    return -1;
  }

  // ********************************** //
  // Serialize into a single CSV string //
  // ********************************** //

  // ----- array name ----- //
  temp.clear();
  temp << array_name;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one array names provided");
    return -1;
  }
  array_schema_csv << temp;

  // ----- attribute_num and attribute names ----- //
  temp.clear();
  temp << attribute_names_str;
  int attribute_num = temp.val_num();
  array_schema_csv << attribute_num;
  array_schema_csv << temp;

  // ----- dim_num and dimension names ----- //
  temp.clear();
  temp << dim_names_str;
  int dim_num = temp.val_num();
  array_schema_csv << dim_num;
  array_schema_csv << temp;

  // ----- dimension domains ----- //
  temp.clear();
  temp << dim_domains_str;
  if(temp.val_num() != 2*dim_num) {
    PRINT_ERROR("The number of domain bounds does not match the "
                "provided number of dimensions");
    return -1;
  }
  array_schema_csv << temp;

  // ----- types ----- // 
  temp.clear();
  temp << types_str;
  if(temp.val_num() != attribute_num + 1) {
    PRINT_ERROR("The number of types does not match the number of "
                "attributes");
    return -1;
  }
  array_schema_csv << temp;

  // ----- tile extents ----- //
  temp.clear();
  if(tile_extents_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << tile_extents_str;
    if(temp.val_num() != dim_num) {
      PRINT_ERROR("The number of tile extents does not match the number of "
                  "dimensions");
      return -1;
    }
    array_schema_csv << temp;
  }

  // ----- cell order ----- //
  temp.clear();
  if(cell_order_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << cell_order_str;
    if(temp.val_num() > 1) {
      PRINT_ERROR("More than one cell orders provided");
      return -1;
    }
    array_schema_csv << temp;
  }

  // ----- tile order ----- //
  temp.clear();
  if(tile_order_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << tile_order_str;
    if(temp.val_num() > 1) {
      PRINT_ERROR("More than one tile orders provided");
      return -1;
    }
    array_schema_csv << temp;
  }

  // ----- capacity ----- //
  temp.clear();
  if(capacity_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << capacity_str;
    if(temp.val_num() > 1) {
      PRINT_ERROR("More than one capacities provided");
      return -1;
    }
    array_schema_csv << temp;
  }

  // ----- consolidation step ----- //
  temp.clear();
  if(consolidation_step_str == "") {
    array_schema_csv << NULL_CHAR;
  } else {
    temp << consolidation_step_str;
    if(temp.val_num() > 1) {
      PRINT_ERROR("More than one consolidation steps provided");
      return -1;
    }
    array_schema_csv << temp;
  }

  // ----- compression ----- // 
  temp.clear();
  temp << compression_str;
  if(compression_str == "") {
    array_schema_csv << NULL_CHAR; 
  } else {
    if(temp.val_num() != attribute_num + 1) {
      PRINT_ERROR("The number of compression types does not match the number "
                  "of attributes");
      return -1;
    }
    array_schema_csv << temp;
  }

  // ********************************* //
  // Set the array schema (CSV) string //
  // ********************************* //
  array_schema_str = array_schema_csv.c_str();

  // Success
  return 0;
}

int main(int argc, char** argv) {
  // Parse command line
  std::string workspace, group, array_schema_str;
  if(parse_options(argc, argv, workspace, group, array_schema_str)) { 
    PRINT_ERROR("Program failed");
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(tiledb_ctx))
    return -1;

  // Store the array schema
  if(tiledb_array_define(tiledb_ctx, workspace.c_str(), group.c_str(), 
                         array_schema_str.c_str())) {
    tiledb_ctx_finalize(tiledb_ctx);
    PRINT_ERROR("Program failed");
    return -1;
  }

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx))
    return -1;

  // Success
  PRINT("Program executed successfully");
  
  return 0;
}
