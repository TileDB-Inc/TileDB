/**
 * @file   tiledb_array_clear.cc
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
 * Implements command "tiledb_array_clear".
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
#include <getopt.h>
#include <iostream>
#include <string>

#define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n"
#define PRINT(x) std::cout << "[TileDB] " << x << ".\n"

/** 
 * Parses the command options. It returns 0 on success. On error, it prints
 * a message on stderr and returns -1.
 */                                                            
int parse_options(
    int argc, 
    char** argv, 
    std::string& workspace,
    std::string& group,
    std::string& array_name) {

  // Initialization
  workspace = "";
  group = "";
  array_name = "";

  // Auxiliary variables
  CSVLine temp, array_schema_csv;

  // *************** //
  // Parse options * //
  // *************** // 

  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"group",1,0,'g'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };

  // Define short options
  const char* short_options = "A:g:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          PRINT_ERROR("More than one array names provided");
          return -1;
        }
        array_name = optarg;
        break;
      case 'g':
        if(group != "") {
          PRINT_ERROR("More than one groups provided");
          return -1;
        }
        group = optarg;
        break;
      case 'w':
        if(workspace != "") {
          PRINT_ERROR("More than one workspaces provided");
          return -1;
        }
        workspace = optarg;
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
  // ----- array name ----- //
  if(array_name == "") {
    PRINT_ERROR("Array name not provided");
    return -1;
  }
  temp << array_name;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one array names provided");
    return -1;
  }
  // ----- workspace ----- //
  temp.clear();
  temp << workspace;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one workspaces provided");
    return -1;
  }
  // ----- group ----- //
  temp.clear();
  temp << group;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one groups provided");
    return -1;
  }

  // Success
  return 0;
}

int main(int argc, char** argv) {
  // Parse command line
  std::string workspace, group, array_name;
  if(parse_options(argc, argv, workspace, group, array_name)) 
    return -1;
 
  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(tiledb_ctx))
    return -1;

  // Clear the array
  if(tiledb_array_clear(tiledb_ctx, workspace.c_str(), 
                        group.c_str(), array_name.c_str())) {
    tiledb_ctx_finalize(tiledb_ctx);
    return -1;
  }

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx))
    return -1;

  // Success
  PRINT("Program executed successfully");
  
  return 0;
}
