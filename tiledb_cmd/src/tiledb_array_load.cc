/**
 * @file   tiledb_array_load.cc
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
 * Implements command "tiledb_array_load".
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
#include "utils.h"
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
    std::string& array_name, 
    std::string& path, 
    std::string& format,
    char& delimiter) {

  // Initialization
  workspace = "";
  group = "";
  array_name = "";
  path = "";
  format = "";
  std::string delimiter_str = "";

  // Auxiliary variable
  CSVLine temp;

  // *************** //
  // Parse options * //
  // *************** // 

  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"format",1,0,'F'},
    {"group",1,0,'g'},
    {"delimiter",1,0,'l'},
    {"path",1,0,'p'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };

  // Define short options
  const char* short_options = "A:F:g:l:p:w:";
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
      case 'F':
        if(format != "") {
          PRINT_ERROR("More than one formats provided");
          return -1;
        }
        format = optarg;
        break;
      case 'g':
        if(group != "") {
          PRINT_ERROR("More than one groups provided");
          return -1;
        }
        group = optarg;
        break;
      case 'l':
        if(delimiter_str != "") {
          PRINT_ERROR("More than one delimiters provided");
          return -1;
        }
        delimiter_str = optarg;
        break;
      case 'p':
        if(path != "") {
          PRINT_ERROR("More than one paths provided");
          return -1;
        }
        path = optarg;
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

  // Check number of arguments
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
  // ----- path ----- //
  if(path == "") {
    PRINT_ERROR("Path not provided");
    return -1;
  }
  temp.clear();
  temp << path;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one paths provided");
    return -1;
  }
  if(!is_file(path) && !is_dir(path)) {
    PRINT_ERROR("Invalid path");
    return -1;
  }
  // ----- format ----- //
  if(format == "") { // Format not given, derive it from filename
    if(is_file(path)) {
      if(ends_with(path, ".sorted.csv")) {
        format = "sorted.csv"; 
      } else if(ends_with(path, ".sorted.csv.gz")) {
        format = "sorted.csv.gz"; 
      } else if(ends_with(path, ".csv")) {
        format = "csv"; 
      } else if(ends_with(path, ".csv.gz")) {
        format = "csv.gz"; 
      } else if(ends_with(path, ".sorted.bin")) {
        format = "sorted.bin"; 
      } else if(ends_with(path, ".sorted.bin.gz")) {
        format = "sorted.bin.gz"; 
      } else if(ends_with(path, ".bin")) {
        format = "bin"; 
      } else if(ends_with(path, ".bin.gz")) {
        format = "bin.gz"; 
      } else {
        PRINT_ERROR("Cannot derive file format");
        return -1;
      }
    } else {
      PRINT_ERROR("Cannot derive file format for a directory");
      return -1;
    }
  } else { // Format is given, check correctness
    if(format != "csv" && format != "csv.gz" &&
       format != "sorted.csv" && format != "sorted.csv.gz" &&
       format != "bin" && format != "bin.gz" &&
       format != "sorted.bin" && format != "sorted.bin.gz") {
      PRINT_ERROR("Invalid file format");
      return -1;
    }
  }
  // ----- delimiter ----- //
  if(delimiter_str == "") {
    delimiter = ',';
  } else if(ends_with(format, "bin") || ends_with(format, "bin.gz")) {
    PRINT_ERROR("The delimiter is meaningless for binary format");
    return -1;
  } else if(delimiter_str == "tab") {
      delimiter = '\t';
  } else if(delimiter_str.size() == 1) {
      delimiter = delimiter_str[0];
  } else {
      PRINT_ERROR("Invalid delimiter");
      return -1;
  }

  // Success
  return 0;
}

int main(int argc, char** argv) {
  // Parse command line
  std::string workspace, group, array_name, path, format;
  char delimiter;
  if(parse_options(argc, argv, workspace, group, array_name, path, 
                   format, delimiter)) 
    return -1;

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(tiledb_ctx))
    return -1;

  // Load
  if(tiledb_array_load(tiledb_ctx, workspace.c_str(), group.c_str(), 
                       array_name.c_str(), path.c_str(), 
                       format.c_str(), delimiter)) {
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
