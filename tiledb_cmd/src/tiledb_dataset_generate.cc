/**
 * @file   tiledb_dataset_generate.cc
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
 * Implements command "tiledb_dataset_generate".
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
#include "utils.h"
#include <chrono>
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
    std::string& filename,
    std::string& format,
    unsigned int& seed,
    int64_t& cell_num,
    char& delimiter) {
  // Initialization
  workspace = "";
  group = "";
  array_name = "";
  filename = "";
  format = "";
  std::string delimiter_str = "";
  std::string seed_str("");
  std::string cell_num_str("");

  // Auxiliary variable
  CSVLine temp;

  // *************** //
  // Parse options * //
  // *************** // 

  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"filename",1,0,'f'},
    {"format",1,0,'F'},
    {"group",1,0,'g'},
    {"delimiter",1,0,'l'},
    {"cell-num",1,0,'n'},
    {"seed",1,0,'s'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };

  // Define short options
  const char* short_options = "A:f:F:g:l:n:s:w:";
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
      case 'f':
        if(filename != "") {
          PRINT_ERROR("More than one file names provided");
          return -1;
        }
        filename = optarg;
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
      case 'n':
        if(cell_num_str != "") {
          PRINT_ERROR("More than one numbers of cells provided");
          return -1;
        }
        cell_num_str = optarg;
        break;
      case 's':
        if(seed_str != "") {
          PRINT_ERROR("More than seeds provided");
          return -1;
        }
        seed_str = optarg;
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

  // ****************** //
  // Check correctness  //
  // ****************** //

  // Check number of arguments
  if((argc-1) != 2*option_num) {
    PRINT_ERROR("Arguments-options mismatch");
    return -1;
  }
  // Check if correct arguments have been ginen
  //----- array name ----- //
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
  // ----- filename ----- //
  if(filename == "") {
    PRINT_ERROR("File name not provided");
    return -1;
  }
  temp.clear();
  temp << filename;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one file names provided");
    return -1;
  }
  // ----- cell number ----- //
  if(cell_num_str == "") {
    PRINT_ERROR("Number of cells not provided");
    return -1;
  }
  temp.clear();
  temp << cell_num_str;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one numbers of cells provided");
    return -1;
  }
  if(!is_non_negative_integer(cell_num_str.c_str())) {
    PRINT_ERROR("The number of cells must be a non-negative integer");
    return -1;
  } else {
    sscanf(cell_num_str.c_str(), "%lld", &cell_num); 
  }
  // ----- seed ----- //
  temp.clear();
  temp << seed_str;
  if(temp.val_num() > 1) {
    PRINT_ERROR("More than one seeds provided");
    return -1;
  }
  if(seed_str == "") {
    seed = std::chrono::system_clock::now().time_since_epoch().count();
  } else if(!is_non_negative_integer(seed_str.c_str())) {
    PRINT_ERROR("The seed must be a non-negative integer");
    return -1;
  } else {
    sscanf(seed_str.c_str(), "%u", &seed); 
  }
  // ----- format ----- //
  if(format == "") { // Format not given, derive it from filename
    if(ends_with(filename, ".csv")) {
      format = "csv"; 
    } else if(ends_with(filename, ".csv.gz")) {
      format = "csv.gz"; 
    } else if(ends_with(filename, ".bin")) {
      format = "bin"; 
    } else if(ends_with(filename, ".bin.gz")) {
      format = "bin.gz"; 
    } else {
      PRINT_ERROR("Cannot derive file format.");
      return -1;
    }
  } else { // Format is given, check correctness
    if(format != "csv" && format != "csv.gz" &&
       format != "bin" && format != "bin.gz") {
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
 
  return 0;
}

int main(int argc, char** argv) {
  // Parse command line
  std::string workspace;
  std::string group;
  std::string array_name;
  std::string filename;
  std::string format;
  unsigned int seed;
  int64_t cell_num;
  char delimiter;

  if(parse_options(
         argc, argv, 
         workspace, group, array_name, 
         filename, format, seed, cell_num, delimiter)) {
    PRINT_ERROR("Program failed");
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(tiledb_ctx))
    return -1;

  // Generate data
  if(tiledb_dataset_generate(
       tiledb_ctx, workspace.c_str(), group.c_str(),
       array_name.c_str(), filename.c_str(), format.c_str(),
       seed, cell_num, delimiter)) {
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
