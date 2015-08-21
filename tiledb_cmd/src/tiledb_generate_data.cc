/**
 * @file   tiledb_generate_data.cc
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
 * Implements command "tiledb_generate_data".
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
#include "utils.h"
#include <chrono>
#include <getopt.h>
#include <iostream>
#include <string>

/** 
 * Parses the command options. It returns 0 on success. On error, it prints
 * a message on stderr and returns -1.
 */                                                            
int parse_options(
    int argc, 
    char** argv, 
    std::string& array_name, 
    std::string& workspace,
    std::string& filename,
    std::string& filetype,
    unsigned int& seed,
    int64_t& cell_num) {
  // Initialization
  array_name = "";
  workspace = "";
  filename = "";
  filetype = "";
  std::string seed_str("");
  std::string cell_num_str("");

  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"filename",1,0,'f'},
    {"seed",1,0,'s'},
    {"filetype",1,0,'t'},
    {"cell-num",1,0,'n'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:f:s:t:n:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'A':
        if(array_name != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one array names provided.\n";
          return -1;
        }
        array_name = optarg;
        break;
      case 'f':
        if(filename != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one file names provided.\n";
          return -1;
        }
        filename = optarg;
        break;
      case 's':
        if(seed_str != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than seeds provided.\n";
          return -1;
        }
        seed_str = optarg;
        break;
      case 't':
        if(filetype != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one file types provided.\n";
          return -1;
        }
        filetype = optarg;
        break;
      case 'n':
        if(cell_num_str != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one numbers of cells provided.\n";
          return -1;
        }
        cell_num_str = optarg;
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

  // Check number of arguments
  if((argc-1) != 2*option_num) {
    std::cerr << ERROR_MSG_HEADER << " Arguments-options mismatch.\n";
    return -1;
  }
  // Check if correct arguments have been ginen
  if(array_name == "") {
    std::cerr << ERROR_MSG_HEADER << " Array name not provided.\n";
    return -1;
  }
  if(workspace == "") {
    std::cerr << ERROR_MSG_HEADER << " Workspace not provided.\n";
    return -1;
  }
  if(filename == "") {
    std::cerr << ERROR_MSG_HEADER << " File name not provided.\n";
    return -1;
  }
  if(filetype == "") {
    std::cerr << ERROR_MSG_HEADER << " File type not provided.\n";
    return -1;
  }
  if(cell_num_str == "") {
    std::cerr << ERROR_MSG_HEADER << " Number of cells not provided.\n";
    return -1;
  }

  // Check number of arguments
  // ----- array_name
  CSVLine temp;
  temp << array_name;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one array names provided.\n";
    return -1;
  }
  // ----- workspace
  temp.clear();
  temp << workspace;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one workspaces provided.\n";
    return -1;
  }
  // ----- filename
  temp.clear();
  temp << filename;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one files names provided.\n";
    return -1;
  }
  // ----- filetype
  temp.clear();
  temp << filetype;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one file types provided.\n";
    return -1;
  }
  if(filetype != "csv" && filetype != "bin") {
    std::cerr << ERROR_MSG_HEADER 
              << " Unknown file type '" << filetype << "'.\n";
    return -1;
  }
  // ----- cell_num_str
  temp.clear();
  temp << cell_num_str;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one numbers of cells provided.\n";
    return -1;
  }
  // ----- seed_str
  temp.clear();
  temp << seed_str;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one seeds provided.\n";
    return -1;
  }

  // Get cell_num
  if(!is_non_negative_integer(cell_num_str.c_str())) {
    std::cerr << ERROR_MSG_HEADER 
              << " The number of cells must be a non-negative integer.\n";
    return -1;
  } else {
    sscanf(cell_num_str.c_str(), "%lld", &cell_num); 
  }

  // Get seed  
  if(seed_str == "") {
    seed = std::chrono::system_clock::now().time_since_epoch().count();
  } else if(!is_non_negative_integer(seed_str.c_str())) {
    std::cerr << ERROR_MSG_HEADER 
              << " The seed must be a non-negative integer.\n";
    return -1;
  } else {
    sscanf(seed_str.c_str(), "%u", &seed); 
  }
 
  return 0;
}

int main(int argc, char** argv) {
  // Arguments
  std::string array_name;
  std::string workspace;
  std::string filename;
  std::string filetype;
  unsigned int seed;
  int64_t cell_num;

  // Stores the return code of the various functions below
  int rc; 
 
  // Parse command line
  rc = parse_options(
           argc, argv, array_name, workspace, filename,
           filetype, seed, cell_num);
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

  // Generate data
  rc = tiledb_generate_data(
           tiledb_ctx, array_name.c_str(), filename.c_str(),
           filetype.c_str(), seed, cell_num); 
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
