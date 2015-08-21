/**
 * @file   tiledb_load_csv.cc
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
 * Implements command "tiledb_load_csv".
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
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
    std::string& workspace,
    std::string& array_name, 
    std::string& path, 
    bool& sorted) {
  // Initialization
  workspace = "";
  array_name = "";
  path = "";
  std::string mode("");

  // Define long options
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"mode",1,0,'m'},
    {"path",1,0,'p'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "A:m:p:w:";
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
      case 'm':
        if(mode != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one modes provided.\n";
          return -1;
        }
        mode = optarg;
        break;
      case 'p':
        if(path != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one paths provided.\n";
          return -1;
        }
        path = optarg;
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
  if(path == "") {
    std::cerr << ERROR_MSG_HEADER << " Path not provided.\n";
    return -1;
  }
  if(workspace == "") {
    std::cerr << ERROR_MSG_HEADER << " Workspace not provided.\n";
    return -1;
  }

  // Check number of arguments
  // ----- array name
  CSVLine temp;
  temp << array_name;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one array names provided.\n";
    return -1;
  }
  // ----- mode
  temp.clear();
  temp << mode;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one modes provided.\n";
    return -1;
  }
  // ----- path
  temp.clear();
  temp << path;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one paths provided.\n";
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

  // Get sorted
  if(mode == "" || mode == "unsorted") {
    sorted = false;
  } else if(mode == "sorted") {
    sorted = true;
  } else {
    std::cerr << ERROR_MSG_HEADER 
              << " Unknown mode.\n";
    return -1;
  }

  return 0;
}

int main(int argc, char** argv) {
  // Arguments
  std::string workspace;
  std::string array_name;
  std::string path;
  bool sorted;

  // Stores the return code of the various functions below
  int rc; 
 
  // Parse command line
  rc = parse_options(argc, argv, workspace, array_name, path, sorted);
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

  // Load binary file
  rc = tiledb_load_csv(
           tiledb_ctx, array_name.c_str(), path.c_str(), sorted);
  if(rc) {
    std::cerr << ERROR_MSG_HEADER << " Failed to load CSV file(s).\n";
    return rc;
  }

  // Finalize TileDB
  rc = tiledb_ctx_finalize(tiledb_ctx);
  if(rc) {
    std::cerr << ERROR_MSG_HEADER << " Failed to finalize TileDB.\n";
    return TILEDB_EFIN;
  }

  std::cout << MSG_HEADER << " Program executed successfully!\n";
  
  return 0;
}
