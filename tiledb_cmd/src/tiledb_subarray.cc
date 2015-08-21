/**
 * @file   tiledb_subarray.cc
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
 * Implements command "tiledb_subarray".
 */

#include "csv_line.h"
#include "special_values.h"
#include "tiledb.h"
#include "utils.h"
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
    std::string& result_name, 
    std::vector<double>& range,
    std::vector<std::string>& attribute_names) {
  // Initialization
  workspace = "";
  array_name = "";
  result_name = "";
  std::string range_str("");
  std::string attribute_names_str("");
  range.clear();
  attribute_names.clear();

  // Define long options
  static struct option long_options[] = 
  {
    {"attribute-names",1,0,'a'},
    {"array-name",1,0,'A'},
    {"range",1,0,'r'},
    {"result-name",1,0,'R'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "a:A:r:R:w:";
  // Get options
  int c;
  int option_num = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num;
    switch(c) {
      case 'a':
        if(attribute_names_str != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one attribute name lists provided.\n";
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
      case 'r':
        if(range_str != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one ranges provided.\n";
          return -1;
        }
        range_str = optarg;
        break;
      case 'R':
        if(result_name != "") {
          std::cerr << ERROR_MSG_HEADER 
                    << " More than one result names provided.\n";
          return -1;
        }
        result_name = optarg;
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
  if(result_name == "") {
    std::cerr << ERROR_MSG_HEADER << " Result name not provided.\n";
    return -1;
  }
  if(range_str == "") {
    std::cerr << ERROR_MSG_HEADER << " Range not provided.\n";
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
  // ----- workspace
  temp.clear();
  temp << workspace;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one workspaces provided.\n";
    return -1;
  }
  // ----- result_name
  temp.clear();
  temp << result_name;
  if(temp.val_num() > 1) {
    std::cerr << ERROR_MSG_HEADER 
              << " More than one result names provided.\n";
    return -1;
  }

  // Get range
  std::vector<std::string> range_str_vec;
  double range_bound;
  range_str_vec = CSVLine(&range_str[0]).values_str_vec();
  for(int i=0; i<int(range_str_vec.size()); ++i) {
    if(!is_real(range_str_vec[i].c_str())) {
      std::cerr << ERROR_MSG_HEADER 
                << " The range bounds must be real numbers.\n";
      return -1;
    }
    sscanf(range_str_vec[i].c_str(), "%lf", &range_bound); 
    range.push_back(range_bound);
  }

  // Get attribute names
  if(attribute_names_str != "")
    attribute_names = CSVLine(&attribute_names_str[0]).values_str_vec();

  return 0;
}

int main(int argc, char** argv) {
  // Arguments
  std::string workspace;
  std::string array_name;
  std::string result_name;
  std::vector<double> range;
  std::vector<std::string> attribute_names;
  const char** attribute_names_c_str;

  // Stores the return code of the various functions below
  int rc; 
 
  // Parse command line
  rc = parse_options(
           argc, argv, workspace, array_name, result_name,
           range, attribute_names);
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

  // Populate attribute_names_c_str
  int attribute_names_num = int(attribute_names.size());
  attribute_names_c_str = new const char*[attribute_names_num];
  for(int i=0; i<attribute_names_num; ++i) 
    attribute_names_c_str[i] = attribute_names[i].c_str();

  // Subarray
  rc = tiledb_subarray(
           tiledb_ctx, array_name.c_str(), result_name.c_str(),
           &range[0], int(range.size()),
           attribute_names_c_str, attribute_names_num);
  if(rc) { 
    delete [] attribute_names_c_str;
    return rc;
  }

  // Finalize TileDB
  rc = tiledb_ctx_finalize(tiledb_ctx);
  if(rc) {
    delete [] attribute_names_c_str;
    std::cerr << ERROR_MSG_HEADER << " Failed to finalize TileDB.\n";
    return TILEDB_EFIN;
  }

  std::cout << MSG_HEADER << " Program executed successfully!\n";

  // Clean up  
  delete [] attribute_names_c_str;

  return 0;
}
