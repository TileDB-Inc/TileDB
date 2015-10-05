/**
 * @file   tiledb_subarray.cc
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
 * Implements command "tiledb_subarray".
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
    std::string& workspace_sub,
    std::string& group,
    std::string& group_sub,
    std::string& array_name, 
    std::string& array_name_sub, 
    double*& range,
    int& range_size,
    std::vector<std::string>& attribute_names) {
  // Initialization
  workspace = "";
  workspace_sub = "";
  group = "";
  group_sub = "";
  array_name = "";
  array_name_sub = "";
  std::string workspaces_str, groups_str, array_names_str;
  std::string range_str("");
  std::string attribute_names_str("");
  attribute_names.clear();

  // Auxiliary variable
  CSVLine temp;

  // *************** //
  // Parse options * //
  // *************** // 

  // Define long options
  static struct option long_options[] = 
  {
    {"attribute-names",1,0,'a'},
    {"array-name",1,0,'A'},
    {"group",1,0,'g'},
    {"range",1,0,'r'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };
  // Define short options
  const char* short_options = "a:A:g:r:w:";

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
        if(array_names_str != "") {
          PRINT_ERROR("More than one array name lists provided");
          return -1;
        }
        array_names_str = optarg;
        break;
      case 'g':
        if(groups_str != "") {
          PRINT_ERROR("More than one group lists provided");
          return -1;
        }
        groups_str = optarg;
        break;
      case 'r':
        if(range_str != "") {
          PRINT_ERROR("More than one ranges provided");
          return -1;
        }
        range_str = optarg;
        break;
      case 'w':
        if(workspaces_str != "") {
          PRINT_ERROR("More than one workspace lists provided");
          return -1;
        }
        workspaces_str = optarg;
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
  //----- array names ----- //
  if(array_names_str == "") {
    PRINT_ERROR("Array names not provided");
    return -1;
  }
  temp = &array_names_str[0];
  if(temp.val_num() != 2) {
    PRINT_ERROR("Invalid number of array names");
    return -1;
  }
  temp >> array_name;
  temp >> array_name_sub;
  // ----- workspaces ----- //
  temp.clear();
  temp = &workspaces_str[0];
  if(temp.val_num() > 2) {
    PRINT_ERROR("Invalid number of workspaces");
    return -1;
  }
  temp >> workspace; 
  temp >> workspace_sub; 
  if(workspace_sub == "")
    workspace_sub = workspace;
  // ----- groups ----- //
  temp.clear();
  temp = &groups_str[0];
  if(temp.val_num() > 2) {
    PRINT_ERROR("Invalid number of groups");
    return -1;
  }
  temp >> group; 
  temp >> group_sub;
 // ----- attribute names ----- //
  if(attribute_names_str != "")
    attribute_names = CSVLine(&attribute_names_str[0]).values_str_vec();
  // ----- range ----- //
  std::vector<std::string> range_str_vec;
  range_str_vec = CSVLine(&range_str[0]).values_str_vec();
  range_size = int(range_str_vec.size());
  if(range_size == 0) {
    PRINT_ERROR("Range not provided");
    return -1;
  } else {
    range = new double[range_size];
    for(int i=0; i<range_size; ++i) {
      if(!is_real(range_str_vec[i].c_str())) {
        PRINT_ERROR("The range bounds must be real numbers");
        delete [] range;
        return -1;
      }
      sscanf(range_str_vec[i].c_str(), "%lf", &range[i]); 
    }
  }   

  // Success
  return 0;
}

int main(int argc, char** argv) {
  // Parse command line
  std::string workspace;
  std::string workspace_sub;
  std::string group;
  std::string group_sub;
  std::string array_name;
  std::string array_name_sub;
  double* range = NULL;
  int range_size = 0;
  const char** attribute_names_c_str;
  int attribute_names_num = 0;
  std::vector<std::string> attribute_names;
 
  if(parse_options(
         argc, argv, workspace, workspace_sub, group, group_sub,
         array_name, array_name_sub, range, range_size,
         attribute_names))
    return -1;

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(tiledb_ctx))
    return -1;

  // Get vector for attribute names
  attribute_names_num = int(attribute_names.size());
  if(attribute_names_num == 0) {
    attribute_names_c_str = NULL;
  } else {
    attribute_names_c_str = new const char*[attribute_names_num];
    for(int i=0; i<attribute_names_num; ++i) 
      attribute_names_c_str[i] = attribute_names[i].c_str();
  }

  // Subarray
  if(tiledb_subarray(
         tiledb_ctx, 
         workspace.c_str(), workspace_sub.c_str(),
         group.c_str(), group_sub.c_str(),
         array_name.c_str(), array_name_sub.c_str(),
         range, range_size,
         attribute_names_c_str, attribute_names_num)) {
    // Error
    if(attribute_names_c_str != NULL) 
      delete [] attribute_names_c_str;
    if(range != NULL)
      delete [] range;
    tiledb_ctx_finalize(tiledb_ctx);
    return -1;
  }

  // Clean up  
  if(attribute_names_c_str != NULL) 
    delete [] attribute_names_c_str;
  if(range != NULL)
    delete [] range;

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx))
    return -1;

  // Success
  PRINT("Program executed successfully");
  
  return 0;
}
