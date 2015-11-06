/**
 * @file   tiledb_array_export_csv.cc
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
 * Implements command "tiledb_array_export".
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
    std::string& filename,
    std::string& format,
    std::vector<std::string>& dim_names,
    std::vector<std::string>& attribute_names,
    double*& range,
    int& range_size,
    char& delimiter,
    int& precision) {

  // Initialization
  workspace = "";
  group = "";
  array_name = "";
  filename = "";
  std::string attribute_names_str("");
  std::string dim_names_str("");
  std::string range_str = "";
  std::string mode("");
  dim_names.clear();
  attribute_names.clear();
  format = "";
  std::string delimiter_str = "";
  std::string precision_str = "";

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
    {"dim-names",1,0,'d'},
    {"filename",1,0,'f'},
    {"format",1,0,'F'},
    {"group",1,0,'g'},
    {"delimiter",1,0,'l'},
    {"precision",1,0,'p'},
    {"range",1,0,'r'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };

  // Define short options
  const char* short_options = "a:A:d:f:F:g:l:p:r:w:";
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
      case 'd':
        if(dim_names_str != "") {
          PRINT_ERROR("More than one dimension name lists provided");
          return -1;
        }
        dim_names_str = optarg;
        break;
      case 'f':
        if(filename != "") {
          PRINT_ERROR("More than one CSV file names provided");
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
      case 'p':
        if(precision_str != "") {
          PRINT_ERROR("More than one precision values provided");
          return -1;
        }
        precision_str = optarg;
        break;
      case 'r':
        if(range_str != "") {
          PRINT_ERROR("More than one ranges provided");
          return -1;
        }
        range_str = optarg;
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
  // ----- format ----- //
  if(format == "") { // Format not given, derive it from filename
    if(ends_with(filename, ".reverse.dense.csv")) {
      format = "reverse.dense.csv"; 
    } else if(ends_with(filename, ".reverse.dense.csv.gz")) {
      format = "reverse.dense.csv.gz"; 
    } else if(ends_with(filename, ".reverse.csv")) {
      format = "reverse.csv"; 
    } else if(ends_with(filename, ".reverse.csv.gz")) {
      format = "reverse.csv.gz"; 
    } else if(ends_with(filename, ".dense.csv")) {
      format = "dense.csv"; 
    } else if(ends_with(filename, ".dense.csv.gz")) {
      format = "dense.csv.gz"; 
    } else if(ends_with(filename, ".csv")) {
      format = "csv"; 
    } else if(ends_with(filename, ".csv.gz")) {
      format = "csv.gz"; 
    } else if(ends_with(filename, ".reverse.dense.bin")) {
      format = "reverse.dense.bin"; 
    } else if(ends_with(filename, ".reverse.dense.bin.gz")) {
      format = "reverse.dense.bin.gz"; 
    } else if(ends_with(filename, ".reverse.bin")) {
      format = "reverse.bin"; 
    } else if(ends_with(filename, ".reverse.bin.gz")) {
      format = "reverse.bin.gz"; 
    } else if(ends_with(filename, ".dense.bin")) {
      format = "dense.bin"; 
    } else if(ends_with(filename, ".dense.bin.gz")) {
      format = "dense.bin.gz"; 
    } else if(ends_with(filename, ".bin")) {
      format = "bin"; 
    } else if(ends_with(filename, ".bin.gz")) {
      format = "bin.gz";
    } else {
      PRINT_ERROR("Cannot derive file format");
      return -1;
    }
  } else { // Format is given, check correctness
    if(format != "csv" && format != "csv.gz" &&
       format != "dense.csv" && format != "dense.csv.gz" &&
       format != "reverse.csv" && format != "reverse.csv.gz" &&
       format != "reverse.dense.csv" && format != "reverse.dense.csv.gz" &&
       format != "bin" && format != "bin.gz" &&
       format != "dense.bin" && format != "dense.bin.gz" &&
       format != "reverse.bin" && format != "reverse.bin.gz" &&
       format != "reverse.dense.bin" && format != "reverse.dense.bin.gz") {
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
  // ----- dimension names ----- //
  if(dim_names_str != "")
    dim_names = CSVLine(&dim_names_str[0]).values_str_vec();
  // ----- attribute names ----- //
  if(attribute_names_str != "")
    attribute_names = CSVLine(&attribute_names_str[0]).values_str_vec();
  // ----- range ----- //
  if(range_str == "") {
    range_size = 0;
    range = NULL;
  } else {
    std::vector<std::string> range_str_vec;
    range_str_vec = CSVLine(&range_str[0]).values_str_vec();
    range_size = int(range_str_vec.size());
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
  // ----- precision ----- //
  if(precision_str == "") {
    precision = 6; // Default
  } else {
    temp.clear();
    temp << precision_str;
    if(temp.val_num() > 1) {
      PRINT_ERROR("More than one precision values");
      return -1;
    }
    if(!is_non_negative_integer(precision_str.c_str())) {
      PRINT_ERROR("The precision value must be a non-negative integer");
      return -1;
    } else {
      sscanf(precision_str.c_str(), "%d", &precision); 
    }
  }

  // Success
  return 0;
}

int main(int argc, char** argv) {
  // Parse command line
  std::string workspace;
  std::string group;
  std::string array_name;
  std::string filename;
  std::string format;
  std::vector<std::string> dim_names;
  std::vector<std::string> attribute_names;
  const char** dim_names_c_str;
  const char** attribute_names_c_str;
  double* range = NULL;
  int dim_names_num, attribute_names_num, range_size, precision;
  char delimiter;

  if(parse_options(
         argc, argv, workspace, group, array_name, filename,
         format, dim_names, attribute_names,
         range, range_size, delimiter, precision))
    return -1;

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(tiledb_ctx))
    return -1;

  // Get vectors for dimension and attribute names
  dim_names_num = int(dim_names.size());
  if(dim_names_num == 0) {
    dim_names_c_str = NULL;
  } else {
    dim_names_c_str = new const char*[dim_names_num];
    for(int i=0; i<dim_names_num; ++i) 
      dim_names_c_str[i] = dim_names[i].c_str();
  }
  attribute_names_num = int(attribute_names.size());
  if(attribute_names_num == 0) {
    attribute_names_c_str = NULL;
  } else {
    attribute_names_c_str = new const char*[attribute_names_num];
    for(int i=0; i<attribute_names_num; ++i) 
      attribute_names_c_str[i] = attribute_names[i].c_str();
  }

  // Export to CSV
  if(tiledb_array_export(
         tiledb_ctx, workspace.c_str(), group.c_str(),
         array_name.c_str(), filename.c_str(), format.c_str(),
         dim_names_c_str, dim_names_num, 
         attribute_names_c_str, attribute_names_num,
         range, range_size, delimiter, precision)) { 
    // Error - Clean up
    tiledb_ctx_finalize(tiledb_ctx);
    if(dim_names_c_str != NULL) 
      delete [] dim_names_c_str;
    if(attribute_names_c_str != NULL) 
      delete [] attribute_names_c_str;
    if(range != NULL)
      delete [] range;
    return -1;
  }

  // Clean up  
  if(dim_names_c_str != NULL) 
    delete [] dim_names_c_str;
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
