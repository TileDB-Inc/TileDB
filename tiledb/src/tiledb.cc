/**
 * @file   tiledb.cc
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
 * This is the main program that receives and executes commands from the user.
 */

#include <string.h>
#include <iostream>
#include "command_line.h"
#include "array_schema.h"
#include "parser.h"
#include "executor.h"

void print_options() {
  std::cout << "\t" << "-q or --query\n";
  std::cout << "\t\t" << "The query to be sent to te engine. Examples:\n";
  std::cout << "\t\t" << "define_array, load, subarray, filter, etc. More\n";
  std::cout << "\t\t" << "information on the syntax of each query below.\n";
  std::cout << "\n";

  std::cout << "\t" << "-w or --workspace\n";
  std::cout << "\t\t" << "The folder in which the array data are created.\n";
  std::cout << "\n";

  std::cout << "\t" << "-A or --array-name\n";
  std::cout << "\t\t" << "An array name.\n";
  std::cout << "\n";

  std::cout << "\t" << "-o or --order\n";
  std::cout << "\t\t" << "The cell (tile) order in the case of irregular\n";
  std::cout << "\t\t" << "(resp. regular) tiles. The following orders are\n";
  std::cout << "\t\t" << "supported: hilbert, row-major, column-major.\n";
  std::cout << "\n";

  std::cout << "\t" << "-c or --capacity\n";
  std::cout << "\t\t" << "The maximum number of cells in a tile in the case\n";
  std::cout << "\t\t" << "of irregular tiles. In the case of regular tiles,\n";
  std::cout << "\t\t" << "the capacity is used to reserve some initial space\n";
  std::cout << "\t\t" << "for eacht tile; however, there are no constraints\n";
  std::cout << "\t\t" << "in the number of maximum cells.\n";
  std::cout << "\n";

  std::cout << "\t" << "-a or --attribute-name\n";
  std::cout << "\t\t" << "An attribute name.\n";
  std::cout << "\n";

  std::cout << "\t" << "-d or --dim-name\n";
  std::cout << "\t\t" << "A dimension name. \n";
  std::cout << "\n";

  std::cout << "\t" << "-t or --type\n";
  std::cout << "\t\t" << "A data type. Supported attribute types:\n";
  std::cout << "\t\t" << "char, int, int64_t, float, double.\n";
  std::cout << "\t\t" << "Supported dimension types: \n";
  std::cout << "\t\t" << "int, int64_t, float, double.\n";
  std::cout << "\n";

  std::cout << "\t" << "-D or --dim-domain\n";
  std::cout << "\t\t" << "A lower or upper bound for a dimension domain.\n";
  std::cout << "\t\t" << "See define_array for more details.\n";
  std::cout << "\n";

  std::cout << "\t" << "-e or --tile-extent\n";
  std::cout << "\t\t" << "A tile extent across some dimension.\n";
  std::cout << "\t\t" << "See define_array for more details.\n";
  std::cout << "\n";

  std::cout << "\t" << "-r or --range\n";
  std::cout << "\t\t" << "A lower or upper bound for a range across some\n";
  std::cout << "\t\t" << "dimmension. See subarray for more details.\n";
}

void print_define_array() {
  std::cout << "\t" << "define_array\n";
  std::cout << "\t\t" << "Defines the schema of an array. Every array must\n";
  std::cout << "\t\t" << "be defined before being used. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q define_array \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace, -o order, }\n";
  std::cout << "\t\t" << "       { -a attribute_name, -d dim_name,        }\n";
  std::cout << "\t\t" << "       { -t type, -D dim_domain,                }\n";
  std::cout << "\t\t" << "       [ -a attribute_name, -d dim_name,        ]\n";
  std::cout << "\t\t" << "       [ -t type, -D dim_domain,                ]\n";
  std::cout << "\t\t" << "       [ -e tile_extent, -c capacity            ]\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is a folder where the data of the\n";
  std::cout << "\t\t" << "array will be stored. A single existing workspace\n";
  std::cout << "\t\t" << "must be given. A unique array name must be given,\n";
  std::cout << "\t\t" << "and at least one attribute and dimension name.\n";
  std::cout << "\t\t" << "All attribute and dimension names must be unique.\n";
  std::cout << "\t\t" << "The number of the attribute/dimension names\n";
  std::cout << "\t\t" << "indicates the number of attributes/dimensions.\n";
  std::cout << "\t\t" << "If the number of attributes is attribute_num, the\n";
  std::cout << "\t\t" << "array must have attribute_name+1 types. The lastly\n";
  std::cout << "\t\t" << "provided type always corresponds to the dimensions\n";
  std::cout << "\t\t" << "type. The permissible types for attributes are: \n";
  std::cout << "\t\t" << "char, int, int64_t, float, double. The permissible\n";
  std::cout << "\t\t" << "types for dimensions are: int, int64_t, float,\n";
  std::cout << "\t\t" << "double. Each dimension must have a domain,\n";
  std::cout << "\t\t" << "oriented by a lower and an upper bound. Therefore,\n";
  std::cout << "\t\t" << "the number of bounds must be twice the number of\n";
  std::cout << "\t\t" << "dimensions. Moreover, the lower bound must be\n";
  std::cout << "\t\t" << "smaller than or equal to its corresponding upper.\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "An array may have regular or irregular tiles. If\n";
  std::cout << "\t\t" << "no tile extents are provided, then the array has\n";
  std::cout << "\t\t" << "irregular tiles. In this case, the order indicates\n";
  std::cout << "\t\t" << "the global cell order. The permissible orders are:\n";
  std::cout << "\t\t" << "hilbert, row-major, column-major. If tile extents\n";
  std::cout << "\t\t" << "are provided, then the array has regular tiles.\n";
  std::cout << "\t\t" << "The number of tile extets should be equal to\n";
  std::cout << "\t\t" << "the number of dimensions. In this case, the order\n";
  std::cout << "\t\t" << "indicates the global tile order (the cells in\n";
  std::cout << "\t\t" << "each tile have a default order, typically\n";
  std::cout << "\t\t" << "row-major). The permissible orders here are also: \n";
  std::cout << "\t\t" << "hilbert, row-major, column-major. Each tile extent\n";
  std::cout << "\t\t" << "must not exceed its corresponding domain range.\n";
}

void print_load() {
  std::cout << "\t" << "load\n";
  std::cout << "\t\t" << "Loads a CSV file into an array. The CSV file must\n";
  std::cout << "\t\t" << "follow the array schema (given in its definition).\n";
  std::cout << "\t\t" << "A line in the CSV file represents a single cell.\n";
  std::cout << "\t\t" << "The coordinates must be put first, and then the\n";
  std::cout << "\t\t" << "attribute values. The order of the coordinates and\n";
  std::cout << "\t\t" << "attribute values must follow the order of the\n";
  std::cout << "\t\t" << "dimensions and attributes, respectively, in the\n";
  std::cout << "\t\t" << "array schema. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q load \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace,           }\n";
  std::cout << "\t\t" << "       { -f filename                            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the data of the\n";
  std::cout << "\t\t" << "array stored. A single existing workspace, array\n";
  std::cout << "\t\t" << "name and file name must be given. You may include \n";
  std::cout << "\t\t" << "the file name into single quotes (').\n";
}

void print_update() {
  std::cout << "\t" << "update\n";
  std::cout << "\t\t" << "Performs the updates included in a CSV file to an\n";
  std::cout << "\t\t" << "array. The array must already be defined and\n";
  std::cout << "\t\t" << "loaded. Similar to load, a line in the CSV file\n";
  std::cout << "\t\t" << "represents a cell. The coordintes must be put\n";
  std::cout << "\t\t" << "first, and then the attribute values. The order of\n";
  std::cout << "\t\t" << "of the coordinates and attribute values must\n";
  std::cout << "\t\t" << "follow the order of the dimensions and attributes,\n";
  std::cout << "\t\t" << "respectively, in the array schema. If a new cell\n";
  std::cout << "\t\t" << "in the CSV file does not correspond to an existing\n";
  std::cout << "\t\t" << "cell in the array, then this cell represents an\n";
  std::cout << "\t\t" << "insertion. If a cell already exists in the array,\n";
  std::cout << "\t\t" << "then this cell represents an overwrite. Finally,\n";
  std::cout << "\t\t" << "a deletion is represented with a cell whose\n";
  std::cout << "\t\t" << "attribute values have a special DELETION value,\n";
  std::cout << "\t\t" << "specified with character '$' in the CSV file.\n";
  std::cout << "\t\t" << "Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q update \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace,           }\n";
  std::cout << "\t\t" << "       { -f filename                            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the data of the\n";
  std::cout << "\t\t" << "array stored. A single existing workspace, array\n";
  std::cout << "\t\t" << "name and file name must be given. You may include \n";
  std::cout << "\t\t" << "the file name into single quotes (').\n";
}

void print_user_manual() {
  std::cout << "\n\n";
  std::cout << "TileDB User Manual\n";
  std::cout << "\n\n";

  std::cout << "NAME\n";
  std::cout << "\t\t" << "tiledb - TileDB interactive query program\n";
  std::cout << "\n\n";

  std::cout << "DESCRIPTION\n";
  std::cout << "\t\t" << "This program is used to send user queries to the\n";
  std::cout << "\t\t" << "TileDB engine. Below is the list of permissible\n";
  std::cout << "\t\t" << "options. Type 'tile help <query>' to see the \n";
  std::cout << "\t\t" << "usage of 'query'.\n";
  std::cout << "\n\n";

  std::cout << "OPTIONS\n";
  print_options();
  std::cout << "\n\n";

  std::cout << "QUERIES\n";
  std::cout << "\t\t" << "Below the syntax of the queries is described.\n"
            << "\t\t" << "Options in brackets ([]) are optional (default\n";
  std::cout << "\t\t" << "values will be used). Options in braces ({}) must\n";
  std::cout << "\t\t" << "appear at least once. Options in brackets or\n";
  std::cout << "\t\t" << "braces can be included in any order. Options\n";
  std::cout << "\t\t" << "outside brackets and braces must be given in\n";
  std::cout << "\t\t" << "the order they appear.\n\n";
  print_define_array();
  std::cout << "\t\t" << "\n";
  print_load();
  std::cout << "\t\t" << "\n";
  print_update();
  std::cout << "\t\t" << "\n";

  std::cout << "Enjoy!\n";

  std::cout << "\n\n";
}

void process_help(const CommandLine& cl, int argc, char** argv) {
  if(!strcmp(argv[1],"help")) { 
    if(argc == 2) {
      print_user_manual();
    } else if(argc == 3) {
      if(!strcmp(argv[2],"define_array")) { 
        print_define_array();
      } else if(!strcmp(argv[2],"load")) { 
        print_load();
      } else if(!strcmp(argv[2],"update")) { 
        print_update();
      } else {
        std::cerr << "[TileDB::fatal_error] Unknown query '" << argv[2] << "'."
                  << " Type 'tiledb help' to see the TileDB User Manual.\n";
        exit(-1);
      } 
    } else {
      std::cerr << "[TileDB::fatal_error] Redundant arguments."
                << " Type 'tiledb help' to see the TileDB User Manual.\n";
      exit(-1);
    }
    exit(0);
  }
}

void process_queries(const CommandLine& cl) {
  Parser parser;

  // Query not provided
  if(cl.query_ == NULL) {
    std::cerr << "[TileDB::fatal_error] Query not provided." 
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  } else if(!strcmp(cl.query_, "define_array")) {
    // DEFINE_ARRAY
    ArraySchema array_schema = parser.parse_define_array(cl);
    try {
      Executor executor(cl.workspace_);
      executor.define_array(array_schema);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "load")) {
    // LOAD
    parser.parse_load(cl);
    try {
      Executor executor(cl.workspace_);
      executor.load(cl.filename_, cl.array_name_);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "update")) {
    // UPDATE
    parser.parse_update(cl);
    try {
      Executor executor(cl.workspace_);
      executor.update(cl.filename_, cl.array_name_);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else {
    std::cerr << "[TileDB::fatal_error] Unknown query '" 
              << cl.query_ << "'."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    exit(-1);
  }

  std::cout << "[TileDB] Query executed successfully!\n";
}

int main(int argc, char** argv) {
  // No input arguments
  if(argc == 1) { 
    std::cerr << "[TileDB::fatal_error] No input arguments." 
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    return -1;
  }

  CommandLine cl;
  cl.parse(argc, argv);

  // HELP
  process_help(cl, argc, argv);

  // Option-argument mismatch
  if(argc != 2*cl.option_num_ + 1) {
    std::cerr << "[TileDB::fatal_error] Unknown arguments."
              << " Type 'tiledb help' to see the TileDB User Manual.\n";
    return -1;
  }

  // QUERIES
  process_queries(cl);  

  return 0;
}
