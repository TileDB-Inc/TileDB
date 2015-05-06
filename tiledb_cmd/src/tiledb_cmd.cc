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
 * Implements a command line-based frontend to TileDB.
 */

#include <string.h>
#include <iostream>
#include "storage_manager.h"
#include "query_processor.h"
#include "loader.h"
#include "command_line.h"
#include "array_schema.h"
#include "cmd_parser.h"

void print_summary() {
  std::cout << "\n\n"
            << "#####   TileDB: A Sparse Array Data Management System #####\n"
            << "\n\n"
            << "-- Usage --\n\n"
            << "Type:\n"
            << "\ttiledb_cmd -q <query> [options]\n\n"
            << "The following queries are currently supported:\n"
            << "\t - clear_array\n"
            << "\t - define_array\n"
            << "\t - delete_array\n"
            << "\t - export_to_csv\n"
            << "\t - load_csv\n"
            << "\t - show_array_schema\n"
            << "\t - subarray\n\n"
            << "\t - update_csv\n\n"
            << "For more information on the usage of each query, type:\n"
            << "\ttiledb_cmd help <query>\n\n";
}

void print_clear_array() {
  std::cout << "\n-- clear_array --\n\n";
  std::cout << "Deletes all the data from an array. However, the array\n"
            << "remains defined after this command.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array to be cleared. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data are stored. \n\n"
            << "Example:\n"
            << "\t tiledb_cmd -q clear_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array\n"
            << "\n";
}

void print_define_array() {
  std::cout << "\n-- define_array --\n\n"
            << "Defines the schema of an array. Every array must be\n"
            << "defined before being used.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array to be defined. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data will be stored. \n"
            << "\t\tThe provided folder must exist.\n\n"
            << "\t-a or --attribute-name: \n"
            << "\t\tAn attribute name. Multiple attribute names can\n"
            << "\t\tbe provided, by repeating the option along with a\n"
            << "\t\tnew attribute value.\n\n"
            << "\t-d or --dim-name: \n"
            << "\t\tA dimension name. Multiple dimension names can\n"
            << "\t\tbe provided, by repeating the option along with a\n"
            << "\t\tnew dimension value.\n\n"
            << "\t-D or --dim-domain-bound: \n"
            << "\t\tA dimension domain bound. If there are d dimensions,\n"
            << "\t\t2*d domain bounds must be given. Every two bounds in\n"
            << "\t\tthe provided order correspond to each dimension (also\n"
            << "\t\tin the provided order). The first is the lower bound,\n"
            << "\t\tand the second is the upper bound. Any real value is\n"
            << "\t\taccepted as a domain bound.\n\n"
            << "\t-t or --type: \n"
            << "\t\tThe type of an attribute or the coordinates. If a\n"
            << "\t\tattributes are provided, a+1 types must be given (by\n"
            << "\t\trepeating the option with a new value each time). The\n"
            << "\t\tfirst a types correpsond to the attributes (in the\n"
            << "\t\tprovided order), whereas the last type corresponds\n"
            << "\t\tto the coordinates (i.e., to all the dimensions\n"
            << "\t\tcollectively).The supported types for an attribute\n"
            << "\t\tare: char, int, int64_t, float and double. The\n"
            << "\t\tsupported types for the coordinates are: int, int64_t,\n"
            << "\t\tfloat and double.\n\n"
            << "\t-e or --tile-extent: \n"
            << "\t\tThis applies only to regular tiles (for irregular\n"
            << "\t\ttiles it must be omitted). It determines the extent\n"
            << "\t\tof each tile along one dimension. If there are d\n"
            << "\t\tdimensions, there must be d tile extents, one for\n"
            << "\t\teach dimension in the provided order. Any real value\n"
            << "\t\tis accepted as a tile extent.\n\n"
            << "\t-o or --cell-order:\n"
            << "\t\tThe cell order. The supported cell orders are:\n"
            << "\t\trow-major, column-major and Hilbert. If no cell order\n"
            << "\t\tis given, row-major will be the default order.\n\n"
            << "\t-O or --tile-order:\n"
            << "\t\tThe tile order (applicable only to regular tiles). The\n"
            << "\t\tsupported tile orders are: row-major, column-major and\n" 
            << "\t\tHilbert. If no tile order is given, row-major will be the\n"
            << "\t\tdefault order.\n\n"
            << "\t-c or --capacity:\n"
            << "\t\tWe distinguish two cases: (i) for irregular tiles, it is\n"
            << "\t\tthe (fixed) number of non-empty cells in each tile, and\n"
            << "\t\t(ii) for regular tiles, it simply helps in determinining\n"
            << "\t\thow much memory to be reserved (as an optimization), when\n"
            << "\t\ta new tile is created. If the capacity is not given, a\n"
            << "\t\tdefault value is used.\n\n"
            << "\t-s or --consolidation-step:\n"
            << "\t\tEvery time a new updates occurs, a new array fragment is\n"
            << "\t\tcreated (thought of as a snapshot of the array containing\n"
            << "\t\tonly the updates. The consolidation step determines after\n"
            << "\t\thow many updates a merging of fragments must occur. A\n"
            << "\t\tlarge consolidation step leads to very fast updates, but\n"
            << "\t\ta potentially slower query time, and vice versa. If the\n"
            << "\t\tconsolidation step is not given, a default value (1) is\n"
            << "\t\tused.\n\n"
            << "Example #1 (irregular tiles):\n"
            << "\t tiledb_cmd -q define_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -a attr1 -attr2 \\\n"
            << "\t            -d dim1 -d dim2 \\\n"
            << "\t            -D 0 -D 100 -D 0 -D 100 \\\n"
            << "\t            -d dim1 -d dim2 \\\n"
            << "\t            -t int -t double -t int64_t \\\n"
            << "\t            -o hilbert \\\n"
            << "\t            -c 10000 \\\n"
            << "\t            -s 5 \n\n"
            << "Example #2 (regular tiles):\n"
            << "\t tiledb_cmd -q define_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -a attr1 -attr2 \\\n"
            << "\t            -d dim1 -d dim2 \\\n"
            << "\t            -D 0 -D 100 -D 0 -D 100 \\\n"
            << "\t            -d dim1 -d dim2 \\\n"
            << "\t            -t int -t double -t int64_t \\\n"
            << "\t            -e 10 -e 20  \\\n"
            << "\t            -o row-major \\\n"
            << "\t            -O column-major \\\n"
            << "\t            -c 10000 \\\n"
            << "\t            -s 5 \n"
            << "\n";
}

void print_delete_array() {
  std::cout << "\n-- delete_array --\n\n";
  std::cout << "Deletes all the data from an array. Contrary to clear_array,\n"
            << "the array does not remain defined after this command.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array to be deleted. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data are stored. \n\n"
            << "Example:\n"
            << "\t tiledb_cmd -q delete_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array\n"
            << "\n";
}

void print_export_to_csv() {
  std::cout << "\n-- export_to_csv --\n\n"
            << "Exports the array data into a CSV file. The CSV file\n"
            << "follows the array schema (given upon its definition).\n"
            << "A line in the CSV file represents a single (logical) cell.\n"
            << "The coordinates appear first, and then the attribute\n"
            << "values. The order of the coordinates and attribute values\n"
            << "follow the order of the dimensions and attributes,\n"
            << "respectively, in the array schema. Character '*' represents\n"
            << "a NULL value.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array the CSV data are loaded into. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data are stored. \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe name of the CSV file (full path).\n\n"
            << "Example:\n"
            << "\t tiledb_cmd -q export_to_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -f my_array.csv\n"
            << "\n";
}

void print_load_csv() {
  std::cout << "\n-- load_csv --\n\n"
            << "Loads a CSV file into an array. The CSV file must\n"
            << "follow the array schema (given upon its definition).\n"
            << "A line in the CSV file represents a single (logical) cell.\n"
            << "The coordinates must appear first, and then the\n"
            << "attribute values. The order of the coordinates and\n"
            << "attribute values must follow the order of the\n"
            << "dimensions and attributes, respectively, in the\n"
            << "array schema. A NULL value is represented by\n"
            << "character '*'.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array the CSV data are loaded into. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data are stored. \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe path to the CSV file.\n\n"
            << "Example:\n"
            << "\t tiledb_cmd -q load_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -f my_array.csv \n"
            << "\n";
}

void print_show_array_schema() {
  std::cout << "\n-- show_array_schema --\n\n"
            << "Prints the array schema of the input array.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array whose schema will be printed. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data are stored. \n\n"
            << "Example:\n"
            << "\t tiledb_cmd -q show_array_schema \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \n"
            << "\n";
}

void print_subarray() {
  std::cout << "\n-- subarray --\n\n"
            << "Creates a new array with the same schema as the input array,\n"
            << "containing only the cells that lie within the input range.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the input array. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data are stored. \n\n"
            << "\t-r or --range-bound:\n"
            << "\t\tA bound of the input range. If there are d dimensions, 2d\n"
            << "\t\trange bounds must be provided. Every pair of bounds\n"
            << "\t\tcorrespond to the lower and upper bound of the range\n"
            << "\t\tacross a dimension (in the order in which the dimensions\n"
            << "\t\twere given upon the definition of the schems of the input\n"
            << "\t\tarray.\n\n"
            << "\t-R or --result-name: \n"
            << "\t\tThe name of the array that will store the results.\n\n"
            << "Example:\n"
            << "\t tiledb_cmd -q subarray \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A input_array \\\n"
            << "\t            -r 15 -r 20 -r 10 -r 3 \\\n"
            << "\t            -R output_array \n"
            << "\n";
}

void print_update_csv() {
  std::cout << "\n-- update_csv --\n\n"
            << "Updates an array with a new CSV file. The CSV file must\n"
            << "follow the array schema (given upon its definition).\n"
            << "A line in the CSV file represents a single (logical) cell.\n"
            << "The coordinates must appear first, and then the\n"
            << "attribute values. The order of the coordinates and\n"
            << "attribute values must follow the order of the\n"
            << "dimensions and attributes, respectively, in the\n"
            << "array schema. A NULL value is represented by\n"
            << "character '*'. A deletion is represented by including\n"
            << "the coordinates of the deleted cell, and filling all\n"
            << "its attribute values with character '$'.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array the CSV data are loaded into. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe folder where the TileDB data are stored. \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe path to the CSV file.\n\n"
            << "Example:\n"
            << "\t tiledb_cmd -q update_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -f my_array.csv \n"
            << "\n";
}

void print_help(int argc, char** argv) {
  if(argc == 3) {
    if(!strcmp(argv[2],"clear_array")) { 
      print_clear_array();
    } else if(!strcmp(argv[2],"define_array")) { 
      print_define_array();
    } else if(!strcmp(argv[2],"export_to_csv")) { 
      print_export_to_csv();
    } else if(!strcmp(argv[2],"load_csv")) { 
      print_load_csv();
    } else if(!strcmp(argv[2],"delete_array")) { 
      print_delete_array();
    } else if(!strcmp(argv[2],"show_array_schema")) { 
      print_show_array_schema();
    } else if(!strcmp(argv[2],"subarray")) { 
      print_subarray();
    } else if(!strcmp(argv[2],"update_csv")) { 
      print_update_csv();
    } else {
      std::cerr << "[TileDB::fatal_error] Unknown help item '" << argv[2] 
                << "'.\n";
      exit(-1);
    } 
  } else if(argc > 3) {
    std::cerr << "[TileDB::fatal_error] Only one help item should be"
              << " provided.\n";
    exit(-1);
  } else {
    print_summary();
  }
}

void run_clear_array(const CommandLine& cl) {
  CmdParser parser;
  parser.parse_clear_array(cl);

  try {
    StorageManager storage_manager(cl.workspace_);
    storage_manager.clear_array(cl.array_names_[0]);
  } catch(StorageManagerException &se) {
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    exit(-1);
  }
}

void run_define_array(const CommandLine& cl) {
  CmdParser parser;
  const ArraySchema* array_schema = parser.parse_define_array(cl);

  try {
    StorageManager storage_manager(cl.workspace_);
    storage_manager.define_array(array_schema);
  } catch(StorageManagerException &se) {
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    delete array_schema;
    exit(-1);
  }

  // Clean up
  delete array_schema;
}

void run_delete_array(const CommandLine& cl) {
  CmdParser parser;
  parser.parse_delete_array(cl);

  try {
    StorageManager storage_manager(cl.workspace_);
    storage_manager.delete_array(cl.array_names_[0]);
  } catch(StorageManagerException &se) {
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    exit(-1);
  }
}

void run_export_to_csv(const CommandLine& cl) {
  CmdParser parser;
  parser.parse_export_to_csv(cl);

  try {
    StorageManager storage_manager(cl.workspace_);
    QueryProcessor query_processor(&storage_manager);
    query_processor.export_to_csv(cl.array_names_[0], cl.filename_);
  } catch(StorageManagerException &se) {
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    exit(-1);
  } catch(QueryProcessorException &qe) {
    std::cerr << "[TileDB::QueryProcessor::fatal_error] " << qe.what() << "\n";
    exit(-1);
  }
}

void run_load_csv(const CommandLine& cl) {
  CmdParser parser;
  parser.parse_load_csv(cl);

  try {
    StorageManager storage_manager(cl.workspace_);
    Loader loader(&storage_manager);
    loader.load_csv(cl.filename_, cl.array_names_[0]);
  } catch(StorageManagerException &se) {
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    exit(-1);
  } catch(LoaderException &le) {
    std::cerr << "[TileDB::Loader::fatal_error] " << le.what() << "\n";
    exit(-1);
  }
}

void run_show_array_schema(const CommandLine& cl) {
  CmdParser parser;
  parser.parse_show_array_schema(cl);
  const ArraySchema* array_schema;

  try {
    StorageManager storage_manager(cl.workspace_);
    array_schema = storage_manager.get_array_schema(cl.array_names_[0]);
    array_schema->print();
    delete array_schema; 
  } catch(StorageManagerException &se) {
    delete array_schema;
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    exit(-1);
  }
}

void run_subarray(const CommandLine& cl) {
  CmdParser parser;
  const double* range = parser.parse_subarray(cl);

  try {
    StorageManager storage_manager(cl.workspace_);
    QueryProcessor query_processor(&storage_manager);
    query_processor.subarray(cl.array_names_[0], range, cl.result_name_);
  } catch(StorageManagerException &se) {
    delete [] range;
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    exit(-1);
  } catch(QueryProcessorException &qe) {
    delete [] range;
    std::cerr << "[TileDB::QueryProcessor::fatal_error] " << qe.what() << "\n";
    exit(-1);
  }

  // Clean up
  delete [] range;
}

void run_update_csv(const CommandLine& cl) {
  CmdParser parser;
  parser.parse_update_csv(cl);

  try {
    StorageManager storage_manager(cl.workspace_);
    Loader loader(&storage_manager);
    loader.update_csv(cl.filename_, cl.array_names_[0]);
  } catch(StorageManagerException &se) {
    std::cerr << "[TileDB::StorageManager::fatal_error] " << se.what() << "\n";
    exit(-1);
  } catch(LoaderException &le) {
    std::cerr << "[TileDB::Loader::fatal_error] " << le.what() << "\n";
    exit(-1);
  }
}

void run_query(const CommandLine& cl) {
  if(cl.query_ == NULL) {
    std::cerr << "[TileDB::fatal_error] Query not provided.\n"; 
    exit(-1);
  } else if(!strcmp(cl.query_, "clear_array")) {
    run_clear_array(cl);
  } else if(!strcmp(cl.query_, "define_array")) {
    run_define_array(cl);
  } else if(!strcmp(cl.query_, "delete_array")) {
    run_delete_array(cl);
  } else if(!strcmp(cl.query_, "export_to_csv")) {
    run_export_to_csv(cl);
  } else if(!strcmp(cl.query_, "load_csv")) {
    run_load_csv(cl);
  } else if(!strcmp(cl.query_, "show_array_schema")) {
    run_show_array_schema(cl);
  } else if(!strcmp(cl.query_, "subarray")) {
    run_subarray(cl);
  } else if(!strcmp(cl.query_, "update_csv")) {
    run_update_csv(cl);
  } else {
    std::cerr << "[TileDB::fatal_error] Unknown query '" 
              << cl.query_ << "'.\n";
    exit(-1);
  }

  std::cout << "[TileDB] Query executed successfully!\n";
}

int main(int argc, char** argv) {
  // No input arguments
  if(argc == 1) { 
    print_summary();
    return 0;
  }

  if(!strcmp(argv[1], "help")) { 
    print_help(argc, argv);
  } else { 
    // Parse command line
    CommandLine cl;
    cl.parse(argc, argv);
   
    // Check for redundant arguments
    if(argc != 2*cl.option_num_ + 1) {
      std::cerr << "[TileDB::fatal_error] Arguments do not match the option"
                << " labels.\n";
      exit(-1);
    }

    // Run the query
    run_query(cl);
  }

  return 0;
}
