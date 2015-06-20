/**
 * @file   tiledb_cmd.cc
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

#include "array_schema.h"
#include "cmd_parser.h"
#include "data_generator.h"
#include "loader.h"
#include "query_processor.h"
#include "storage_manager.h"
#include <assert.h>
#include <iostream>
#include <string.h>

void error_cmd_parser(const std::string& err_msg) {
  std::cerr << "[TileDB::CmdParser::fatal_error] " << err_msg << "\n";
  exit(-1);
}

void error_data_generator(const std::string& err_msg) {
  std::cerr << "[TileDB::DataGenerator::fatal_error] " << err_msg << "\n";
  exit(-1);
}

void error_loader(const std::string& err_msg) {
  std::cerr << "[TileDB::Loader::fatal_error] " << err_msg << "\n";
  exit(-1);
}

void error_query_processor(const std::string& err_msg) {
  std::cerr << "[TileDB::QueryProcessor::fatal_error] " << err_msg << "\n";
  exit(-1);
}

void error_storage_manager(const std::string& err_msg) {
  std::cerr << "[TileDB::StorageManager::fatal_error] " << err_msg << "\n";
  exit(-1);
}

void print_clear_array() {
  std::cout << "\n-- clear_array --\n\n";
  std::cout << "Deletes all the data from an array. However, the array\n"
            << "remains defined after this command.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array to be cleared. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe worskpace where the array data are stored. \n\n"
            << "Example:\n"
            << "\t tiledb_cmd clear_array \\\n"
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
            << "\t\tThe worskpace where the array data will be stored.\n\n"
            << "\t-a or --attribute-names: \n"
            << "\t\tThe list of attribute names. \n\n"
            << "\t-d or --dim-names: \n"
            << "\t\tThe list of dimension names.\n\n"
            << "\t-D or --dim-domains: \n"
            << "\t\tThe list of dimension domain bounds. If there are d\n"
            << "\t\tdimensions, 2*d domain bounds must be given. Every two\n"
            << "\t\tbounds in the provided order correspond to each dimension\n"
            << "\t\t(also in the provided order). The first is the lower\n"
            << "\t\tbound, and the second is the upper bound. Any real value\n"
            << "\t\tis accepted as a domain bound.\n\n"
            << "\t-t or --types: \n"
            << "\t\tThe list of types for the attributes and the coordinates.\n"
            << "\t\tIf a attributes are provided, a+1 types must be given.\n"
            << "\t\tThe first a types correpsond to the attributes (in the\n"
            << "\t\tprovided order), whereas the last type corresponds\n"
            << "\t\tto the coordinates (i.e., to all the dimensions\n"
            << "\t\tcollectively). The supported types for an attribute\n"
            << "\t\tare: char, int, int64_t, float and double. The\n"
            << "\t\tsupported types for the coordinates are: int, int64_t,\n"
            << "\t\tfloat and double. Optionally, one may specify the number\n"
            << "\t\tof values to be stored per attribute. This is done by\n"
            << "\t\tappending ':' followed by the number of values after\n"
            << "\t\teach type. If no such value is provided, the default 1 is\n"
            << "\t\tused. If one needs a variable number of values per\n"
            << "\t\tattribute, \":var\" must be appended. Note that the\n"
            << "\t\tdimension type cannot have multiple values (i.e., there\n"
            << "\t\tshould be a single set of coordinates that identifies\n"
            << "\t\teach cell).\n\n"
            << "\t-e or --tile-extents: \n"
            << "\t\tThis applies only to regular tiles (for irregular\n"
            << "\t\ttiles, it must be omitted). It determines the extent\n"
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
            << "\t\tThis is applicable only to irregular tiles. It specifies\n"
            << "\t\tthe (fixed) number of non-empty cells in each tile.\n"
            << "\t\tIf the capacity is not given, a default value is used.\n\n"
            << "\t-s or --consolidation-step:\n"
            << "\t\tEvery time a new updates occurs, a new array fragment is\n"
            << "\t\tcreated, thought of as a snapshot of the array containing\n"
            << "\t\tonly the updates. The consolidation step determines after\n"
            << "\t\thow many updates a merging of fragments must occur. A\n"
            << "\t\tlarge consolidation step leads to very fast updates, but\n"
            << "\t\ta potentially slower query time, and vice versa. If the\n"
            << "\t\tconsolidation step is not given, the default value 1 is\n"
            << "\t\tused.\n\n"
            << "Example #1 (irregular tiles):\n"
            << "\t tiledb_cmd define_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -a 'attr1,attr2' \\\n"
            << "\t            -d 'dim1,dim2' \\\n"
            << "\t            -D '0,100,0,100' \\\n"
            << "\t            -t 'int:var,double:2,int64_t' \\\n"
            << "\t            -o hilbert \\\n"
            << "\t            -c 10000 \\\n"
            << "\t            -s 5 \n\n"
            << "Example #2 (regular tiles):\n"
            << "\t tiledb_cmd define_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -a 'attr1,attr2' \\\n"
            << "\t            -d 'dim1,dim2' \\\n"
            << "\t            -D '0,100,0,100' \\\n"
            << "\t            -t 'int:var,double,int64_t' \\\n"
            << "\t            -e '10,20' \\\n"
            << "\t            -o row-major \\\n"
            << "\t            -O column-major \\\n"
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
            << "\t\tThe workspace where the data are stored. \n\n"
            << "Example:\n"
            << "\t tiledb_cmd delete_array \\\n"
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
            << "a NULL value. If an attribute has variable size (except\n"
            << "for character strings), the number of values precedes the\n"
            << "list of values for that attribute.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array the CSV data are created from. \n\n"
            << "\t-a or --attribute-names: \n"
            << "\t\tThe attributes whose values will be exported. This option\n"
            << "\t\tis optional. If omitted, all attribute values are\n"
            << "\t\texported. If \"__hide\" is specified, then no attribute\n"
            << "\t\tvalues are exported. Multiplicities are allowed, and any\n"
            << "\t\tattribute order is acceptable. The attribute values are\n"
            << "\t\texported in the specified order.\n\n"
            << "\t-d or --dim-names: \n"
            << "\t\tThe dimension coordinates whose values will be exported.\n"
            << "\t\tThis option is optional. If omitted, all coordinate\n"
            << "\t\tvalues are exported. If \"__hide\" is specified, then no\n"
            << "\t\tcoordinate values are exported. Multiplicities are\n"
            << "\t\tallowed, and any coordinate order is acceptable.\n"
            << "\t\tThe coordinates are exported in the specified order.\n\n"
            << "\t-m or --mode: \n"
            << "\t\tSpecifies whether the cells will be exported in the order\n"
            << "\t\tthey are stored in the input array ('normal'), or in the\n"
            << "\t\treverse order ('reverse'). If the option is omitted, the\n"
            << "\t\tdefault 'normal' is assumed.\n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe workspace where the array data are stored. \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe name of the CSV file.\n\n"
            << "Example #1:\n"
            << "\t tiledb_cmd export_to_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -f my_array.csv\n\n"
            << "Example #2:\n"
            << "\t tiledb_cmd export_to_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -a attr1,attr2,attr1 \\\n"
            << "\t            -d dim1 \\\n"
            << "\t            -f my_array.csv\n"
            << "\n"
            << "Example #3:\n"
            << "\t tiledb_cmd export_to_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -d __hide \\\n"
            << "\t            -f my_array.csv\n"
            << "\n"
            << "Example #4:\n"
            << "\t tiledb_cmd export_to_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -m reverse \\\n"
            << "\t            -f my_array.csv\n\n";
}

void print_generate_synthetic_data() {
  std::cout << "\n-- generate_syntetic_data --\n\n"
            << "Generates a file with synthetic data for a defined array. The\n"
            << "user may specify the distribution of the coordinates within\n"
            << "the array domain, whereas the attribute values are always\n"
            << "drawn uniformly at random from their corresponding type\n"
            << "domain (except for characters which are uniformly drawn from\n"
            << "the decimal ASCII interval [45,126]). The user may specify\n"
            << "the type of the output file, which can be a CSV (csv), a\n"
            << "sorted CSV (sorted_csv) along the array cell order, a binary\n"
            << "binary (bin), or a sorted binary (sorted_bin). Finally, the\n"
            << "user may specify either the number of cells to be generated,\n"
            << " or the size (in GBs) of the file to be generated.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array for which the synthetic data are\n"
            << "\t\tgenerated.\n\n"
            << "\t-d or --distribution:\n"
            << "\t\tThe distribution of the coordinates to be generated\n"
            << "\t\t(optional, the default is 'uniform'). \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe name of the file to be generated. \n\n"
            << "\t-n or --cell-number\n"
            << "\t\tThe number of cells to be generated.\n\n"
            << "\t-s or --seed:\n"
            << "\t\tA seed for the random generator (optional, the default\n"
            << "\t\tis derived from the current time). \n\n"
            << "\t-S or --file-size:\n"
            << "\t\tThe size of the file to be generated. \n\n"
            << "\t-t or --file-type:\n"
            << "\t\tThe type of the file to be generated ('csv',\n"
            << "\t\t'sorted_csv', 'bin', 'sorted_bin'). This is optional, the\n"
            << "\t\tdefault is 'csv'.\n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe workspace where the input array is defined. \n\n"
            << "Example #1:\n"
            << "\t tiledb_cmd generate_synthetic_data \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -d uniform \\\n"
            << "\t            -n 10000 \\\n"
            << "\t            -t csv \\\n"
            << "\t            -f my_array.csv \n"
            << "\n"
            << "Example #2:\n"
            << "\t tiledb_cmd generate_synthetic_data \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -S 0.5 \\\n"
            << "\t            -s 0 \\\n"
            << "\t            -t bin \\\n"
            << "\t            -f my_array.bin \n"
            << "\n";
}

void print_load_bin() {
  std::cout << "\n-- load_csv --\n\n"
            << "Loads a binary file into an array. The file must\n"
            << "follow the array schema (given upon its definition).\n\n"
            << "Options:\n"
            << "\t-A or --array-names: \n"
            << "\t\tThe name of the array the data are loaded into. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe workspace where the array data will be stored. \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe path to the binary file.\n\n"
            << "Example:\n"
            << "\t tiledb_cmd load_bin \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -f my_array.bin \n"
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
            << "\t-A or --array-names: \n"
            << "\t\tThe name of the array the CSV data are loaded into. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe workspace where the array data will be stored. \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe path to the CSV file.\n\n"
            << "Example:\n"
            << "\t tiledb_cmd load_csv \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -f my_array.csv \n"
            << "\n";
}

void print_load_sorted_bin() {
  std::cout << "\n-- load_sorted_bin --\n\n"
            << "Loads a binary file, or a set of binary files. Each file is\n"
            << "a set of serialized (binary) cells, sorted on the global cell\n"
            << "order specified in the schema of the array. Each cell must\n"
            << "have a specific format recognizable by TileDB, which strongly\n"
            << "depends on the array schema. Therefore, the binary files are\n"
            << "typically generated by a specialized TileDB sister program.\n"
            << "The binary file name, or the directory containing the binary\n"
            << "files, is given as input. Note that, if a directory is given\n"
            << "as input, all the files in the directory will be merged.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array the CSV data are loaded into. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe workspace where the array data will be stored. \n\n"
            << "\t-p or --path:\n"
            << "\t\tThe binary file name or the directory that contains the\n"
            << "\t\tthe binary files.\n\n"
            << "Example #2:\n"
            << "\t tiledb_cmd load_sorted_bin \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -p my_dir/ \n"
            << "\n"
            << "\t tiledb_cmd load_sorted_bin \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A my_array \\\n"
            << "\t            -p my_array.bin \\\n"
            << "\n";
}

void print_show_array_schema() {
  std::cout << "\n-- show_array_schema --\n\n"
            << "Prints the array schema of the input array.\n\n"
            << "Options:\n"
            << "\t-A or --array-name: \n"
            << "\t\tThe name of the array whose schema will be printed. \n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe workspace where the array data are stored. \n\n"
            << "Example:\n"
            << "\t tiledb_cmd show_array_schema \\\n"
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
            << "\t-a or --attribute-names: \n"
            << "\t\tThe attribute names from the input array that will be\n"
            << "\t\twritten to the output. The attributes are written in the\n"
            << "\t\torder they are given. This is an optional option. If it\n"
            << "\t\tis omitted, then all the attributes of the input array\n"
            << "\t\twill be written to the output array.\n\n"
            << "\t-m or --mode: \n"
            << "\t\tSpecifies whether the cells will be written to the output\n"
            << "\t\tarray in the order they are stored in the input array\n"
            << "\t\t('normal'), or in the reverse order ('reverse'). If the\n"
            << "\t\tthe option is omitted, the default 'normal' is\n"
            << "\t\tassumed.\n\n"
            << "\t-w or --workspace: \n"
            << "\t\tThe workspace where the array data are stored. \n\n"
            << "\t-r or --range:\n"
            << "\t\tThe list of range bounds. If there are d dimensions, 2d\n"
            << "\t\trange bounds must be provided. Every pair of bounds\n"
            << "\t\tcorrespond to the lower and upper bound of the range\n"
            << "\t\tacross a dimension (in the order in which the dimensions\n"
            << "\t\twere given upon the definition of the schems of the input\n"
            << "\t\tarray.\n\n"
            << "\t-R or --result-name: \n"
            << "\t\tThe name of the array that will store the results.\n\n"
            << "Example #1:\n"
            << "\t tiledb_cmd subarray \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A input_array \\\n"
            << "\t            -r '15,20,10,13' \\\n"
            << "\t            -R output_array \n\n"
            << "Example #2:\n"
            << "\t tiledb_cmd subarray \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A input_array \\\n"
            << "\t            -a attr1 \\\n"
            << "\t            -r '15,20,10,13' \\\n"
            << "\t            -R output_array \n\n"
            << "Example #3:\n"
            << "\t tiledb_cmd subarray \\\n"
            << "\t            -w ~/TileDB/ \\\n"
            << "\t            -A input_array \\\n"
            << "\t            -m reverse \\\n"
            << "\t            -r '15,20,10,13' \\\n"
            << "\t            -R output_array \n\n";
}

void print_summary() {
  std::cout << "\n\n"
            << "#####   TileDB: A Sparse Array Data Management System #####\n"
            << "\n\n"
            << "-- Usage --\n\n"
            << "Type:\n"
            << "\ttiledb_cmd <query> [options]\n\n"
            << "The following queries are currently supported:\n"
            << "\t - clear_array\n"
            << "\t - define_array\n"
            << "\t - delete_array\n"
            << "\t - export_to_csv\n"
            << "\t - generate_synthetic_data\n"
            << "\t - load_csv\n"
            << "\t - load_sorted_bin\n"
            << "\t - show_array_schema\n"
            << "\t - subarray\n"
            << "\t - update_csv\n\n"
            << "For more information on the usage of each query, type:\n"
            << "\ttiledb_cmd help <query>\n\n";
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
            << "\t\tThe workspace where the array data are stored. \n\n"
            << "\t-f or --filename:\n"
            << "\t\tThe path to the CSV file.\n\n"
            << "Example:\n"
            << "\t tiledb_cmd update_csv \\\n"
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
    } else if(!strcmp(argv[2],"generate_synthetic_data")) { 
      print_generate_synthetic_data();
    } else if(!strcmp(argv[2],"load_csv")) { 
      print_load_csv();
    } else if(!strcmp(argv[2],"load_sorted_bin")) { 
      print_load_sorted_bin();
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

void run_clear_array(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser cmd_parser;
  err = cmd_parser.parse_clear_array(argc, argv, array_name, 
                                     workspace, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  err = storage_manager.clear_array(array_name, err_msg);
  if(err == -1)
    error_storage_manager(err_msg);
}

void run_define_array(int argc, char** argv) {
  // Query arguments
  std::string workspace;
  const ArraySchema* array_schema;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser cmd_parser;
  err = cmd_parser.parse_define_array(argc, argv, array_schema, 
                                      workspace, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  err = storage_manager.define_array(array_schema, err_msg);
  if(err == -1) {
    delete array_schema;
    error_storage_manager(err_msg);
  }
  delete array_schema;
}

void run_delete_array(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_delete_array(argc, argv, array_name, 
                                  workspace, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);
  
  // Execute query
  StorageManager storage_manager(workspace);
  err = storage_manager.delete_array(array_name, err_msg);
  if(err == -1)
    error_storage_manager(err_msg);
}

void run_export_to_csv(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;
  std::string filename;
  std::vector<std::string> dim_names;
  std::vector<std::string> attribute_names;
  bool reverse;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_export_to_csv(argc, argv, array_name, workspace, 
                                   filename, dim_names, attribute_names, 
                                   reverse, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  QueryProcessor query_processor(&storage_manager);
  err = query_processor.export_to_csv(array_name, filename, dim_names, 
                                      attribute_names, reverse, err_msg);
  if(err == -1)
    error_query_processor(err_msg);
}

void run_generate_synthetic_data(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;
  std::string distribution;
  std::string filename;
  std::string file_type;
  unsigned seed;
  int64_t cell_num;
  size_t file_size; 

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_generate_synthetic_data(argc, argv, array_name, workspace,
                                             filename, file_type, seed,
                                             distribution, cell_num, 
                                             file_size, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);
  
  // Execute query
  StorageManager storage_manager(workspace);
  ArraySchema* array_schema;
  err = storage_manager.get_array_schema(array_name, array_schema, err_msg);
  if(err == -1)
    error_storage_manager(err_msg);
  DataGenerator data_generator(array_schema);

  if(distribution == "uniform") {
    if(file_type == "csv") {
      if(cell_num != -1)
        err = data_generator.generate_uniform_csv(seed, filename, 
                                                  cell_num, err_msg);
      else
        err = data_generator.generate_uniform_csv(seed, filename, 
                                                  file_size, err_msg);
    } else if(file_type == "bin") {
      if(cell_num != -1)
        err = data_generator.generate_uniform_bin(seed, filename, 
                                                  cell_num, err_msg);
      else
        err = data_generator.generate_uniform_bin(seed, filename, 
                                                  file_size, err_msg);
    } else if(file_type == "sorted_csv") {
      if(cell_num != -1)
        err = data_generator.generate_sorted_uniform_csv(seed, filename, 
                                                         cell_num, err_msg);
      else
        err = data_generator.generate_sorted_uniform_csv(seed, filename, 
                                                         file_size, err_msg);
    } else if(file_type == "sorted_bin") {
      if(cell_num != -1)
        err = data_generator.generate_sorted_uniform_bin(seed, filename, 
                                                         cell_num, err_msg);
      else
        err = data_generator.generate_sorted_uniform_bin(seed, filename, 
                                                         file_size, err_msg);
    }
  }

  if(err == -1)
    error_data_generator(err_msg);
}

void run_load_bin(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;
  std::string filename;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_load_bin(argc, argv, array_name, workspace, 
                              filename, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  Loader loader(&storage_manager);
  err = loader.load_bin(filename, array_name);
  if(err == -1)
    error_loader(err_msg);
}

void run_load_csv(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;
  std::string filename;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_load_csv(argc, argv, array_name, workspace, 
                              filename, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  Loader loader(&storage_manager);
  err = loader.load_csv(filename, array_name, err_msg);
  if(err == -1)
    error_loader(err_msg);
}

void run_load_sorted_bin(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;
  std::string path;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_load_sorted_bin(argc, argv, array_name, 
                                     workspace, path, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  if(is_dir(path)) { // Directory of files
    err = storage_manager.load_sorted_bin(path, array_name, err_msg);
    if(err == -1)
      error_storage_manager(err_msg);
  } else if(is_file(path)) {
    Loader loader(&storage_manager);
    err = loader.load_sorted_bin(path, array_name); 
    assert(!err);
  }
}

void run_show_array_schema(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_show_array_schema(argc, argv, array_name, 
                                       workspace, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  ArraySchema* array_schema;
  StorageManager storage_manager(workspace);
  err = storage_manager.get_array_schema(array_name, array_schema, err_msg);
  if(err == -1)
    error_storage_manager(err_msg);
  array_schema->print();
  delete array_schema; 
}

void run_subarray(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;
  std::string result_name;
  std::vector<std::string> attribute_names;
  bool reverse;
  std::vector<double> range;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_subarray(argc, argv, array_name, workspace, result_name,
                              range, attribute_names, reverse, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  QueryProcessor query_processor(&storage_manager);
  err = query_processor.subarray(array_name, range, result_name,
                                 attribute_names, reverse, err_msg);
  if(err == -1)
    error_query_processor(err_msg);
}

void run_update_csv(int argc, char** argv) {
  // Query arguments
  std::string array_name;
  std::string workspace;
  std::string filename;

  // Error handling
  std::string err_msg;
  int err;

  // Parse command line
  CmdParser parser;
  err = parser.parse_update_csv(argc, argv, array_name, workspace, 
                                filename, err_msg);
  if(err == -1)
    error_cmd_parser(err_msg);

  // Execute query
  StorageManager storage_manager(workspace);
  Loader loader(&storage_manager);
  err = loader.update_csv(filename, array_name, err_msg);
  if(err == -1)
    error_loader(err_msg);
}

void run_query(int argc, char** argv) {
  if(!strcmp(argv[1], "clear_array")) {
    run_clear_array(argc, argv);
  } else if(!strcmp(argv[1], "define_array")) {
    run_define_array(argc, argv);
  } else if(!strcmp(argv[1], "delete_array")) {
    run_delete_array(argc, argv);
  } else if(!strcmp(argv[1], "export_to_csv")) {
    run_export_to_csv(argc, argv);
  } else if(!strcmp(argv[1], "generate_synthetic_data")) {
    run_generate_synthetic_data(argc, argv);
  } else if(!strcmp(argv[1], "load_bin")) {
    run_load_bin(argc, argv);
  } else if(!strcmp(argv[1], "load_csv")) {
    run_load_csv(argc, argv);
  } else if(!strcmp(argv[1], "load_sorted_bin")) {
    run_load_sorted_bin(argc, argv);
  } else if(!strcmp(argv[1], "show_array_schema")) {
    run_show_array_schema(argc, argv);
  } else if(!strcmp(argv[1], "subarray")) {
    run_subarray(argc, argv);
  } else if(!strcmp(argv[1], "update_csv")) {
    run_update_csv(argc, argv);
  } else {
    std::cerr << "[TileDB::fatal_error] Unknown query '" 
              << argv[1] << "'.\n";
    exit(-1);
  }

  std::cout << "[TileDB] Query executed successfully!\n";
}

int main(int argc, char** argv) {
  if(argc == 1) 
    print_summary();
  else if(!strcmp(argv[1], "help")) 
    print_help(argc, argv);
  else 
    run_query(argc, argv);

  return 0;
}
