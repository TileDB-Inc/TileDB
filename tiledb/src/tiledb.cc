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
  std::cout << "\t" << "-A, --array-name\n";
  std::cout << "\t\t" << "An array name.\n";
  std::cout << "\n";

  std::cout << "\t" << "-a, --attribute-name\n";
  std::cout << "\t\t" << "An attribute name.\n";
  std::cout << "\n";

  std::cout << "\t" << "-c, --capacity\n";
  std::cout << "\t\t" << "The maximum number of cells in a tile in the case\n";
  std::cout << "\t\t" << "of irregular tiles. In the case of regular tiles,\n";
  std::cout << "\t\t" << "the capacity is used to reserve some initial space\n";
  std::cout << "\t\t" << "for each tile; however, there are no constraints\n";
  std::cout << "\t\t" << "in the number of maximum cells.\n";
  std::cout << "\n";

  std::cout << "\t" << "-C, --coordinate\n";
  std::cout << "\t\t" << "A coordinate across some dimension.\n";
  std::cout << "\n";

  std::cout << "\t" << "-D, --dim-domain-bound\n";
  std::cout << "\t\t" << "A lower or upper bound for a dimension domain.\n";
  std::cout << "\t\t" << "See define_array for more details.\n";
  std::cout << "\n";

  std::cout << "\t" << "-d, --dim-name\n";
  std::cout << "\t\t" << "A dimension name. \n";
  std::cout << "\n";

  std::cout << "\t" << "-E, --expression\n";
  std::cout << "\t\t" << "A mathematical expression.\n";
  std::cout << "\t\t" << "See filter for more details.\n";
  std::cout << "\n";

  std::cout << "\t" << "-e, --tile-extent\n";
  std::cout << "\t\t" << "A tile extent across some dimension.\n";
  std::cout << "\t\t" << "See define_array for more details.\n";
  std::cout << "\n";

  std::cout << "\t" << "-f, --filename\n";
  std::cout << "\t\t" << "A file name.\n";
  std::cout << "\n";

  std::cout << "\t" << "-N, --number\n";
  std::cout << "\t\t" << "An integral number.\n";
  std::cout << "\n";

  std::cout << "\t" << "-o, --cell_order\n";
  std::cout << "\t\t" << "The cell order. The following orders are\n";
  std::cout << "\t\t" << "supported: hilbert, row-major, column-major.\n";
  std::cout << "\n";

  std::cout << "\t" << "-O, --tile_order\n";
  std::cout << "\t\t" << "The tile order. The following orders are\n";
  std::cout << "\t\t" << "supported: hilbert, row-major, column-major.\n";
  std::cout << "\n";

  std::cout << "\t" << "-q, --query\n";
  std::cout << "\t\t" << "The query to be sent to the engine. Examples:\n";
  std::cout << "\t\t" << "define_array, load, subarray, filter, etc. More\n";
  std::cout << "\t\t" << "information on the syntax of each query below.\n";
  std::cout << "\n";

  std::cout << "\t" << "-r, --range-bound\n";
  std::cout << "\t\t" << "A lower or upper bound for a range across some\n";
  std::cout << "\t\t" << "dimmension. See subarray for more details.\n";
  std::cout << "\n";

  std::cout << "\t" << "-s, --consolidation-step\n";
  std::cout << "\t\t" << "The consolidation step of an array.\n";
  std::cout << "\t\t" << "See define_array for more details.\n";
  std::cout << "\n";

  std::cout << "\t" << "-t, --type\n";
  std::cout << "\t\t" << "A data type. Supported attribute types:\n";
  std::cout << "\t\t" << "char, int, int64_t, float, double.\n";
  std::cout << "\t\t" << "Supported dimension types: \n";
  std::cout << "\t\t" << "int, int64_t, float, double.\n";
  std::cout << "\n";

  std::cout << "\t" << "-w, --workspace\n";
  std::cout << "\t\t" << "The folder in which the array data are created.\n";
  std::cout << "\n";
}

void print_clear_array() {
  std::cout << "\t" << "clear_array\n";
  std::cout << "\t\t" << "Deletes all the fragments of an array. The array\n";
  std::cout << "\t\t" << "remains defined after this command. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q clear_array \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, and array\n";
  std::cout << "\t\t" << "name must be given.\n";
}

void print_define_array() {
  std::cout << "\t" << "define_array\n";
  std::cout << "\t\t" << "Defines the schema of an array. Every array must\n";
  std::cout << "\t\t" << "be defined before being used. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q define_array \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace,           }\n";
  std::cout << "\t\t" << "       { -a attribute_name, -d dim_name,        }\n";
  std::cout << "\t\t" << "       { -t type, -D dim_domain,                }\n";
  std::cout << "\t\t" << "       [ -a attribute_name, -d dim_name,        ]\n";
  std::cout << "\t\t" << "       [ -t type, -D dim_domain,                ]\n";
  std::cout << "\t\t" << "       [ -e tile_extent, -c capacity            ]\n";
  std::cout << "\t\t" << "       [ -s consolidation step                  ]\n";
  std::cout << "\t\t" << "       [ -o cell_order, -O tile_order,          ]\n";
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
  std::cout << "\t\t" << "irregular tiles. The cell and tile orders specify\n";
  std::cout << "\t\t" << "how the cells and tiles will be organized on the\n";
  std::cout << "\t\t" << "disk. The permissible orders are:\n";
  std::cout << "\t\t" << "hilbert, row-major, column-major. If tile extents\n";
  std::cout << "\t\t" << "are provided, then the array has regular tiles.\n";
  std::cout << "\t\t" << "The number of tile extets should be equal to\n";
  std::cout << "\t\t" << "the number of dimensions. Each tile extent\n";
  std::cout << "\t\t" << "must not exceed its corresponding domain range.\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The consolidation step is used in updates. Every\n";
  std::cout << "\t\t" << "time a batch update occurs, the cells therein are\n";
  std::cout << "\t\t" << "loaded in a new array fragment. The consolidation\n";
  std::cout << "\t\t" << "step specifies the number of array fragments that\n";
  std::cout << "\t\t" << "will be created before they are merged into a\n";
  std::cout << "\t\t" << "single fragment.\n";
}

void print_delete_array() {
  std::cout << "\t" << "delete_array\n";
  std::cout << "\t\t" << "Deletes completely the array (fragments and\n";
  std::cout << "\t\t" << "schema). Contrary to clear_array, the array\n";
  std::cout << "\t\t" << "does not remain defined after the command is\n";
  std::cout << "\t\t" << "executed. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q delete_array \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, and array\n";
  std::cout << "\t\t" << "name must be given.\n";
}

void print_export_to_csv() {
  std::cout << "\t" << "export_to_csv\n";
  std::cout << "\t\t" << "Exports an array to a CSV file. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q export_to_csv \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, and array\n";
  std::cout << "\t\t" << "name must be given.\n";
}

void print_filter() {
  std::cout << "\t" << "filter\n";
  std::cout << "\t\t" << "Creates an array that has the same schema as the\n";
  std::cout << "\t\t" << "input array, and contains only the cells that\n";
  std::cout << "\t\t" << "make the input expression evaluate to true.\n";
  std::cout << "\t\t" << "Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q filter \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace            }\n";
  std::cout << "\t\t" << "       { -E filter_expression                   }\n";
  std::cout << "\t\t" << "       { -R result_name                         }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single expression, existing\n";
  std::cout << "\t\t" << "workspace and array name must be given. The filter\n";
  std::cout << "\t\t" << "expression is a boolean expression defined over a\n";
  std::cout << "\t\t" << "subset of the attributes.\n";
}

void print_join() {
  std::cout << "\t" << "join\n";
  std::cout << "\t\t" << "Merges two arrays into a single one, which\n";
  std::cout << "\t\t" << "contains the union of their attribute values.\n";
  std::cout << "\t\t" << "Specifically, the input arrays must be join-\n";
  std::cout << "\t\t" << "compatible: (i) They must have the same tiling\n";
  std::cout << "\t\t" << "(regular/irregular) and the same tile/cell order.\n";
  std::cout << "\t\t" << "(ii) If the they have regular tiles, they must\n";
  std::cout << "\t\t" << "have the same tile extents across every dimension.\n";
  std::cout << "\t\t" << "(iii) They must have the same number, type and\n";
  std::cout << "\t\t" << "domain of dimensions.\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The resulting array has: (i) The same dimensions \n";
  std::cout << "\t\t" << "as the inputs, whose names are taken from the\n";
  std::cout << "\t\t" << "first input array. (ii) The union of the\n";
  std::cout << "\t\t" << "attributes of the input arrays, with the \n";
  std::cout << "\t\t" << "attributes of the first input array preceding\n";
  std::cout << "\t\t" << "those of the second. (iii) A cell if and only if\n";
  std::cout << "\t\t" << "there is a non-empty cell on the same coordinates\n";
  std::cout << "\t\t" << "in both the input arrays. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q join \n";
  std::cout << "\t\t" << "       { -A array_name_1, -A array_name_2        }\n";
  std::cout << "\t\t" << "       { -R result_name, -w workspace            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace must be\n";
  std::cout << "\t\t" << "given, and exactly two array names. The arrays \n";
  std::cout << "\t\t" << "must be already defined.\n";
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
  std::cout << "\t\t" << "array schema. A NULL value is represented by\n";
  std::cout << "\t\t" << "character '*'. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q load \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace,           }\n";
  std::cout << "\t\t" << "       { -f filename                            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, array\n";
  std::cout << "\t\t" << "name and file name must be given. You may include \n";
  std::cout << "\t\t" << "the file name into single quotes (').\n";
}

void print_nearest_neighbors() {
  std::cout << "\t" << "nearest_neighbors\n";
  std::cout << "\t\t" << "Creates a new array with the same schema as the\n";
  std::cout << "\t\t" << "input, containing only the N nearest (non-empty)\n";
  std::cout << "\t\t" << "cells of the input array to the reference cell\n";
  std::cout << "\t\t" << "given in the input. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q nearest_neighbors \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace            }\n";
  std::cout << "\t\t" << "       { -C coordinate, -N number               }\n";
  std::cout << "\t\t" << "       { -R result_name,                        }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, and array\n";
  std::cout << "\t\t" << "name must be given. A positive (non-zero) number N\n";
  std::cout << "\t\t" << "specifies the number of the nearest neighbors. The\n";
  std::cout << "\t\t" << "cell for which the nearest neighbors are computed\n";
  std::cout << "\t\t" << "is specified by a set of coordinates, whose number\n";
  std::cout << "\t\t" << "must be equal to the number of array dimensions.\n";
}

void print_retile() {
  std::cout << "\t" << "retile\n";
  std::cout << "\t\t" << "Retiles the input array. This involves switching\n";
  std::cout << "\t\t" << "from regular to irregular tiles (and vice versa),\n";
  std::cout << "\t\t" << "changing the tile extents, the capacity, and the\n";
  std::cout << "\t\t" << "tile/cell order. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q retile \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace            }\n";
  std::cout << "\t\t" << "       [ -o order, -c capacity                  ]\n";
  std::cout << "\t\t" << "       [ -e tile_extent                         ]\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, and array\n";
  std::cout << "\t\t" << "name must be given. If tile extents are provided\n";
  std::cout << "\t\t" << "(i) in the case of regular tiles, if the extents\n";
  std::cout << "\t\t" << "differ from those in the array schema, retiling\n";
  std::cout << "\t\t" << "occurs, (ii) in the case of irregular tiles, the\n";
  std::cout << "\t\t" << "array is retiled so that it has regular tiles.\n";
  std::cout << "\t\t" << "If tile extents are not provided for the case of\n"; 
  std::cout << "\t\t" << "regular tiles, the array is retiled to one with\n";
  std::cout << "\t\t" << "irregular tiles. If order is provided (different\n";
  std::cout << "\t\t" << "from the existing order) retiling occurs.\n";
  std::cout << "\t\t" << "If a capacity is provided, (i) in the case of\n";
  std::cout << "\t\t" << "regular tiles it has no effect (only the schema\n";
  std::cout << "\t\t" << "changes), (ii) in the case of irregular tiles,\n";
  std::cout << "\t\t" << "only the book-keeping structures and array schema\n";
  std::cout << "\t\t" << "are altered to accommodate the change.\n";
}

void print_subarray() {
  std::cout << "\t" << "subarray\n";
  std::cout << "\t\t" << "Creates an array that has the same schema as the\n";
  std::cout << "\t\t" << "input array, and contains only the cells that lie\n";
  std::cout << "\t\t" << "within the input range. Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q subarray \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace            }\n";
  std::cout << "\t\t" << "       { -r range_bound                         }\n";
  std::cout << "\t\t" << "       { -R result_name                         }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, and array\n";
  std::cout << "\t\t" << "name must be given. There must be a lower and an\n";
  std::cout << "\t\t" << "upper range bound across every dimension (given\n";
  std::cout << "\t\t" << "in the same order as the dimensions were defined).\n";
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
  std::cout << "\t\t" << "attribute values have a special DEL value,\n";
  std::cout << "\t\t" << "specified with character '$' in the CSV file.\n";
  std::cout << "\t\t" << "Syntax:\n\n";
  std::cout << "\t\t" << "tiledb -q update \n";
  std::cout << "\t\t" << "       { -A array_name, -w workspace,           }\n";
  std::cout << "\t\t" << "       { -f filename                            }\n";
  std::cout << "\t\t" << "\n";
  std::cout << "\t\t" << "The workspace is the folder where the array data\n";
  std::cout << "\t\t" << "are stored. A single existing workspace, array\n";
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
  print_clear_array();
  std::cout << "\t\t" << "\n";
  print_define_array();
  std::cout << "\t\t" << "\n";
  print_delete_array();
  std::cout << "\t\t" << "\n";
  print_export_to_csv();
  std::cout << "\t\t" << "\n";
  print_filter();
  std::cout << "\t\t" << "\n";
  print_join();
  std::cout << "\t\t" << "\n";
  print_load();
  std::cout << "\t\t" << "\n";
  print_nearest_neighbors();
  std::cout << "\t\t" << "\n";
  print_retile();
  std::cout << "\t\t" << "\n";
  print_subarray();
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
      if(!strcmp(argv[2],"clear_array")) { 
        print_clear_array();
      } else if(!strcmp(argv[2],"define_array")) { 
        print_define_array();
      } else if(!strcmp(argv[2],"delete_array")) { 
        print_delete_array();
      } else if(!strcmp(argv[2],"export_to_csv")) { 
        print_export_to_csv();
      } else if(!strcmp(argv[2],"filter")) { 
        print_filter();
      } else if(!strcmp(argv[2],"join")) { 
        print_join();
      } else if(!strcmp(argv[2],"load")) { 
        print_load();
      } else if(!strcmp(argv[2],"nearest_neighbors")) { 
        print_nearest_neighbors();
      } else if(!strcmp(argv[2],"retile")) { 
        print_retile();
      } else if(!strcmp(argv[2],"subarray")) { 
        print_subarray();
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
  } else if(!strcmp(cl.query_, "clear_array")) {
    // CLEAR_ARRAY
    parser.parse_clear_array(cl);
    try {
      Executor executor(cl.workspace_);
      executor.clear_array(cl.array_names_[0]);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "define_array")) {
    // DEFINE_ARRAY
    ArraySchema array_schema = parser.parse_define_array(cl);
    try {
      Executor executor(cl.workspace_);
      executor.define_array(&array_schema);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "delete_array")) {
    // DELETE_ARRAY
    parser.parse_delete_array(cl);
    try {
      Executor executor(cl.workspace_);
      executor.delete_array(cl.array_names_[0]);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "export_to_csv")) {
    // EXPORT_TO_CSV
    parser.parse_export_to_csv(cl);
    try {
      Executor executor(cl.workspace_);
      executor.export_to_csv(cl.array_names_[0], cl.filename_);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "filter")) {
    // FILTER
    parser.parse_filter(cl);
    try {
      Executor executor(cl.workspace_);
      executor.filter(cl.array_names_[0], cl.expression_, cl.result_name_);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "join")) {
    // JOIN
    parser.parse_join(cl);
    try {
      Executor executor(cl.workspace_);
      executor.join(cl.array_names_[0], cl.array_names_[1], cl.result_name_);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "load")) {
    // LOAD
    parser.parse_load(cl);
    try {
      Executor executor(cl.workspace_);
      executor.load(cl.filename_, cl.array_names_[0]);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "nearest_neighbors")) {
    // NEAREST_NEIGHBORS
    // Get pair (reference cell, number of nearest neighbors)
    std::pair<std::vector<double>,int64_t> NN = 
        parser.parse_nearest_neighbors(cl);
    try {
      Executor executor(cl.workspace_);
      executor.nearest_neighbors(cl.array_names_[0], NN.first, NN.second, 
                                 cl.result_name_);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "retile")) {
    // RETILE
    int64_t capacity;
    ArraySchema::CellOrder cell_order;
    std::vector<double> tile_extents; 
    parser.parse_retile(cl, capacity, cell_order, tile_extents);
    try {
      Executor executor(cl.workspace_);
      executor.retile(cl.array_names_[0], capacity, cell_order, tile_extents);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "subarray")) {
    // SUBARRAY
    std::vector<double> range = parser.parse_subarray(cl);
    try {
      Executor executor(cl.workspace_);
      executor.subarray(cl.array_names_[0], &range[0], cl.result_name_);
    } catch(ExecutorException& ee) {
      std::cerr << "[TileDB::fatal_error] " << ee.what() << "\n";
      exit(-1);
    }
  } else if(!strcmp(cl.query_, "update")) {
    // UPDATE
    parser.parse_update(cl);
    try {
      Executor executor(cl.workspace_);
      executor.update(cl.filename_, cl.array_names_[0]);
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
