/**
 * @file   command_line.cc
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
 * This file implements the CommandLine class.
 */

#include "command_line.h"
#include <iostream>

CommandLine::CommandLine() {
  arg_bitmap_ = 0;
  capacity_ = NULL;
  cell_order_ = NULL;
  consolidation_step_ = NULL;
  expression_ = NULL;
  filename_ = NULL;
  query_ = NULL;
  result_name_ = NULL;
  tile_order_ = NULL;
  workspace_ = NULL;
}

void CommandLine::parse(int argc, char** argv) {
  static struct option long_options[] = 
  {
    {"array-name",1,0,'A'},
    {"attribute-name",1,0,'a'},
    {"capacity",1,0,'c'},
    {"coordinate",1,0,'C'},
    {"dim-domain-bound",1,0,'D'},
    {"dim-name",1,0,'d'},
    {"expression",1,0,'E'},
    {"tile-extent",1,0,'e'},
    {"filename",1,0,'f'},
    {"query",1,0,'q'},
    {"number",1,0,'N'},
    {"cell-order",1,0,'o'},
    {"tile-order",1,0,'O'},
    {"range-bound",1,0,'r'},
    {"result-name",1,0,'R'},
    {"consolidation-step",1,0,'s'},
    {"type",1,0,'t'},
    {"workspace",1,0,'w'},
    {0,0,0,0},
  };

  const char* short_options = "A:a:c:C:D:d:E:e:f:q:N:o:O:r:R:s:t:w:";

  int c;
  option_num_ = 0;
  while((c=getopt_long(argc, argv, short_options, long_options, NULL)) >= 0) {
    ++option_num_;
    switch(c) {
      case 'A':
        arg_bitmap_ |= CL_ARRAY_NAME_BITMAP;
        array_names_.push_back(optarg);
        break;
      case 'a':
        arg_bitmap_ |= CL_ATTRIBUTE_NAME_BITMAP;
        attribute_names_.push_back(optarg);
        break;
      case 'C':
        arg_bitmap_ |= CL_COORDINATE_BITMAP;
        coords_.push_back(optarg);
        break;
      case 'c':
        if(capacity_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one capacities"
                    << " provided."
                    << " Type 'tiledb help' to see the TileDB User Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_CAPACITY_BITMAP;
        capacity_ = optarg;
        break;
      case 'D':
        arg_bitmap_ |= CL_DIM_DOMAIN_BITMAP;
        dim_domains_.push_back(optarg);
        break;
      case 'd':
        arg_bitmap_ |= CL_DIM_NAME_BITMAP;
        dim_names_.push_back(optarg);
        break;
      case 'E':
        if(expression_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one expressions"
                    << " provided."
                    << " Type 'tiledb help' to see the TileDB User Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_EXPRESSION_BITMAP;
        expression_ = optarg;
        break;
      case 'e':
        arg_bitmap_ |= CL_TILE_EXTENT_BITMAP;
        tile_extents_.push_back(optarg);
        break;
      case 'f':
        if(filename_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one filenames"
                    << " provided."
                    << " Type 'tiledb help' to see the TileDB User Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_FILENAME_BITMAP;
        filename_ = optarg;
        break;
      case 'N':
        arg_bitmap_ |= CL_NUMBER_BITMAP;
        numbers_.push_back(optarg);
        break;
      case 'o':
        if(cell_order_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one cell orders"
                    << " provided. Type 'tiledb help' to see the TileDB User"
                    << " Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_CELL_ORDER_BITMAP;
        cell_order_ = optarg;
        break;
      case 'O':
        if(tile_order_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one tile orders"
                    << " provided. Type 'tiledb help' to see the TileDB User"
                    << " Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_TILE_ORDER_BITMAP;
        tile_order_ = optarg;
        break;
      case 'q':
        if(query_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one queries provided."
                    << " Type 'tiledb help' to see the TileDB User Manual.\n";
          exit(-1);
        }
        query_ = optarg;
        break;
      case 'r':
        arg_bitmap_ |= CL_RANGE_BITMAP;
        range_.push_back(optarg);
        break;
      case 'R':
        if(result_name_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one result names"
                    << " provided."
                    << " Type 'tiledb help' to see the TileDB User Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_RESULT_BITMAP;
        result_name_ = optarg;
        break;
      case 's':
        if(consolidation_step_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one consolidation"
                    << " steps provided. Type 'tiledb help' to see the TileDB"
                    << " User Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_CONSOLIDATION_STEP_BITMAP;
        consolidation_step_ = optarg;
        break;
      case 't':
        arg_bitmap_ |= CL_TYPE_BITMAP;
        types_.push_back(optarg);
        break;
      case 'w':
        if(workspace_ != NULL) {
          std::cerr << "[TileDB::fatal_error] More than one workspaces" 
                    << " provided."
                    << " Type 'tiledb help' to see the TileDB User Manual.\n";
          exit(-1);
        }
        arg_bitmap_ |= CL_WORKSPACE_BITMAP;
        workspace_ = optarg;
        break;
      default:
        std::cerr << "[TileDB::fatal_error] Unknown option."
                  << " Type 'tiledb help' to see the TileDB User Manual.\n";
        exit(-1);
        break;
    }
  }
}
