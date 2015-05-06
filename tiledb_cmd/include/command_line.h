/**
 * @file   command_line.h
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
 * This file defines class CommandLine, used to receive queries and options from
 * the command line.
 */

#ifndef COMMAND_LINE_H
#define COMMAND_LINE_H

#include <vector>
#include <stdlib.h>
#include <getopt.h>
#include <inttypes.h>

/** Indicates array name was given as an argument. */
#define CL_ARRAY_NAME_BITMAP 0x1
/** Indicates attribute name was given as an argument. */
#define CL_ATTRIBUTE_NAME_BITMAP 0x2
/** Indicates capacity was given as an argument. */
#define CL_CAPACITY_BITMAP 0x4
/** Indicates cell order was given as an argument. */
#define CL_CELL_ORDER_BITMAP 0x8
/** Indicates consolidation step was given as an argument. */
#define CL_CONSOLIDATION_STEP_BITMAP 0x10
/** Indicates coordinate was given as an argument. */
#define CL_COORDINATE_BITMAP 0x20
/** Indicates dimension domain was given as an argument. */
#define CL_DIM_DOMAIN_BITMAP 0x40
/** Indicates dimension name was given as an argument. */
#define CL_DIM_NAME_BITMAP 0x80
/** Indicates expression was given as an argument. */
#define CL_EXPRESSION_BITMAP 0x100
/** Indicates range bound was given as an argument. */
#define CL_FILENAME_BITMAP 0x200
/** Indicates number was given as an argument. */
#define CL_NUMBER_BITMAP 0x400
/** Indicates range bound was given as an argument. */
#define CL_RANGE_BITMAP 0x800
/** Indicates result name was given as an argument. */
#define CL_RESULT_BITMAP 0x1000
/** Indicates tile extent was given as an argument. */
#define CL_TILE_EXTENT_BITMAP 0x2000
/** Indicates tile order was given as an argument. */
#define CL_TILE_ORDER_BITMAP 0x4000
/** Indicates type was given as an argument. */
#define CL_TYPE_BITMAP 0x8000
/** Indicates worspace was given as an argument. */
#define CL_WORKSPACE_BITMAP 0x10000

/** This class stores the command line options given by the user. */
class CommandLine {
 public:
  // CONSTRUCTORS
  CommandLine(); 

  // PARSING METHODS
  /** Parses the command line. */
  void parse(int argc, char** argv);

  /** 
   * This bitmap is used to check which arguments are provided. 
   * From right to left, each bit corresponds to:
   * CommandLine::array_names_,
   * CommandLine::attribute_names_,
   * CommandLine::capacity_,
   * CommandLine::cell_order_,
   * CommandLine::consolidation_step_,
   * CommandLine::coordinate_,
   * CommandLine::dim_domains_,
   * CommandLine::dim_names_,
   * CommandLine::expression_,
   * CommandLine::filename_.
   * CommandLine::number_.
   * CommandLine::range_.
   * CommandLine::result_name_.
   * CommandLine::tile_extents_,
   * CommandLine::tile_order_,
   * CommandLine::types_,
   * CommandLine::workspace_,
   */
  uint64_t arg_bitmap_;
  /** Array names. */
  std::vector<char*> array_names_;
  /** Attribute names. */
  std::vector<char*> attribute_names_;
  /** Capacity. */
  char* capacity_;
  /** Cell order */
  char* cell_order_;
  /** Consolidation step */
  char* consolidation_step_;
  /** Coordinates. */
  std::vector<char*> coords_;
  /** Dimension names. */
  std::vector<char*> dim_names_;
  /** Dimension domains. */
  std::vector<char*> dim_domains_;
  /** Expression. */
  char* expression_;
  /** File name. */
  char* filename_;
  /** Numbers. */
  std::vector<char*> numbers_;
  /** Number of (option,value) pairs in the command line. */
  int option_num_;
  /** The query. */
  char* query_;
  /** The result name. */
  char* result_name_;
  /** A multidimensional range */
  std::vector<char*> range_;
  /** Tile extents. */
  std::vector<char*> tile_extents_;
  /** Tile order */
  char* tile_order_;
  /** Types.  */
  std::vector<char*> types_;
  /** The workspace where the query will be executed. */
  char* workspace_;

  // TODO segment_size
  // TODO other configuration parameters?
  // TODO perhaps get parameters from config file?
};

#endif
