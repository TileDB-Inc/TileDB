/**
 * @file   tiledb.cc
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
 * This file implements the main TileDB program.
 */

#include "tiledb.h"
#include "utils.h"
#include <getopt.h>
#include <algorithm>
#include <iostream>
#include <set>
#include <string>
#include <cstring>
#include <vector>

/* ************************ */
/*          MACROS          */
/* ************************ */
#define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n"
#define PRINT(x) std::cout << "[TileDB] " << x << ".\n"

/* ************************ */
/*       OPTION CODES       */
/* ************************ */
#define OPT_ARRAY                  2
#define OPT_ATTRIBUTES             3
#define OPT_CAPACITY               4
#define OPT_CELL_NUM               5
#define OPT_CELL_ORDER             6
#define OPT_CLEAR                  7 
#define OPT_COMPRESSION            8
#define OPT_CONSOLIDATE            9
#define OPT_CONSOLIDATION_STEP    10
#define OPT_CREATE                11
#define OPT_DELETE                12
#define OPT_DELIMITER             13
#define OPT_DENSE                 14
#define OPT_DIMENSIONS            15
#define OPT_DOMAIN                16
#define OPT_FORMAT                17
#define OPT_GENERATE              18
#define OPT_GROUP                 19
#define OPT_IN                    20
#define OPT_KEY                   21
#define OPT_LIST                  22
#define OPT_MASTER_CATALOG        23
#define OPT_METADATA              24
#define OPT_MOVE                  25
#define OPT_OUT                   26
#define OPT_PRECISION             27
#define OPT_RANGE                 28
#define OPT_READ                  29
#define OPT_STAT                  30
#define OPT_TILE_EXTENTS          31
#define OPT_TILE_ORDER            32
#define OPT_TYPES                 33
#define OPT_VERSION               34
#define OPT_WORKSPACE             35
#define OPT_WRITE                 36
#define OPT_ERROR                 63 // '?' 

/* ************************ */
/*   TILEDB OPTIONS STRUCT  */
/* ************************ */

struct TileDBOptions {
  int query_;
  std::vector<std::string> array_;
  std::vector<std::string> attributes_;
  int64_t capacity_;
  int64_t cell_num_;
  std::string cell_order_;
  std::vector<std::string> compression_;
  bool consolidate_;
  int consolidation_step_;
  char delimiter_;
  bool dense_;
  std::vector<std::string> dimensions_;
  std::vector<double> domain_;
  std::string format_;
  std::vector<std::string> group_;
  std::vector<std::string> in_;
  std::string key_;
  std::string list_;
  std::vector<std::string> master_catalog_;
  std::vector<std::string> metadata_;
  std::set<int> opt_codes_;
  std::string out_;
  int precision_;
  std::vector<double> range_;
  std::vector<double> tile_extents_;
  std::string tile_order_;
  std::vector<std::string> types_;
  std::vector<std::string> value_;
  std::vector<std::string> workspace_;
};

/* ************************ */
/*   COMMAND LINE OPTIONS   */
/* ************************ */

// Long options
static struct option long_options[] = 
{
  {"array",required_argument,NULL,OPT_ARRAY},
  {"attributes",required_argument,NULL,OPT_ATTRIBUTES},
  {"capacity",required_argument,NULL,OPT_CAPACITY},
  {"cell-num",required_argument,NULL,OPT_CELL_NUM},
  {"cell-order",required_argument,NULL,OPT_CELL_ORDER},
  {"clear",no_argument,NULL,OPT_CLEAR},
  {"compression",required_argument,NULL,OPT_COMPRESSION},
  {"consolidate",no_argument,NULL,OPT_CONSOLIDATE},
  {"consolidation-step",required_argument,NULL,OPT_CONSOLIDATION_STEP},
  {"create",no_argument,NULL,OPT_CREATE},
  {"delete",no_argument,NULL,OPT_DELETE},
  {"delimiter",required_argument,NULL,OPT_DELIMITER},
  {"dense",no_argument,NULL,OPT_DENSE},
  {"dimensions",required_argument,NULL,OPT_DIMENSIONS},
  {"domain",required_argument,NULL,OPT_DOMAIN},
  {"format",required_argument,NULL,OPT_FORMAT},
  {"generate",no_argument,NULL,OPT_GENERATE},
  {"group",required_argument,NULL,OPT_GROUP},
  {"in",required_argument,NULL,OPT_IN},
  {"key",required_argument,NULL,OPT_KEY},
  {"list",required_argument,NULL,OPT_LIST},
  {"master-catalog",required_argument,NULL,OPT_MASTER_CATALOG},
  {"metadata",required_argument,NULL,OPT_METADATA},
  {"move",no_argument,NULL,OPT_MOVE},
  {"out",required_argument,NULL,OPT_OUT},
  {"precision",required_argument,NULL,OPT_PRECISION},
  {"range",required_argument,NULL,OPT_RANGE},
  {"read",no_argument,NULL,OPT_READ},
  {"stat",optional_argument,NULL,OPT_STAT},
  {"tile-extents",required_argument,NULL,OPT_TILE_EXTENTS},
  {"tile-order",required_argument,NULL,OPT_TILE_ORDER},
  {"types",required_argument,NULL,OPT_TYPES},
  {"version",no_argument,NULL,OPT_VERSION},
  {"workspace",required_argument,NULL,OPT_WORKSPACE},
  {"write",no_argument,NULL,OPT_WRITE},
  {0,0,0,0},
};

// Define short options
const char* short_options = "-";

/* ************************ */
/*   FORWARD DECLARATIONS   */
/* ************************ */

// Execution functions
int execute(TileDBOptions& tiledb_options);

int execute_array_clear(TileDBOptions& tiledb_options);
int execute_array_create(TileDBOptions& tiledb_options);
int execute_array_delete(TileDBOptions& tiledb_options);
int execute_array_move(TileDBOptions& tiledb_options);
int execute_array_read(TileDBOptions& tiledb_options);
int execute_array_write(TileDBOptions& tiledb_options);

int execute_group_clear(TileDBOptions& tiledb_options);
int execute_group_create(TileDBOptions& tiledb_options);
int execute_group_delete(TileDBOptions& tiledb_options);
int execute_group_move(TileDBOptions& tiledb_options);

int execute_workspace_clear(TileDBOptions& tiledb_options);
int execute_workspace_create(TileDBOptions& tiledb_options);
int execute_workspace_delete(TileDBOptions& tiledb_options);
int execute_workspace_move(TileDBOptions& tiledb_options);

int execute_master_catalog_delete(TileDBOptions& tiledb_options);
int execute_master_catalog_move(TileDBOptions& tiledb_options);

int execute_metadata_clear(TileDBOptions& tiledb_options);
int execute_metadata_create(TileDBOptions& tiledb_options);
int execute_metadata_delete(TileDBOptions& tiledb_options);
int execute_metadata_move(TileDBOptions& tiledb_options);
int execute_metadata_read(TileDBOptions& tiledb_options);
int execute_metadata_write(TileDBOptions& tiledb_options);

int execute_list(TileDBOptions& tiledb_options);

// Parsing functions
int parse_command_line(
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

std::string parse_error(
    const std::string& opt_name, 
    int indexptr);

int parse_query_option( 
    int query, 
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_array(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_attributes(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_capacity(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_cell_num(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_cell_order(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_compression(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_consolidate(
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_consolidation_step(
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_delimiter(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_dense(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_dimensions(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_domain(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_format(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_group(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_in(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_key(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_list(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_master_catalog(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_metadata(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_out(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_precision(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_range(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_tile_extents(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_tile_order(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_types(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

int parse_option_workspace(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options);

// Printing functions
void print_usage();
int print_version(TileDBOptions& tiledb_options);

// Misc
bool opt_code_exists(
    int opt_code,
    const TileDBOptions& tiledb_options);

bool opt_codes_are_correct(
    const std::set<int>& required_opt_codes,
    const std::set<int>& optional_opt_codes,
    const TileDBOptions& tiledb_options);

/* ************************ */
/*           MAIN           */
/* ************************ */

int main(int argc, char** argv) {
  // Print usage
  if(argc == 1) {
    print_usage();
    return 0;
  }

  // TileDB options
  TileDBOptions tiledb_options = {};

  // Parse command line
  if(parse_command_line(argc, argv, tiledb_options))
    return -1; 

  // Execute query 
  if(execute(tiledb_options))
    return -1;

  // Success 
  return 0;
}

/* ************************ */
/*      PARSING FUNCTIONS   */
/* ************************ */

int parse_command_line(
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // No query seen yet
  tiledb_options.query_ = -1; // no query seen yet

  // getopt should not report error messages
  opterr = 0; 

  // Get options
  int c, indexptr;
  while((c=getopt_long(argc, argv, short_options, 
                       long_options, &indexptr)) >= 0) {
    // Check if option has been seen before
    if(tiledb_options.opt_codes_.find(c) != tiledb_options.opt_codes_.end()) {
      PRINT_ERROR(std::string("Duplicate option '") + 
                  long_options[indexptr].name + "'");
      return -1;
    }
    tiledb_options.opt_codes_.insert(c);
 
    // Parse option
    switch(c) {
     case OPT_ERROR: 
       PRINT_ERROR(parse_error(argv[optind-1], indexptr));
       return -1;  
       break;
     case OPT_ARRAY: 
       if(parse_option_array(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_ATTRIBUTES: 
       if(parse_option_attributes(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_CAPACITY: 
       if(parse_option_capacity(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_CELL_NUM: 
       if(parse_option_cell_num(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_CELL_ORDER: 
       if(parse_option_cell_order(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_CLEAR: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_COMPRESSION: 
       if(parse_option_compression(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_CONSOLIDATE: 
       if(parse_option_consolidate(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_CONSOLIDATION_STEP: 
       if(parse_option_consolidation_step(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_CREATE: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_DELETE: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_DELIMITER: 
       if(parse_option_delimiter(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_DENSE: 
       if(parse_option_dense(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_DIMENSIONS: 
       if(parse_option_dimensions(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_DOMAIN: 
       if(parse_option_domain(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_FORMAT: 
       if(parse_option_format(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_GENERATE: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_GROUP: 
       if(parse_option_group(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_IN: 
       if(parse_option_in(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_KEY: 
       if(parse_option_key(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_LIST: 
       if(parse_option_list(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_MASTER_CATALOG: 
       if(parse_option_master_catalog(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_METADATA: 
       if(parse_option_metadata(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_MOVE: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_OUT: 
       if(parse_option_out(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_PRECISION: 
       if(parse_option_precision(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_RANGE: 
       if(parse_option_range(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_READ: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_STAT: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_TILE_EXTENTS: 
       if(parse_option_tile_extents(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_TILE_ORDER: 
       if(parse_option_tile_order(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_TYPES: 
       if(parse_option_types(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_VERSION: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_WORKSPACE: 
       if(parse_option_workspace(argc, argv, tiledb_options))
         return -1;
       break;
     case OPT_WRITE: 
       if(parse_query_option(c, argc, argv, tiledb_options))
         return -1;
       break;
    }
  }

  // Success
  return 0;
}

std::string parse_error(const std::string& opt_name, int indexptr) {
  // Error message
  std::string err_msg;
 
  // Find matching options
  std::vector<std::string> matching;
  std::string dashes = "--";
  for(int i=0; long_options[i].name != 0; ++i) {
    if(starts_with(dashes + long_options[i].name, opt_name))
      matching.push_back(dashes + long_options[i].name);
  }

  // Construct error message
  if(matching.size() == 0) {
    err_msg = std::string("Unknown option '" + opt_name + "'");
  } else if(matching.size() > 1) {
    err_msg = std::string("Ambiguous option '") + 
              opt_name + "'; possibilities:";
    for(int i=0; i<int(matching.size()); ++i) 
      err_msg += std::string(" '") + matching[i] + "'";
  } else if(optarg == NULL) {
    err_msg = std::string("Option '") + opt_name  + "' expects an argument";
  } else {
    err_msg = std::string("Unknown error for option '") + opt_name  + "'";
  }
 
  // Return error message
  return err_msg;  
}

int parse_query_option(    
    int query, int argc, char** argv, TileDBOptions& tiledb_options) {
  // Check if another query has been given
  if(tiledb_options.query_ != -1) {
    PRINT_ERROR("Duplicate queries specified");
    return -1;
  }

  // Make sure no argument follows the option
  if(optind < argc && *argv[optind] != '-') {
    PRINT_ERROR(std::string("Option '") + argv[optind-1] + 
                "' takes no argument");
    return -1;
  }

  // Set query
  tiledb_options.query_ = query;

  // Success 
  return 0;
}

int parse_option_array(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set array
  tiledb_options.array_ = args; 

  // Success 
  return 0;
}

int parse_option_attributes(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.attributes_ = args; 

  // Success 
  return 0;
}

int parse_option_capacity(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--capacity' expects a single argument"));
    return -1;
  }

  // Convert to integer
  int64_t capacity;
  if(args.size() == 1) { // Default capacity
    capacity = 0;
  } else if(!is_positive_integer(optarg)) {
    PRINT_ERROR("The capacity must be a positive integer");
    return -1;
  }
  sscanf(optarg, "%lld", &capacity);

  // Set metadata
  tiledb_options.capacity_ = capacity; 

  // Success 
  return 0;
}

int parse_option_cell_num(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--cell-num' expects a single argument"));
    return -1;
  }

  // Convert to integer
  int64_t cell_num;
  if(args.size() == 1) { // Default capacity
    cell_num = 0;
  } else if(!is_positive_integer(optarg)) {
    PRINT_ERROR("The number of cells must be a positive integer");
    return -1;
  }
  sscanf(optarg, "%lld", &cell_num);

  // Set metadata
  tiledb_options.cell_num_ = cell_num; 

  // Success 
  return 0;
}

int parse_option_cell_order(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--cell-order' expects a single argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.cell_order_ = optarg; 

  // Success 
  return 0;
}

int parse_option_compression(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.compression_ = args; 

  // Success 
  return 0;
}

int parse_option_consolidate(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 0) {
    PRINT_ERROR(std::string("Option '--consolidate' does not take any argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.consolidate_ = true; 

  // Success 
  return 0;
}

int parse_option_consolidation_step(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--consolidation_step' expects a single "
                            "argument"));
    return -1;
  }

  // Convert to integer
  int consolidation_step;
  if(args.size() == 1) { // Default capacity
    consolidation_step = 0;
  } else if(!is_positive_integer(optarg)) {
    PRINT_ERROR("The consolidation_step must be a positive integer");
    return -1;
  }
  sscanf(optarg, "%d", &consolidation_step);

  // Set metadata
  tiledb_options.consolidation_step_ = consolidation_step; 

  // Success 
  return 0;
}

int parse_option_delimiter(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--delimiter' expects a single argument"));
    return -1;
  }

  // Error if delimiter is not a single character
  if(strlen(optarg) != 1) {
    PRINT_ERROR("Option '--delimiter' expects a single character as argument");
    return -1;
  }

  // Set metadata
  tiledb_options.delimiter_ = optarg[0]; 

  // Success 
  return 0;
}

int parse_option_dense(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 0) {
    PRINT_ERROR(std::string("Option '--dense' does not take any argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.dense_ = true; 

  // Success 
  return 0;
}

int parse_option_dimensions(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.dimensions_ = args; 

  // Success 
  return 0;
}

int parse_option_domain(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Convert to double
  std::vector<double> domain;
  double domain_bound;
  for(int i=0; i<int(args.size()); ++i) {
    if(!is_real(args[i].c_str())) {
      PRINT_ERROR("The domain bounds must be real numbers");
      return -1;
    }
    sscanf(args[i].c_str(), "%lf", &domain_bound);
    domain.push_back(domain_bound);
  }

  // Set metadata
  tiledb_options.domain_ = domain; 

  // Success 
  return 0;
}

int parse_option_format(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--format' expects a single argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.format_ = optarg; 

  // Success 
  return 0;
}

int parse_option_group(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.group_ = args; 

  // Success 
  return 0;
}

int parse_option_in(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.in_ = args; 

  // Success 
  return 0;
}

int parse_option_key(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--key' expects a single argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.key_ = optarg; 

  // Success 
  return 0;
}

int parse_option_list(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--list' expects a single argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.list_ = optarg; 

  // Success 
  return 0;
}

int parse_option_master_catalog(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set master catalog
  tiledb_options.master_catalog_ = args; 

  // Success 
  return 0;
}

int parse_option_metadata(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.metadata_ = args; 

  // Success 
  return 0;
}

int parse_option_out(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--out' expects a single argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.out_ = optarg; 

  // Success 
  return 0;
}

int parse_option_precision(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--precision' expects a single argument"));
    return -1;
  }

  // Convert to integer
  int precision;
  if(args.size() == 1) { // Default capacity
    precision = 0;
  } else if(!is_positive_integer(optarg)) {
    PRINT_ERROR("The precision must be a positive integer");
    return -1;
  }
  sscanf(optarg, "%d", &precision);

  // Set metadata
  tiledb_options.precision_ = precision; 

  // Success 
  return 0;
}

int parse_option_range(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Convert to double
  std::vector<double> range;
  double range_bound;
  for(int i=0; i<int(args.size()); ++i) {
    if(!is_real(args[i].c_str())) {
      PRINT_ERROR("The range bounds must be real numbers");
      return -1;
    }
    sscanf(args[i].c_str(), "%lf", &range_bound);
    range.push_back(range_bound);
  }

  // Set metadata
  tiledb_options.range_ = range; 

  // Success 
  return 0;
}

int parse_option_tile_order(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Error if more than one arguments
  if(args.size() != 1) {
    PRINT_ERROR(std::string("Option '--tile-order' expects a single argument"));
    return -1;
  }

  // Set metadata
  tiledb_options.tile_order_ = optarg; 

  // Success 
  return 0;
}

int parse_option_tile_extents(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Convert to double
  std::vector<double> tile_extents;
  double tile_extent;
  for(int i=0; i<int(args.size()); ++i) {
    if(!is_real(args[i].c_str())) {
      PRINT_ERROR("The tile extents must be real numbers");
      return -1;
    }
    sscanf(args[i].c_str(), "%lf", &tile_extent);
    tile_extents.push_back(tile_extent);
  }

  // Set metadata
  tiledb_options.tile_extents_ = tile_extents; 

  // Success 
  return 0;
}

int parse_option_types(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.types_ = args; 

  // Success 
  return 0;
}

int parse_option_workspace(    
    int argc, 
    char** argv, 
    TileDBOptions& tiledb_options) {
  // Get arguments
  --optind;
  std::vector<std::string> args;
  for(; optind < argc && *argv[optind] != '-'; ++optind)
    args.push_back(argv[optind]);

  // Set metadata
  tiledb_options.workspace_ = args; 

  // Success 
  return 0;
}

/* ************************ */
/*    EXECUTION FUNCTIONS   */
/* ************************ */

int execute(TileDBOptions& tiledb_options) {
  // Return code
  int rc;
  if(tiledb_options.query_ == OPT_CLEAR) {
    // CLEAR
    if(opt_code_exists(OPT_ARRAY, tiledb_options)) {
      // CLEAR ARRAY
      rc = execute_array_clear(tiledb_options);
    } else if(opt_code_exists(OPT_GROUP, tiledb_options)) {
      // CLEAR GROUP
      rc = execute_group_clear(tiledb_options);
    } else if(opt_code_exists(OPT_METADATA, tiledb_options)) {
      // CLEAR METADATA
      rc = execute_metadata_clear(tiledb_options);
    } else if(opt_code_exists(OPT_WORKSPACE, tiledb_options)) {
      // CLEAR WORKSPACE
      rc = execute_workspace_clear(tiledb_options);
    } else {
      PRINT_ERROR("Incorrect usage of query '--clear'");
      return -1;
    }
  } else if(tiledb_options.query_ == OPT_CREATE) {
    // CREATE
    if(opt_code_exists(OPT_ARRAY, tiledb_options)) {
      // CREATE ARRAY
      rc = execute_array_create(tiledb_options);
    } else if(opt_code_exists(OPT_GROUP, tiledb_options)) {
      // CREATE GROUP
      rc = execute_group_create(tiledb_options);
    } else if(opt_code_exists(OPT_METADATA, tiledb_options)) {
      // CREATE METADATA
      rc = execute_metadata_create(tiledb_options);
    } else if(opt_code_exists(OPT_WORKSPACE, tiledb_options)) {
      // CREATE WORKSPACE
      rc = execute_workspace_create(tiledb_options);
    } else {
      PRINT_ERROR("Incorrect usage of query '--create'");
      return -1;
    }
  } else if(tiledb_options.query_ == OPT_DELETE) {
    // DELETE
    if(opt_code_exists(OPT_ARRAY, tiledb_options)) {
      // DELETE ARRAY
      rc = execute_array_delete(tiledb_options);
    } else if(opt_code_exists(OPT_GROUP, tiledb_options)) {
      // DELETE GROUP
      rc = execute_group_delete(tiledb_options);
    } else if(opt_code_exists(OPT_METADATA, tiledb_options)) {
      // DELETE METADATA
      rc = execute_metadata_delete(tiledb_options);
    } else if(opt_code_exists(OPT_WORKSPACE, tiledb_options)) {
      // DELETE WORKSPACE
      rc = execute_workspace_delete(tiledb_options);
    } else if(opt_code_exists(OPT_MASTER_CATALOG, tiledb_options)) {
      // DELETE MASTER CATALOG
      rc = execute_master_catalog_delete(tiledb_options);
    } else {
      PRINT_ERROR("Incorrect usage of query '--delete'");
      return -1;
    }
  } else if(tiledb_options.list_ != "") {
    // LIST
    rc = execute_list(tiledb_options);
  } else if(tiledb_options.query_ == OPT_MOVE) {
    // MOVE
    if(opt_code_exists(OPT_ARRAY, tiledb_options)) {
      // MOVE ARRAY
      rc = execute_array_move(tiledb_options);
    } else if(opt_code_exists(OPT_GROUP, tiledb_options)) {
      // MOVE GROUP
      rc = execute_group_move(tiledb_options);
    } else if(opt_code_exists(OPT_METADATA, tiledb_options)) {
      // MOVE METADATA
      rc = execute_metadata_move(tiledb_options);
    } else if(opt_code_exists(OPT_WORKSPACE, tiledb_options)) {
      // MOVE WORKSPACE
      rc = execute_workspace_move(tiledb_options);
    } else if(opt_code_exists(OPT_MASTER_CATALOG, tiledb_options)) {
      // MOVE MASTER CATALOG
      rc = execute_master_catalog_move(tiledb_options);
    } else {
      PRINT_ERROR("Incorrect usage of query '--move'");
      return -1;
    }
  } else if(tiledb_options.query_ == OPT_READ) {
    // READ
    if(opt_code_exists(OPT_ARRAY, tiledb_options)) {
      // READ ARRAY
      rc = execute_array_read(tiledb_options);
    } else if(opt_code_exists(OPT_METADATA, tiledb_options)) {
      // READ METADATA
      rc = execute_metadata_read(tiledb_options);
    } else {
      PRINT_ERROR("Incorrect usage of query '--read'");
      return -1;
    }
  } else if(tiledb_options.query_ == OPT_WRITE) {
    // WRITE
    if(opt_code_exists(OPT_ARRAY, tiledb_options)) {
      // WRITE ARRAY
      rc = execute_array_write(tiledb_options);
    } else if(opt_code_exists(OPT_METADATA, tiledb_options)) {
      // WRITE METADATA
      rc = execute_metadata_write(tiledb_options);
    } else {
      PRINT_ERROR("Incorrect usage of query '--write'");
      return -1;
    }
  } else if(tiledb_options.query_ == OPT_VERSION) {
    // VERSION
    rc = print_version(tiledb_options);
  } else { 
    // Error
    PRINT_ERROR("No query is given");
    return -1;
  } 

  // 0 for success and -1 for error
  return rc;
}

int execute_array_clear(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CLEAR); 
  required_opt_codes.insert(OPT_ARRAY); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check array
  if(tiledb_options.array_.size() != 1) {
    PRINT_ERROR(std::string("Option '--array' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_array_clear(
      tiledb_ctx, 
      tiledb_options.array_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_array_create(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CREATE); 
  required_opt_codes.insert(OPT_ARRAY); 
  required_opt_codes.insert(OPT_ATTRIBUTES); 
  required_opt_codes.insert(OPT_DIMENSIONS); 
  required_opt_codes.insert(OPT_DOMAIN); 
  required_opt_codes.insert(OPT_TYPES); 
  std::set<int> optional_opt_codes;
  optional_opt_codes.insert(OPT_CAPACITY);
  optional_opt_codes.insert(OPT_CELL_ORDER);
  optional_opt_codes.insert(OPT_COMPRESSION);
  optional_opt_codes.insert(OPT_CONSOLIDATION_STEP);
  optional_opt_codes.insert(OPT_DENSE);
  optional_opt_codes.insert(OPT_TILE_EXTENTS);
  optional_opt_codes.insert(OPT_TILE_ORDER);
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check array
  if(tiledb_options.array_.size() != 1) {
    PRINT_ERROR(std::string("Option '--array' expects a single argument"));
    return -1;
  }

  // Check that the number of attributes given plus 1 (for the coordinates) is 
  // the same as the number of types
  if(tiledb_options.attributes_.size()+1 != tiledb_options.types_.size()) {
    PRINT_ERROR("The number of types must be the same as the number of " 
                "attributes plus 1 (last type is for the coordinates)");
    return -1;
  }

  // Check dimension number and domain
  if(2*tiledb_options.dimensions_.size() != tiledb_options.domain_.size()) {
    PRINT_ERROR("The number of domain bounds must be twice the number "
                "of dimensions");
    return -1;
  }

  // Check tile extents
  if(tiledb_options.tile_extents_.size() != 0 &&
     tiledb_options.dimensions_.size() != 
     tiledb_options.tile_extents_.size()) {
    PRINT_ERROR("The number of tile extents must be the same as the number "
                "of dimensions");
    return -1;
  }

  // Check compression
  if(tiledb_options.compression_.size() != 0 &&
     tiledb_options.compression_.size() != 
     tiledb_options.attributes_.size() + 1) {
    PRINT_ERROR("The number of compression types must equal to the number "
                "of attributes plus 1 (last type is for the coordinates)");
    return -1;
  }

  // Check dense
  if(tiledb_options.dense_ && tiledb_options.tile_extents_.size() == 0) {
    PRINT_ERROR("Dense arrays should have regular tiles");
    return -1;
  }

  // Create TileDB_ArraySchema struct 
  TileDB_ArraySchema array_schema = {};
  // --- array name ---
  array_schema.array_name_ = tiledb_options.array_[0].c_str();
  // --- attributes --- 
  array_schema.attribute_num_ = tiledb_options.attributes_.size();
  array_schema.attributes_ = new const char*[array_schema.attribute_num_];
  for(int i=0; i<array_schema.attribute_num_; ++i) 
    array_schema.attributes_[i] = tiledb_options.attributes_[i].c_str();
  // --- capacity ---
  array_schema.capacity_ = tiledb_options.capacity_;
  // --- cell order ---
  if(tiledb_options.cell_order_ != "")
    array_schema.cell_order_ = tiledb_options.cell_order_.c_str();
  // ---- compression ---
  if(tiledb_options.compression_.size() != 0) {
    array_schema.compression_ = new const char*[array_schema.attribute_num_+1];
    for(int i=0; i<array_schema.attribute_num_+1; ++i) 
      array_schema.compression_[i] = tiledb_options.compression_[i].c_str();
  }
  // --- consolidation step ---
  array_schema.consolidation_step_ = tiledb_options.consolidation_step_;
  // --- dense ---
  array_schema.dense_ = tiledb_options.dense_;
  // --- dimensions ---
  array_schema.dim_num_ = tiledb_options.dimensions_.size();
  array_schema.dimensions_ = new const char*[array_schema.dim_num_];
  for(int i=0; i<array_schema.dim_num_; ++i) 
    array_schema.dimensions_[i] = tiledb_options.dimensions_[i].c_str(); 
  // --- domain ---
  int domain_num = 2*array_schema.dim_num_;
  array_schema.domain_ = new double[domain_num];
  for(int i=0; i<domain_num; ++i) 
    array_schema.domain_[i] = tiledb_options.domain_[i];
  // --- tile extents ---
  if(tiledb_options.tile_extents_.size() != 0) {
    int tile_extents_num = array_schema.dim_num_;
    array_schema.tile_extents_ = new double[tile_extents_num];
    for(int i=0; i<tile_extents_num; ++i) 
      array_schema.tile_extents_[i] = tiledb_options.tile_extents_[i];
  }
  // --- tile order ---
  if(tiledb_options.tile_order_ != "")
    array_schema.tile_order_ = tiledb_options.tile_order_.c_str();
  // --- types ---
  array_schema.types_ = new const char*[array_schema.attribute_num_+1];
  for(int i=0; i<array_schema.attribute_num_+1; ++i) 
    array_schema.types_[i] = tiledb_options.types_[i].c_str();

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) {
    delete [] array_schema.attributes_;
    if(array_schema.compression_ != NULL)
      delete[] array_schema.compression_;
    delete [] array_schema.dimensions_;
    delete [] array_schema.domain_;
    if(array_schema.tile_extents_ != NULL)
      delete [] array_schema.tile_extents_;
    delete [] array_schema.types_;
    return -1;
  }

  // Execute query
  int rc = tiledb_array_create(tiledb_ctx, &array_schema);

  // Clean up
  delete [] array_schema.attributes_;
  if(array_schema.compression_ != NULL)
    delete[] array_schema.compression_;
  delete [] array_schema.dimensions_;
  delete [] array_schema.domain_;
  if(array_schema.tile_extents_ != NULL)
    delete [] array_schema.tile_extents_;
  delete [] array_schema.types_;

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_array_delete(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_DELETE); 
  required_opt_codes.insert(OPT_ARRAY); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check array
  if(tiledb_options.array_.size() != 1) {
    PRINT_ERROR(std::string("Option '--array' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_array_delete(
      tiledb_ctx, 
      tiledb_options.array_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_array_move(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_MOVE); 
  required_opt_codes.insert(OPT_ARRAY); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check array
  if(tiledb_options.array_.size() != 2) {
    PRINT_ERROR(std::string("Option '--array' expects two arguments"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_array_move(
               tiledb_ctx, 
               tiledb_options.array_[0].c_str(),
               tiledb_options.array_[1].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_array_read(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_ARRAY); 
  required_opt_codes.insert(OPT_READ); 
  std::set<int> optional_opt_codes;
  optional_opt_codes.insert(OPT_RANGE); 
  optional_opt_codes.insert(OPT_OUT); 
  optional_opt_codes.insert(OPT_ATTRIBUTES); 
  optional_opt_codes.insert(OPT_DIMENSIONS); 
  optional_opt_codes.insert(OPT_FORMAT); 
  optional_opt_codes.insert(OPT_DELIMITER); 
  optional_opt_codes.insert(OPT_PRECISION); 
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check array
  if(tiledb_options.array_.size() != 1) {
    PRINT_ERROR(std::string("Option '--array' expects a single argument"));
    return -1;
  }
  
  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK)
    return -1;

  // Read into file or array
  // ----- get output
  if(tiledb_options.out_ == "") { // Default
    tiledb_options.out_ = "/dev/stdout";
    if(tiledb_options.format_ == "")
      tiledb_options.format_ = "csv";
  }
  // ----- get dimensions
  int dimensions_num = tiledb_options.dimensions_.size(); 
  const char** dimensions;
  if(dimensions_num == 0) {
    dimensions = NULL;
  } else {
    dimensions = new const char*[dimensions_num];
    for(int i=0; i<dimensions_num; ++i) 
      dimensions[i] = tiledb_options.dimensions_[i].c_str(); 
  }
  // ----- get attributes
  int attributes_num = tiledb_options.attributes_.size(); 
  const char** attributes;
  if(attributes_num == 0) {
    attributes = NULL;
  } else {
    attributes = new const char*[attributes_num];
    for(int i=0; i<attributes_num; ++i) 
      attributes[i] = tiledb_options.attributes_[i].c_str(); 
  }
  // ----- get range
  const double* range;
  int range_num = tiledb_options.range_.size();
  if(range_num == 0)
    range = NULL;
  else
    range = &tiledb_options.range_[0];
  // ----- get delimiter
  char delimiter;
  if(tiledb_options.delimiter_ == 0)
    delimiter = ',';
  else
    delimiter = tiledb_options.delimiter_;
  // ----- get precision
  char precision;
  if(tiledb_options.precision_ == 0)
    precision = 6;
  else
    precision = tiledb_options.precision_;

  // ----- execute command
  int rc;
  if(tiledb_array_exists(tiledb_ctx, tiledb_options.out_.c_str()))
    rc = tiledb_array_read_into_array(
             tiledb_ctx,
             tiledb_options.array_[0].c_str(), 
             tiledb_options.out_.c_str(), 
             range,
             range_num,
             dimensions,
             dimensions_num,
             attributes,
             attributes_num);
  else
    rc = tiledb_array_read_into_file(
             tiledb_ctx,
             tiledb_options.array_[0].c_str(), 
             tiledb_options.out_.c_str(), 
             range,
             range_num,
             dimensions,
             dimensions_num,
             attributes,
             attributes_num,
             tiledb_options.format_.c_str(), 
             delimiter,
             precision);

  // Clean up
  if(attributes != NULL)
    delete [] attributes;

  if(dimensions != NULL)
    delete [] dimensions;
  
  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_array_write(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_WRITE); 
  required_opt_codes.insert(OPT_ARRAY); 
  std::set<int> optional_opt_codes;
  optional_opt_codes.insert(OPT_IN); 
  optional_opt_codes.insert(OPT_FORMAT); 
  optional_opt_codes.insert(OPT_DELIMITER); 
  optional_opt_codes.insert(OPT_CONSOLIDATE); 
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check array
  if(tiledb_options.array_.size() != 1) {
    PRINT_ERROR(std::string("Option '--array' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK)
    return -1;

  // Write from file
  // ----- get input(s)
  if(tiledb_options.in_.size() == 0) { // Default
    tiledb_options.in_.push_back("/dev/stdin");
    if(tiledb_options.format_ == "")
      tiledb_options.format_ = "csv";
  }
  int inputs_num = tiledb_options.in_.size();
  const char** inputs = new const char*[inputs_num]; 
  for(int i=0; i<inputs_num; ++i)
    inputs[i] = tiledb_options.in_[i].c_str();
  // ----- execute command
  int rc = tiledb_array_write_from_file(
               tiledb_ctx,
               tiledb_options.array_[0].c_str(), 
               inputs, 
               inputs_num, 
               tiledb_options.format_.c_str(), 
               tiledb_options.delimiter_);
  delete [] inputs;
 
  // Potentially consolidate
  if(rc == TILEDB_OK && tiledb_options.consolidate_)
     rc = tiledb_array_consolidate(
              tiledb_ctx, 
              tiledb_options.array_[0].c_str()); 
 
  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_group_clear(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CLEAR); 
  required_opt_codes.insert(OPT_GROUP); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.group_.size() != 1) {
    PRINT_ERROR(std::string("Option '--group' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_group_clear(tiledb_ctx, tiledb_options.group_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_group_create(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CREATE); 
  required_opt_codes.insert(OPT_GROUP); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.group_.size() != 1) {
    PRINT_ERROR(std::string("Option '--group' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_group_create(tiledb_ctx, tiledb_options.group_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_group_delete(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_DELETE); 
  required_opt_codes.insert(OPT_GROUP); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.group_.size() != 1) {
    PRINT_ERROR(std::string("Option '--group' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_group_delete(tiledb_ctx, tiledb_options.group_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_group_move(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_MOVE); 
  required_opt_codes.insert(OPT_GROUP); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check array
  if(tiledb_options.group_.size() != 2) {
    PRINT_ERROR(std::string("Option '--group' expects two arguments"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_group_move(
               tiledb_ctx, 
               tiledb_options.group_[0].c_str(),
               tiledb_options.group_[1].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_metadata_clear(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CLEAR); 
  required_opt_codes.insert(OPT_METADATA); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.metadata_.size() != 1) {
    PRINT_ERROR(std::string("Option '--metadata' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_metadata_clear(
      tiledb_ctx, 
      tiledb_options.metadata_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_metadata_create(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CREATE); 
  required_opt_codes.insert(OPT_METADATA); 
  required_opt_codes.insert(OPT_ATTRIBUTES); 
  required_opt_codes.insert(OPT_TYPES); 
  std::set<int> optional_opt_codes;
  optional_opt_codes.insert(OPT_COMPRESSION); 
  optional_opt_codes.insert(OPT_CONSOLIDATION_STEP); 
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.metadata_.size() != 1) {
    PRINT_ERROR(std::string("Option '--metadata' expects a single argument"));
    return -1;
  }

  // Check that the number of attributes given is the same as the
  // number of types
  if(tiledb_options.attributes_.size() != tiledb_options.types_.size()) {
    PRINT_ERROR("The number of types must be the same as the number of " 
                "attributes");
    return -1;
  }

  // Check compression
  if(tiledb_options.compression_.size() != 0 &&
     tiledb_options.compression_.size() != 
     tiledb_options.attributes_.size()) {
    PRINT_ERROR("The number of compression types must equal to the number "
                "of attributes");
    return -1;
  }

  // Create TileDBMetadata struct 
  TileDB_MetadataSchema metadata_schema = {};
  // --- name ---
  metadata_schema.metadata_name_ = tiledb_options.metadata_[0].c_str();
  // --- attributes --- 
  metadata_schema.attribute_num_ = tiledb_options.attributes_.size();
  metadata_schema.attributes_ = new const char*[metadata_schema.attribute_num_];
  for(int i=0; i<metadata_schema.attribute_num_; ++i) 
    metadata_schema.attributes_[i] = tiledb_options.attributes_[i].c_str();
  // ---- compression ---
  if(tiledb_options.compression_.size() != 0) {
    metadata_schema.compression_ = 
        new const char*[metadata_schema.attribute_num_];
    for(int i=0; i<metadata_schema.attribute_num_; ++i) 
      metadata_schema.compression_[i] = tiledb_options.compression_[i].c_str();
  }
  // --- consolidation step ---
  metadata_schema.consolidation_step_ = tiledb_options.consolidation_step_;
  // --- types ---
  metadata_schema.types_ = new const char*[metadata_schema.attribute_num_];
  for(int i=0; i<metadata_schema.attribute_num_; ++i) 
    metadata_schema.types_[i] = tiledb_options.types_[i].c_str();

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) {
    delete [] metadata_schema.attributes_;
    delete [] metadata_schema.compression_;
    delete [] metadata_schema.types_;
    return -1;
  }

  // Execute query
  int rc = tiledb_metadata_create(tiledb_ctx, &metadata_schema);

  // Clean up
  delete [] metadata_schema.attributes_;
  delete [] metadata_schema.compression_;
  delete [] metadata_schema.types_;

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_metadata_delete(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_DELETE); 
  required_opt_codes.insert(OPT_METADATA); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.metadata_.size() != 1) {
    PRINT_ERROR(std::string("Option '--metadata' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_metadata_delete(
      tiledb_ctx, 
      tiledb_options.metadata_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_metadata_move(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_MOVE); 
  required_opt_codes.insert(OPT_METADATA); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.metadata_.size() != 2) {
    PRINT_ERROR(std::string("Option '--metadata' expects two arguments"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_metadata_move(
               tiledb_ctx, 
               tiledb_options.metadata_[0].c_str(),
               tiledb_options.metadata_[1].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_metadata_read(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_READ); 
  required_opt_codes.insert(OPT_METADATA); 
  required_opt_codes.insert(OPT_KEY); 
  std::set<int> optional_opt_codes;
  optional_opt_codes.insert(OPT_OUT); 
  optional_opt_codes.insert(OPT_ATTRIBUTES); 
  optional_opt_codes.insert(OPT_FORMAT); 
  optional_opt_codes.insert(OPT_DELIMITER); 
  optional_opt_codes.insert(OPT_PRECISION); 
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.metadata_.size() != 1) {
    PRINT_ERROR(std::string("Option '--metadata' expects a single argument"));
    return -1;
  }
  
  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK)
    return -1;

  // Write from file
  // ----- get output
  if(tiledb_options.out_ == "") { // Default
    tiledb_options.out_ = "/dev/stdout";
    if(tiledb_options.format_ == "")
      tiledb_options.format_ = "csv";
  }
  // ----- get attributes
  int attributes_num = tiledb_options.attributes_.size(); 
  const char** attributes;
  if(attributes_num == 0) {
    attributes = NULL;
  } else {
    attributes = new const char*[attributes_num];
    for(int i=0; i<attributes_num; ++i) 
      attributes[i] = tiledb_options.attributes_[i].c_str(); 
  }
  // ----- execute command
  int rc = tiledb_metadata_read_into_file(
               tiledb_ctx,
               tiledb_options.metadata_[0].c_str(), 
               tiledb_options.out_.c_str(), 
               tiledb_options.key_.c_str(),
               attributes,
               attributes_num,
               tiledb_options.format_.c_str(), 
               tiledb_options.delimiter_,
               tiledb_options.precision_);

  if(attributes != NULL)
    delete [] attributes;
  
  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_metadata_write(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_WRITE); 
  required_opt_codes.insert(OPT_METADATA); 
  std::set<int> optional_opt_codes;
  optional_opt_codes.insert(OPT_IN); 
  optional_opt_codes.insert(OPT_FORMAT); 
  optional_opt_codes.insert(OPT_DELIMITER); 
  optional_opt_codes.insert(OPT_CONSOLIDATE); 
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.metadata_.size() != 1) {
    PRINT_ERROR(std::string("Option '--metadata' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK)
    return -1;

  // Write from file
  // ----- get input(s)
  if(tiledb_options.in_.size() == 0) { // Default
    tiledb_options.in_.push_back("/dev/stdin");
    if(tiledb_options.format_ == "")
      tiledb_options.format_ = "csv";
  }
  int inputs_num = tiledb_options.in_.size();
  const char** inputs = new const char*[inputs_num]; 
  for(int i=0; i<inputs_num; ++i)
    inputs[i] = tiledb_options.in_[i].c_str();
  // ----- execute command
  int rc = tiledb_metadata_write_from_file(
               tiledb_ctx,
               tiledb_options.metadata_[0].c_str(), 
               inputs, 
               inputs_num, 
               tiledb_options.format_.c_str(), 
               tiledb_options.delimiter_);
  delete [] inputs;
 
  // Potentially consolidate
  if(rc == TILEDB_OK && tiledb_options.consolidate_)
     rc = tiledb_metadata_consolidate(
              tiledb_ctx, 
              tiledb_options.metadata_[0].c_str()); 
 
  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_workspace_clear(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CLEAR); 
  required_opt_codes.insert(OPT_WORKSPACE); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.workspace_.size() != 1) {
    PRINT_ERROR(std::string("Option '--workspace' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_workspace_clear(
      tiledb_ctx, 
      tiledb_options.workspace_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_workspace_create(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_CREATE); 
  required_opt_codes.insert(OPT_WORKSPACE); 
  required_opt_codes.insert(OPT_MASTER_CATALOG); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.workspace_.size() != 1) {
    PRINT_ERROR(std::string("Option '--workspace' expects a single argument"));
    return -1;
  }

  // Check master catalog
  if(tiledb_options.master_catalog_.size() != 1) {
    PRINT_ERROR(std::string("Option '--master-catalog' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_workspace_create(
      tiledb_ctx, 
      tiledb_options.workspace_[0].c_str(),
      tiledb_options.master_catalog_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_master_catalog_move(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_MOVE); 
  required_opt_codes.insert(OPT_MASTER_CATALOG); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check master catalog
  if(tiledb_options.master_catalog_.size() != 2) {
    PRINT_ERROR(std::string("Option '--master-catalog' expects two arguments"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_master_catalog_move(
      tiledb_ctx, 
      tiledb_options.master_catalog_[0].c_str(),
      tiledb_options.master_catalog_[1].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_master_catalog_delete(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_DELETE); 
  required_opt_codes.insert(OPT_MASTER_CATALOG); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check master catalog
  if(tiledb_options.master_catalog_.size() != 1) {
    PRINT_ERROR(std::string("Option '--master-catalog' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_master_catalog_delete(
      tiledb_ctx, 
      tiledb_options.master_catalog_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_workspace_delete(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_DELETE); 
  required_opt_codes.insert(OPT_WORKSPACE); 
  required_opt_codes.insert(OPT_MASTER_CATALOG); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.workspace_.size() != 1) {
    PRINT_ERROR(std::string("Option '--workspace' expects a single argument"));
    return -1;
  }

  // Check master catalog
  if(tiledb_options.master_catalog_.size() != 1) {
    PRINT_ERROR(std::string("Option '--master-catalog' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_workspace_delete(
      tiledb_ctx, 
      tiledb_options.workspace_[0].c_str(),
      tiledb_options.master_catalog_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_workspace_move(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_MOVE); 
  required_opt_codes.insert(OPT_WORKSPACE); 
  required_opt_codes.insert(OPT_MASTER_CATALOG); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Check metadata
  if(tiledb_options.workspace_.size() != 2) {
    PRINT_ERROR(std::string("Option '--workspace' expects two arguments"));
    return -1;
  }

  // Check master catalog
  if(tiledb_options.master_catalog_.size() != 1) {
    PRINT_ERROR(std::string("Option '--master-catalog' expects a single argument"));
    return -1;
  }

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_workspace_move(
      tiledb_ctx, 
      tiledb_options.workspace_[0].c_str(),
      tiledb_options.workspace_[1].c_str(),
      tiledb_options.master_catalog_[0].c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    PRINT("Program executed successfully");
    return 0;
  } else { // Error
    return -1;
  }
}

int execute_list(TileDBOptions& tiledb_options) {
  // Check wrong options
  std::set<int> required_opt_codes;
  required_opt_codes.insert(OPT_LIST); 
  std::set<int> optional_opt_codes;
  if(!opt_codes_are_correct(
        required_opt_codes, 
        optional_opt_codes, 
        tiledb_options))
    return -1;

  // Initialize TileDB
  TileDB_CTX* tiledb_ctx;
  if(tiledb_ctx_init(&tiledb_ctx) != TILEDB_OK) 
    return -1;

  // Execute query
  int rc = tiledb_list(
      tiledb_ctx, 
      tiledb_options.list_.c_str());

  // Finalize TileDB
  if(tiledb_ctx_finalize(tiledb_ctx) != TILEDB_OK)
    return -1;

  // Return 
  if(rc == TILEDB_OK) { // Success
    return 0;
  } else { // Error
    return -1;
  }
}

/* ************************ */
/*    PRINTING FUNCTIONS    */
/* ************************ */

// TODO
void print_usage() {
  std::cout << 
  "\n"
  "***************\n"
  "* v" << tiledb_version() << " *\n"
  "***************\n\n"
  "USAGE:\n\n";
}

int print_version(TileDBOptions& tiledb_options) {
  std::cout << "v" << tiledb_version() << "\n";

  // Success
  return 0;
}

bool opt_code_exists(
    int opt_code,
    const TileDBOptions& tiledb_options) {
  return tiledb_options.opt_codes_.find(opt_code) != 
         tiledb_options.opt_codes_.end();
}

bool opt_codes_are_correct(
    const std::set<int>& required_opt_codes,
    const std::set<int>& optional_opt_codes,
    const TileDBOptions& tiledb_options) {
  // Check if all required options are given
  std::vector<int> required_minus_given;
  std::set_difference(
    required_opt_codes.begin(),
    required_opt_codes.end(),
    tiledb_options.opt_codes_.begin(),
    tiledb_options.opt_codes_.end(),
    std::back_inserter(required_minus_given));
  if(required_minus_given.size() != 0) {
    PRINT_ERROR(std::string("Option '--") +
                long_options[required_minus_given[0] - 2].name + 
                "' is missing"); 
    return false;
  }

  // Check for redundat options 
  std::vector<int> required_and_optional;
  std::set_union(
    required_opt_codes.begin(),
    required_opt_codes.end(),
    optional_opt_codes.begin(),
    optional_opt_codes.end(),
    std::back_inserter(required_and_optional));
  std::vector<int> given_minus_required_and_optional;
  std::set_difference(
    tiledb_options.opt_codes_.begin(),
    tiledb_options.opt_codes_.end(),
    required_and_optional.begin(),
    required_and_optional.end(),
    std::back_inserter(given_minus_required_and_optional));
  if(given_minus_required_and_optional.size() != 0) {
    int redundant_num = given_minus_required_and_optional.size();
    std::string msg_redundant;
    for(int i=0; i<redundant_num; ++i)
      msg_redundant +=  std::string(" '--") +
          long_options[given_minus_required_and_optional[i] - 2].name +
          "'";
    PRINT_ERROR(std::string("Redundant options; remove") + msg_redundant);
    return false;
  }

  return true;
}
