/**
 * @file   distributed_executor.cc
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
 * This file implements class DistributedExecutor.
 */

#include "distributed_executor.h"

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

DistributedExecutor::DistributedExecutor(const std::string& workspace,
                                         MPIModule* mpi_module)
    : workspace_(workspace), mpi_module_(mpi_module) {
  executor_ = new Executor(workspace_);
}

DistributedExecutor::~DistributedExecutor() {
  delete executor_;
}

/******************************************************
********************** QUERIES ************************
******************************************************/

void DistributedExecutor::define_matrix(
    const std::string& matrix_name, int64_t row_num, int64_t col_num) const {
  // Create the array schema
  std::vector<std::string> attribute_names;
  attribute_names.push_back("values");
  std::vector<std::string> dim_names;
  dim_names.push_back("row");
  dim_names.push_back("col");
  std::vector<std::pair<double, double> > dim_domains;
  dim_domains.push_back(std::pair<double, double>(0, row_num-1));
  dim_domains.push_back(std::pair<double, double>(0, col_num-1));
  std::vector<const std::type_info*> types;
  types.push_back(&typeid(double)); // Type for values
  types.push_back(&typeid(int64_t)); // Type for row/col indexes/coordinates
  
  ArraySchema array_schema(matrix_name,
                           attribute_names,
                           dim_names,
                           dim_domains,
                           types,
                           ArraySchema::CO_ROW_MAJOR); // Cell order
  // Note: Default values are used for tile capacity (10000) and 
  // consolidation step (1) for updates

  // Define TileDB array
  executor_->define_array(&array_schema);
}

void DistributedExecutor::load(const std::string& filename, 
                               const std::string& array_name) const {
  executor_->load(filename, array_name);
}

void DistributedExecutor::transpose(
    const std::string& matrix_name, 
    const std::string& result_matrix_name) const {
  // Load array_schema for matrix_name
  const ArraySchema* array_schema = executor_->load_array_schema(matrix_name);

  // Create a result array schema (that of the transpose)
  // TODO: stavros 

  // Open the input and output arrays
  // TODO: stavros 
 
  // TODO stavros to implement the skeleton and some basic read and write APIs  

  // TODO jeff to implement the transpose using stavros's APIs

  // Close the input and output arrays
  // TODO: stavros 
}
