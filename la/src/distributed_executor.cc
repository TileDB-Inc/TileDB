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
#include <assert.h>

/******************************************************
****************** ARRAY DESCRIPTOR *******************
******************************************************/

DistributedExecutor::ArrayDescriptor::ArrayDescriptor(
    const ArraySchema* array_schema,
    const StorageManager::ArrayDescriptor* ad, 
    int wold_size, int world_rank) {
  array_schema_ = array_schema;
  ad_ = ad;
  fd_ = NULL;

  // Compute the local domains
  // NOTE: This will change in the future - it will become much more flexible
  compute_local_dim_domains(array_schema, wold_size, world_rank); 
}

DistributedExecutor::ArrayDescriptor::ArrayDescriptor(
    const ArraySchema* array_schema,
    StorageManager::FragmentDescriptor* fd, 
    int wold_size, int world_rank) {
  array_schema_ = array_schema;
  ad_ = NULL;
  fd_ = fd;

  // Compute the local domains
  // NOTE: This will change in the future - it will become much more flexible
  compute_local_dim_domains(array_schema, wold_size, world_rank); 
}

void DistributedExecutor::ArrayDescriptor::
compute_local_dim_domains(const ArraySchema* array_schema, 
                          int world_size, int world_rank) {
  // Number of rows of the entire array
  int64_t row_num = array_schema->dim_domains()[0].second + 1;

  // Number of rows for the local partition
  int64_t local_row_num = row_num / world_size;
  // Handle the case row_num % world_size != 0 
  // The highest rank will get slightly more rows
  if(world_rank == world_size - 1) 
    local_row_num = row_num - (world_size-1)*local_row_num;

  // Index of the first and last row in the local parition
  int64_t local_first_row = (row_num / world_size) * world_rank; 
  int64_t local_last_row = local_first_row + local_row_num - 1; 

  // Set the local dimension domains: 
  // -- a fraction of rows, namely [local_first_row, local_last_row]
  // -- all the columns
  local_dim_domains_.push_back(
      std::pair<double, double>(local_first_row, local_last_row));
  local_dim_domains_.push_back(array_schema->dim_domains()[1]);
}

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

void DistributedExecutor::close_array(const ArrayDescriptor* ad) const {
  if(ad->ad_ != NULL)
   executor_->close_array(ad->ad_);
  if(ad->fd_ != NULL) 
   executor_->close_fragment(ad->fd_);

  delete ad;
}

void DistributedExecutor::define_array(
    const std::string& array_name, int64_t row_num, int64_t col_num) const {
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
  
  ArraySchema array_schema(array_name,
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

const std::vector<std::pair<double, double> >& 
DistributedExecutor::local_dim_domains(const ArrayDescriptor* ad) const {
  return ad->local_dim_domains_;
}

const DistributedExecutor::ArrayDescriptor* DistributedExecutor::open_array(
    const ArraySchema* array_schema, Mode mode) const {
  if(mode == READ) {
     const StorageManager::ArrayDescriptor* ad = 
         executor_->open_array(array_schema);
     return new ArrayDescriptor(array_schema, ad, mpi_module_->size(), 
                                mpi_module_->rank());
  } else if(mode == WRITE) {
     StorageManager::FragmentDescriptor* fd = 
         executor_->open_fragment(array_schema);
     return new ArrayDescriptor(array_schema, fd, mpi_module_->size(), 
                                mpi_module_->rank());
  }
}

void DistributedExecutor::read(
    const ArrayDescriptor* ad, const void* range,
    void*& coords, size_t& coords_size,
    void*& values, size_t& values_size) const {
  assert(ad->ad_ != NULL);

  // The values will be retrieved from the first attribute of the array,
  // since no other attribute is specified.
  int attribute_id = 0;

  executor_->read(ad->ad_, attribute_id, range, 
                  coords, coords_size, values, values_size);
}

void DistributedExecutor::transpose(
    const std::string& array_name, 
    const std::string& result_array_name) const {
  // Load array schema
  const ArraySchema* array_schema = executor_->load_array_schema(array_name);

  // Create a result array schema (that of the transpose)
  const ArraySchema* result_array_schema = 
      array_schema->transpose(result_array_name);

  // Define the result array
  executor_->define_array(result_array_schema);

  // Open the input and result arrays
  const ArrayDescriptor* ad = 
      open_array(array_schema, READ);
  const ArrayDescriptor* result_ad = 
      open_array(result_array_schema, WRITE);

  // TODO jeff to implement the transpose using the following stavros's APIs of
  // DistributedExecutor:
  // 1. read  
  // 2. write  
  // 3. write_sorted 
  // 4. local_dim_domains

  // Close the input and output arrays
  close_array(ad);
  close_array(result_ad);
}

void DistributedExecutor::write(
    const ArrayDescriptor* ad, 
    const void* coords, size_t coords_size,
    const void* values, size_t values_size) const {
  assert(ad->fd_ != NULL);

  executor_->write(ad->fd_, coords, coords_size, values, values_size);
}

void DistributedExecutor::write_sorted(
    const ArrayDescriptor* ad, 
    const void* coords, size_t coords_size,
    const void* values, size_t values_size) const {
  // TODO: Stavros

}
