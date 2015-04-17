/**
 * @file   executor.cc
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
 * This file implements the Executor class.
 */
  
#include "executor.h"
#include "utils.h"
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Executor::Executor(std::string workspace) { 
  set_workspace(workspace);
  create_directory(workspace_); 

  storage_manager_ = new StorageManager(workspace);
  loader_ = new Loader(workspace, storage_manager_);
  query_processor_ = new QueryProcessor(workspace, storage_manager_);
  consolidator_ = new Consolidator(workspace, storage_manager_);
  // TODO read config file
}

Executor::~Executor() {
  delete loader_;
  delete query_processor_;
  delete consolidator_;
  delete storage_manager_; 
} 

/******************************************************
************************ QUERIES **********************
******************************************************/

void Executor::clear_array(const std::string& array_name) const {
  if(!storage_manager_->array_defined(array_name))
    throw ExecutorException("Array is not defined.");

  storage_manager_->clear_array(array_name);
}

void Executor::close_array(
    const StorageManager::ArrayDescriptor* ad) const {
  storage_manager_->close_array(ad);
}

void Executor::close_fragment(
    StorageManager::FragmentDescriptor* fd) const {
  storage_manager_->close_fragment(fd);
}

void Executor::define_array(const ArraySchema* array_schema) const {
  if(storage_manager_->array_defined(array_schema->array_name()))
    throw ExecutorException("Array is already defined.");

  storage_manager_->define_array(array_schema);
}

void Executor::delete_array(const std::string& array_name) const {
  if(!storage_manager_->array_defined(array_name))
    throw ExecutorException("Array is not defined.");

  storage_manager_->delete_array(array_name);
}

void Executor::export_to_csv(const std::string& array_name,
                             const std::string& filename) const {
  // Check if the array is defined
  if(!storage_manager_->array_defined(array_name))
    throw ExecutorException("Array is not defined.");

  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  // Get fragment names
  std::vector<std::string> fragment_names = 
      get_all_fragment_names(array_schema);

  // Check if the array is empty
  if(fragment_names.size() == 0)
    throw ExecutorException("Input array is empty.");

  // Open array
  const StorageManager::ArrayDescriptor* ad =
      storage_manager_->open_array(array_schema, 
                                   fragment_names,
                                   StorageManager::READ);

  // Dispatch query to query processor
  // Single fragment
  try {
    if(fragment_names.size() == 1)  {  
      const StorageManager::FragmentDescriptor* fd = ad->fd()[0];
      query_processor_->export_to_csv(fd, filename);
    } else { // Multiple fragments
/* TODO
      const std::vector<const StorageManager::FragmentDescriptor*>& fd = 
          ad->fd();
      query_processor_->export_to_csv(fd, filename);
*/
    }
  } catch(QueryProcessorException& qe) {
    storage_manager_->close_array(ad);
    throw ExecutorException(qe.what());
  } 

  // Clean up
  storage_manager_->close_array(ad);
}

bool Executor::file_exists(const std::string& filename) const {
  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

void Executor::filter(const std::string& array_name,
                      const std::string& expression,
                      const std::string& result_array_name) const {
  // Check if the input array is defined
  if(!storage_manager_->array_defined(array_name))
    throw ExecutorException("Input array is not defined.");

  // Check if the output array is defined
  if(storage_manager_->array_defined(result_array_name))
    throw ExecutorException("Result array is already defined.");

  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  // Get fragment names
  std::vector<std::string> fragment_names = 
      get_all_fragment_names(array_schema);

  // Check if the array is empty
  if(fragment_names.size() == 0)
    throw ExecutorException("Input array is empty.");

  // Open array
  const StorageManager::ArrayDescriptor* ad =
      storage_manager_->open_array(array_schema, 
                                   fragment_names,
                                   StorageManager::READ);

  // Hardcode some expression - TODO: parse input expression and check soundness
  ExpressionTree* hardcoded_expression;
  // TODO: hardcode_expression(expression);

  // Define the result array
  ArraySchema result_array_schema = array_schema->clone(result_array_name);
  storage_manager_->define_array(&result_array_schema);

  // Create the result array
  StorageManager::FragmentDescriptor* result_fd = 
      storage_manager_->open_fragment(&result_array_schema,  "0_0", 
                                      StorageManager::CREATE);

  // Dispatch query to query processor
  try {
    if(fragment_names.size() == 1)  {  // Single fragment
      const StorageManager::FragmentDescriptor* fd = ad->fd()[0]; 
/* TODO
      query_processor_->filter(fd, hardcoded_expression, result_fd);
*/
    } else { // Multiple fragments 
      const std::vector<StorageManager::FragmentDescriptor*>& fd = 
          ad->fd();
/* TODO
      query_processor_->filter(fd, hardcoded_expression, result_fd);
*/
    }
  } catch(QueryProcessorException& qe) {
    storage_manager_->delete_fragment(result_array_name, "0_0");
    storage_manager_->close_array(ad);
    throw ExecutorException(qe.what());
  }

  // Update the fragment information of result array at the consolidator
  update_fragment_info(result_fd->array_schema());

  // Clean up
  storage_manager_->close_fragment(result_fd);
  storage_manager_->close_array(ad);
}

void Executor::join(const std::string& array_name_A,
                    const std::string& array_name_B,
                    const std::string& result_array_name) const {
  // Check if the input arrays are defined
  if(!storage_manager_->array_defined(array_name_A))
    throw ExecutorException("Input array #1 is not defined.");
  if(!storage_manager_->array_defined(array_name_B))
    throw ExecutorException("Input array #2 is not defined.");

  // Check if the output array is defined
  if(storage_manager_->array_defined(result_array_name))
    throw ExecutorException("Result array is already defined.");


  const ArraySchema* array_schema_A =
      storage_manager_->load_array_schema(array_name_A);

  const ArraySchema* array_schema_B =
      storage_manager_->load_array_schema(array_name_B);

  // Get fragment names
  std::vector<std::string> fragment_names_A = 
      get_all_fragment_names(array_schema_A);
  std::vector<std::string> fragment_names_B = 
      get_all_fragment_names(array_schema_B);

  // Check if the arrays are empty
  if(fragment_names_A.size() == 0)
    throw ExecutorException("Input array #1 is empty.");
  if(fragment_names_B.size() == 0)
    throw ExecutorException("Input array #2 is empty.");

  // Open arrays
  const StorageManager::ArrayDescriptor* ad_A =
      storage_manager_->open_array(array_schema_A, 
                                   fragment_names_A,
                                   StorageManager::READ);
  const StorageManager::ArrayDescriptor* ad_B =
      storage_manager_->open_array(array_schema_B, 
                                   fragment_names_B,
                                   StorageManager::READ); 

  // Check join-compatibility
  std::pair<bool, std::string> join_comp =
      ArraySchema::join_compatible(*array_schema_A, *array_schema_B);
  if(!join_comp.first)
    throw ExecutorException(
        std::string("The input arrays are not join-compatible") + ". " +
        join_comp.second);

  // Define the result array
  ArraySchema result_array_schema = 
      ArraySchema::create_join_result_schema(*array_schema_A, 
                                             *array_schema_B, 
                                             result_array_name);
  storage_manager_->define_array(&result_array_schema);

  // Create the result array
  StorageManager::FragmentDescriptor* result_fd = 
      storage_manager_->open_fragment(&result_array_schema,  "0_0", 
                                      StorageManager::CREATE);
  // Dispatch query to query processor
  try {
    if(fragment_names_A.size() == 1 && fragment_names_B.size() == 1)  { 
      // Single fragment
      const StorageManager::FragmentDescriptor* fd_A = ad_A->fd()[0]; 
      const StorageManager::FragmentDescriptor* fd_B = ad_B->fd()[0]; 
/* TODO
      query_processor_->join(fd_A, fd_B, result_fd);
*/
    } else { // Multiple fragments 
      const std::vector<StorageManager::FragmentDescriptor*>& fd_A = 
          ad_A->fd();
      const std::vector<StorageManager::FragmentDescriptor*>& fd_B = 
          ad_B->fd();
/* TODO
      query_processor_->join(fd_A, fd_B, result_fd);
*/
    }
  } catch(QueryProcessorException& qe) {
    storage_manager_->delete_fragment(result_array_name, "0_0");
    storage_manager_->close_array(ad_A);
    storage_manager_->close_array(ad_B);
    throw ExecutorException(qe.what());
  }

  // Update the fragment information of result array at the consolidator
  update_fragment_info(result_fd->array_schema());

  // Clean up
  storage_manager_->close_array(ad_A);
  storage_manager_->close_array(ad_B);
  storage_manager_->close_fragment(result_fd);
}

void Executor::load(const std::string& filename, 
                    const std::string& array_name) const {
  // Check if array is defined
  if(!storage_manager_->array_defined(array_name)) 
    throw ExecutorException("Array is not defined."); 

  // Check if array is loaded
  if(storage_manager_->array_loaded(array_name)) 
    throw ExecutorException("Array is already loaded.");

  // Check if file exists
  if(!file_exists(filename)) 
    throw ExecutorException(std::string("File '") + filename +
                            "' not found.");

  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  // Load
  try {
    loader_->load(filename, array_name, "0_0");
  } catch(LoaderException& le) {
    throw ExecutorException(le.what());
  }

  // Update the fragment information of the array at the consolidator
  update_fragment_info(array_schema);
}

const ArraySchema* Executor::load_array_schema(
    const std::string& array_name) const {
  return storage_manager_->load_array_schema(array_name);
}

void Executor::nearest_neighbors(const std::string& array_name,
                                 const std::vector<double>& q,
                                 int64_t k,
                                 const std::string& result_array_name) const {
  // Check if the input array is defined
  if(!storage_manager_->array_defined(array_name))
    throw ExecutorException("Input array is not defined.");

  // Check if the output array is defined
  if(storage_manager_->array_defined(result_array_name))
    throw ExecutorException("Result array is already defined.");

  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  // Get fragment names
  std::vector<std::string> fragment_names = 
      get_all_fragment_names(array_schema);

  // Check if the array is empty
  if(fragment_names.size() == 0)
    throw ExecutorException("Input array is empty.");

  // Open array
  const StorageManager::ArrayDescriptor* ad =
      storage_manager_->open_array(array_schema, 
                                   fragment_names,
                                   StorageManager::READ);

  // Check soundness of reference cell/point q
  if(q.size() != array_schema->dim_num()) {
    storage_manager_->close_array(ad);
    throw ExecutorException("The reference cell does not match input array "
                            "dimensionality.");
  }

  // Define the result array
  ArraySchema result_array_schema = array_schema->clone(result_array_name);
  storage_manager_->define_array(&result_array_schema);

  // Create the result array
  StorageManager::FragmentDescriptor* result_fd = 
      storage_manager_->open_fragment(&result_array_schema,  "0_0", 
                                      StorageManager::CREATE);

  // Dispatch query to query processor
  try {
    if(fragment_names.size() == 1)  {  // Single fragment
      const StorageManager::FragmentDescriptor* fd = ad->fd()[0]; 
/* TODO
      query_processor_->nearest_neighbors(fd, q, k, result_fd);
*/
    } else { // Multiple fragments TODO 
    /*
      const std::vector<const StorageManager::FragmentDescriptor*>& fd = 
          ad->fd();
      query_processor_->nearest_neighbors(fd, q, k, result_fd);
    */
    throw ExecutorException("Nearest neighbors on multiple fragments"
                            " currently not supported.");
    }
  } catch(QueryProcessorException& qe) {
    storage_manager_->delete_fragment(result_array_name, "0_0");
    storage_manager_->close_array(ad);
    throw ExecutorException(qe.what());
  }

  // Update the fragment information of result array at the consolidator
  update_fragment_info(result_fd->array_schema());

  // Clean up
  storage_manager_->close_fragment(result_fd);
  storage_manager_->close_array(ad);
}

const StorageManager::ArrayDescriptor* Executor::open_array(
    const ArraySchema* array_schema) const {
  // Check if the input array is defined
  if(!storage_manager_->array_defined(array_schema->array_name()))
    throw ExecutorException("Input array is not defined.");

  // Get fragment names
  std::vector<std::string> fragment_names = 
      get_all_fragment_names(array_schema);

  // Check if the array is empty
  if(fragment_names.size() == 0)
    throw ExecutorException("Input array is empty.");

  // Open array
  return storage_manager_->open_array(array_schema, fragment_names, 
                                      StorageManager::READ);
}

StorageManager::FragmentDescriptor* Executor::open_fragment(
    const ArraySchema* array_schema) const {
  // Get fragment name from the consolidator
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema, Consolidator::WRITE);
  std::string fragment_name = consolidator_->get_next_fragment_name(ad);
  consolidator_->close_array(ad);

  // Open fragment and return descriptor
  return storage_manager_->open_fragment(array_schema, fragment_name, 
                                         StorageManager::CREATE);
}

void Executor::read(const StorageManager::ArrayDescriptor* ad,
                    int attribute_id, const void* range,
                    void*& coords, size_t& coords_size,
                    void*& attrs, size_t& attrs_size) const {
  // Check if the array is empty
  if(ad->is_empty())
    throw ExecutorException("Input array is empty.");

  // Dispatch query to query processor
  try {
    if(ad->fd().size() == 1)  {  // Single fragment
      const StorageManager::FragmentDescriptor* fd = ad->fd()[0];
      query_processor_->read(fd, attribute_id, range, 
                             coords, coords_size, attrs, attrs_size);
    } else { // Multiple fragments TODO 
      throw ExecutorException("Operation currently not supported on "
                              "multiple fragments.");
    }
  } catch(QueryProcessorException& qe) {
    throw ExecutorException(qe.what());
  }
} 

void Executor::retile(const std::string& array_name,
                      int64_t capacity,
                      ArraySchema::CellOrder cell_order,
                      const std::vector<double>& tile_extents) const {
  // Check if the input array is defined
  if(!storage_manager_->array_defined(array_name))
    throw ExecutorException("Input array is not defined.");

  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  // Get fragment names
  std::vector<std::string> fragment_names = 
      get_all_fragment_names(array_schema);

  // Check if the array is empty
  if(fragment_names.size() == 0)
    throw ExecutorException("Input array is empty.");

  // Open array
  const StorageManager::ArrayDescriptor* ad =
      storage_manager_->open_array(array_schema, 
                                   fragment_names,
                                   StorageManager::READ);

  // Check soundness of tile extents
  if(tile_extents.size() != 0) {
    if(tile_extents.size() != array_schema->dim_num()) {
      storage_manager_->close_array(ad);
      throw ExecutorException("Tile extents do not match input array"
                              " dimensionality.");
    }
    for(int i=0; i<array_schema->dim_num(); ++i) {
      if(tile_extents[i] > array_schema->dim_domains()[i].second - 
                        array_schema->dim_domains()[i].first +1) {
        storage_manager_->close_array(ad);
        throw ExecutorException("The tile extents must not exceed"
                                " their corresponding domain ranges.");
      }
    }
  }

  // Check if there is nothing to do
  bool capacity_changed = (capacity != 0 && 
                           capacity != array_schema->capacity());
  bool order_changed = (cell_order != ArraySchema::CO_NONE &&
                        cell_order != array_schema->cell_order()); 
  bool tile_extents_changed = false;
  const std::vector<double>& array_schema_tile_extents = 
      array_schema->tile_extents();
  if(array_schema->has_irregular_tiles()) {
    if(tile_extents.size() != 0)
      tile_extents_changed = true;
  } else { // Regular tiles
    if(tile_extents.size() == 0 || 
       !std::equal(array_schema_tile_extents.begin(),
                   array_schema_tile_extents.end(),
                   tile_extents.begin()))
      tile_extents_changed = true;
  }

  if(!capacity_changed && !order_changed && !tile_extents_changed)  {
      storage_manager_->close_array(ad);
      throw ExecutorException("Nothing to do; retiling arguments are"
                              " the same as in the schema of the input"
                              " array.");
  }

  try {
    const std::vector<StorageManager::FragmentDescriptor*>& fd = 
        ad->fd();
/* TODO
    query_processor_->retile(fd, capacity, cell_order, tile_extents);
*/

/* TODO MULTIPLE FRAGMENTS */

  } catch(QueryProcessorException& qe) {
     storage_manager_->close_array(ad);   
    throw ExecutorException(qe.what());
  }  
  
  // Clean up
  storage_manager_->close_array(ad);
}
                      

void Executor::subarray(const std::string& array_name,
                        const double* range,
                        const std::string& result_array_name) const {
  // Check if the input array is defined
  if(!storage_manager_->array_defined(array_name))
    throw ExecutorException("Input array is not defined.");

  // Check if the output array is defined
  if(storage_manager_->array_defined(result_array_name))
    throw ExecutorException("Result array is already defined.");

  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  // Get fragment names
  std::vector<std::string> fragment_names = 
      get_all_fragment_names(array_schema);

  // Check if the array is empty
  if(fragment_names.size() == 0)
    throw ExecutorException("Input array is empty.");

  // Open array
  const StorageManager::ArrayDescriptor* ad =
      storage_manager_->open_array(array_schema, 
                                   fragment_names,
                                   StorageManager::READ);

  // Define the result array
  ArraySchema result_array_schema = array_schema->clone(result_array_name);
  storage_manager_->define_array(&result_array_schema);

  // Create the result array
  StorageManager::FragmentDescriptor* result_fd = 
      storage_manager_->open_fragment(&result_array_schema,  "0_0", 
                                      StorageManager::CREATE);

  // Dispatch query to query processor
  try {
    if(fragment_names.size() == 1)  {  // Single fragment
      const StorageManager::FragmentDescriptor* fd = ad->fd()[0]; 
/* TODO
      query_processor_->subarray(fd, range, result_fd);
*/
    } else { // Multiple fragments 
      const std::vector<StorageManager::FragmentDescriptor*>& fd = 
          ad->fd();
/*
      query_processor_->subarray(fd, range, result_fd);
*/
    }
  } catch(QueryProcessorException& qe) {
    storage_manager_->delete_fragment(result_array_name, "0_0");
    storage_manager_->close_array(ad);
    throw ExecutorException(qe.what());
  }

  // Update the fragment information of result array at the consolidator
  update_fragment_info(result_fd->array_schema());

  // Clean up
  storage_manager_->close_fragment(result_fd);
  storage_manager_->close_array(ad);
}

void Executor::update(const std::string& filename, 
                      const std::string& array_name) const {
  // Check if array is defined
  if(!storage_manager_->array_defined(array_name)) 
    throw ExecutorException("Array is not defined."); 

  // Check if array is loaded
  if(!storage_manager_->array_loaded(array_name)) 
    throw ExecutorException("Array is not loaded.");

  // Check if file exists
  if(!file_exists(filename)) 
    throw ExecutorException(std::string("File '") + filename +
                            "' not found.");

  const ArraySchema* array_schema =
      storage_manager_->load_array_schema(array_name);

  // Get fragment name
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema, Consolidator::WRITE);
  std::string fragment_name = consolidator_->get_next_fragment_name(ad);

  // Load the new segment
  try {
    loader_->load(filename, array_schema, fragment_name);
  } catch(LoaderException& le) {
    throw ExecutorException(le.what());
  }

  // Update the fragment information of the array at the consolidator.
  // This may trigger consolidation.
  consolidator_->add_fragment(ad);  
  consolidator_->close_array(ad);
}

void Executor::write(
    StorageManager::FragmentDescriptor* fd,
    void* coords, size_t coords_size,
    void* attrs, size_t attrs_size) const {
  storage_manager_->write_cells(fd, coords, coords_size, attrs, attrs_size);
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

std::vector<std::string> Executor::get_all_fragment_names(
    const ArraySchema* array_schema) const {
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema, Consolidator::READ);
  std::vector<std::string> fragment_names = 
      consolidator_->get_all_fragment_names(ad);  
  consolidator_->close_array(ad);

  return fragment_names;
}

bool Executor::path_exists(const std::string& path) const {
  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

inline
void Executor::set_workspace(const std::string& path) {
  workspace_ = path;
  
  // Replace '~' with the absolute path
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }

  // Check if the input path is an existing directory 
  if(!path_exists(workspace_)) 
    throw ExecutorException("Workspace does not exist.");
 
  workspace_ += "/Executor";
}

void Executor::update_fragment_info(const ArraySchema* array_schema) const {
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema, Consolidator::WRITE);
  consolidator_->add_fragment(ad);  
  consolidator_->close_array(ad);
}

