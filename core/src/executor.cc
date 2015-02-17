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
  create_workspace(); 

  storage_manager_ = new StorageManager(workspace);
  loader_ = new Loader(workspace, *storage_manager_);
  query_processor_ = new QueryProcessor(workspace, *storage_manager_);
  consolidator_ = new Consolidator(workspace, *storage_manager_);
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

void Executor::define_array(const ArraySchema& array_schema) const {
  if(storage_manager_->array_defined(array_schema.array_name()))
    throw ExecutorException("Array is already defined.");

  storage_manager_->define_array(array_schema);
}


// TODO REMOVE
/*
void Executor::delete_array(const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  
  // Check if array exists
  if(!consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot delete array: "
                            "array does not exist.");

  // Get fragment suffixes
  std::vector<std::string> fragment_suffixes = 
      get_fragment_suffixes(array_schema);

  // Delete fragments at the storage manager
  for(unsigned int i=0; i<fragment_suffixes.size(); ++i)
    storage_manager_->delete_array(array_name + "_" + fragment_suffixes[i]);
 
  // Delete fragment book-keeping info at the consolidator
  consolidator_->delete_array(array_name); 
}
*/

//TODO remove
/*
void Executor::export_to_CSV(const std::string& filename, 
                             const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
 
  // Check if array exists
  if(!consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot export array: "
                            "array does not exist.");

  // Get fragment suffixes
  std::vector<std::string> fragment_suffixes = 
      get_fragment_suffixes(array_schema);

  // Dispatch query to query processor
  if(fragment_suffixes.size() == 1)  {  // Single fragment
    const StorageManager::FragmentDescriptor* fd = 
        storage_manager_->open_fragment(array_name + std::string("_")  
                                        + fragment_suffixes[0]);
    query_processor_->export_to_CSV(fd, filename);
    storage_manager_->close_fragment(fd);
  } else { // Multiple fragments
    std::vector<const StorageManager::FragmentDescriptor*> fd;
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      fd.push_back(storage_manager_->open_fragment(
                       array_name + std::string("_") +
                       fragment_suffixes[i]));
    query_processor_->export_to_CSV(fd, filename);
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      storage_manager_->close_fragment(fd[i]);
  }
}
*/

bool Executor::file_exists(const std::string& filename) const {
  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

//TODO remove
/*
void Executor::filter(const ArraySchema& array_schema,
                      const ExpressionTree* expression,
                      const std::string& result_array_name) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();

  // Check if array exists
  if(!consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot filter array: "
                            "array does not exist.");

  // Get fragment suffixes
  std::vector<std::string> fragment_suffixes = 
      get_fragment_suffixes(array_schema);

  // Dispatch query to query processor
  if(fragment_suffixes.size() == 1)  {  // Single fragment
    const StorageManager::FragmentDescriptor* fd = 
        storage_manager_->open_fragment(array_name + std::string("_")  
                                        + fragment_suffixes[0]);
    query_processor_->filter(fd, expression, result_array_name + "_0_0");
    storage_manager_->close_fragment(fd);
  } else { // Multiple fragments
    std::vector<const StorageManager::FragmentDescriptor*> fd;
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      fd.push_back(storage_manager_->open_fragment(
                       array_name + std::string("_") +
                       fragment_suffixes[i]));
    query_processor_->filter(fd, expression, result_array_name + "_0_0");
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      storage_manager_->close_fragment(fd[i]);
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = 
      array_schema.clone(result_array_name);
  update_fragment_info(result_array_schema);
}
*/

// TODO remove
/*
void Executor::join(const ArraySchema& array_schema_A,
                    const ArraySchema& array_schema_B,
                    const std::string& result_array_name) const {
  // For easy reference
  const std::string& array_name_A = array_schema_A.array_name();
  const std::string& array_name_B = array_schema_B.array_name();

  // Check if arrays exist
  if(!consolidator_->array_exists(array_name_A))
    throw ExecutorException("[Executor] Cannot join arrays: "
                            "input array #1 does not exist.");
  if(!consolidator_->array_exists(array_name_B))
    throw ExecutorException("[Executor] Cannot join arrays: "
                            "input array #2 does not exist.");

  // Get fragment suffixes
  std::vector<std::string> fragment_suffixes_A = 
      get_fragment_suffixes(array_schema_A);
  std::vector<std::string> fragment_suffixes_B = 
      get_fragment_suffixes(array_schema_B);

  // Dispatch query to query processor
  if(fragment_suffixes_A.size() == 1 && fragment_suffixes_B.size() == 1)  {  
    // Single fragment
    const StorageManager::FragmentDescriptor* fd_A = 
        storage_manager_->open_fragment(array_name_A + std::string("_")  
                                        + fragment_suffixes_A[0]);
    const StorageManager::FragmentDescriptor* fd_B = 
        storage_manager_->open_fragment(array_name_B + std::string("_")  
                                                + fragment_suffixes_B[0]);
    try {
      query_processor_->join(fd_A, fd_B, result_array_name + "_0_0");
    } catch(QueryProcessorException& qe) {
      storage_manager_->close_fragment(fd_A);
      storage_manager_->close_fragment(fd_B);
      throw ExecutorException(qe.what());
    }
    storage_manager_->close_fragment(fd_A);
    storage_manager_->close_fragment(fd_B);
  } else { // Multiple fragments
    std::vector<const StorageManager::FragmentDescriptor*> fd_A;
    std::vector<const StorageManager::FragmentDescriptor*> fd_B;
    for(unsigned int i=0; i<fragment_suffixes_A.size(); ++i)
      fd_A.push_back(storage_manager_->open_fragment(
           array_name_A + std::string("_") + fragment_suffixes_A[i]));
    for(unsigned int i=0; i<fragment_suffixes_B.size(); ++i)
      fd_B.push_back(storage_manager_->open_fragment(
           array_name_B + std::string("_") + fragment_suffixes_B[i]));

    query_processor_->join(fd_A, fd_B, result_array_name + "_0_0");

    for(unsigned int i=0; i<fragment_suffixes_A.size(); ++i)  
      storage_manager_->close_fragment(fd_A[i]);
    for(unsigned int i=0; i<fragment_suffixes_B.size(); ++i)  
      storage_manager_->close_fragment(fd_B[i]);
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = ArraySchema::create_join_result_schema(
                                   array_schema_A, 
                                   array_schema_B, 
                                   result_array_name);
  update_fragment_info(result_array_schema);
}
*/

// TODO: remove
/*
void Executor::load(const std::string& filename,  
                    const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name(); 

  // Check if array exists
  if(consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot load array: "
                            "array already exists.");

  // Create from the CSV file the 0-th (i.e., first) array fragment
  // (see class Consolidator for details about the array fragments)
  ArraySchema fragment_schema = array_schema.clone(array_name + "_0_0");
  loader_->load(filename, fragment_schema);

  // Update the fragment information of the array at the consolidator
  update_fragment_info(array_schema);
}
*/

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

  // Load
  try {
    loader_->load(filename, array_name, "0_0");
  } catch(LoaderException& le) {
    throw ExecutorException(le.what());
  }

  // Update the fragment information of the array at the consolidator
  update_fragment_info(array_name);
}

// TODO 
/*
void Executor::nearest_neighbors(const ArraySchema& array_schema,
                                 const std::vector<double>& q,
                                 uint64_t k,
                                 const std::string& result_array_name) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();

  // Check if array exists
  if(!consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot perform nearest neighbor search:"
                            " array does not exist.");

  // Get fragment suffixes
  std::vector<std::string> fragment_suffixes = 
      get_fragment_suffixes(array_schema);

  // Dispatch query to query processor
  if(fragment_suffixes.size() == 1)  {  // Single fragment
    const StorageManager::FragmentDescriptor* fd = 
        storage_manager_->open_fragment(array_name + std::string("_")  
                                                + fragment_suffixes[0]);
    query_processor_->nearest_neighbors(fd, q, k, result_array_name + "_0_0");
    storage_manager_->close_fragment(fd);
  } else { // Multiple fragments TODO
    throw ExecutorException("[Executor] Cannot perform nearest neighbor search:"
                            " query over multiple array fragments currently not"
                            " supported.");
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = 
      array_schema.clone(result_array_name);
  update_fragment_info(result_array_schema);
}
*/

// TODO remove
/*
void Executor::subarray(const ArraySchema& array_schema,
                        const Tile::Range& range,
                        const std::string& result_array_name) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();

  // Check if array exists
  if(!consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot perform subarray: "
                            "array does not exist.");

  // Get fragment suffixes
  std::vector<std::string> fragment_suffixes = 
      get_fragment_suffixes(array_schema);

  // Dispatch query to query processor
  if(fragment_suffixes.size() == 1)  {  // Single fragment
    const StorageManager::FragmentDescriptor* fd = 
        storage_manager_->open_fragment(array_name + std::string("_")  
                                                + fragment_suffixes[0]);
    query_processor_->subarray(fd, range, result_array_name + "_0_0");
    storage_manager_->close_fragment(fd);
  } else { // Multiple fragments 
    std::vector<const StorageManager::FragmentDescriptor*> fd;
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      fd.push_back(storage_manager_->open_fragment(
                       array_name + std::string("_") +
                       fragment_suffixes[i]));
    query_processor_->subarray(fd, range, result_array_name + "_0_0");
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      storage_manager_->close_fragment(fd[i]);
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = 
      array_schema.clone(result_array_name);
  update_fragment_info(result_array_schema);
}
*/

//TODO remove
/*
void Executor::update(const std::string& filename, 
                      const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name(); 

  // Check with the consolidator if the array exists (the consolidator keeps
  // book-keeping info about each existing array).
  if(!consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot update array: "
                            "array does not exist.");

  // Create from the CSV file the n-th array fragment
  // (see class Consolidator for details about the array fragments)
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema);
   std::string fragment_name = consolidator_->get_next_fragment_name(ad);
  ArraySchema fragment_schema = array_schema.clone(fragment_name);
  loader_->load(filename, fragment_schema);

  // Update the fragment information of the array at the consolidator
  consolidator_->add_fragment(ad);  
  consolidator_->close_array(ad);
}
*/

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

  // Get fragment name
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_name);
  std::string fragment_name = consolidator_->get_next_fragment_name(ad);

  // Load the n-th fragment
  try {
    loader_->load(filename, array_name, fragment_name);
  } catch(LoaderException& le) {
    throw ExecutorException(le.what());
  }

  // Update the fragment information of the array at the consolidator
  consolidator_->add_fragment(ad);  
  consolidator_->close_array(ad);
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

void Executor::create_workspace() const {
  struct stat st;
  stat(workspace_.c_str(), &st);

  // If the workspace does not exist, create it
  if(!S_ISDIR(st.st_mode)) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

// TODO remove

/*
std::vector<std::string> Executor::get_fragment_suffixes(
    const ArraySchema& array_schema) const {
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema);
  std::vector<std::string> fragment_suffixes = 
      consolidator_->get_all_fragment_names(ad);  
  assert(fragment_suffixes.size() > 0);
  consolidator_->close_array(ad);

  return fragment_suffixes;
}
*/

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

void Executor::update_fragment_info(const std::string& array_name) const {
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_name);
  consolidator_->add_fragment(ad);  
  consolidator_->close_array(ad);
}

