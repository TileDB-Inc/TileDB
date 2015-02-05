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
#include <stdlib.h>
#include <iostream>
#include <sstream>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Executor::Executor(const std::string& workspace) { 
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
    const StorageManager::ArrayDescriptor* ad = 
        storage_manager_->open_array(array_name + std::string("_")  
                                                + fragment_suffixes[0]);
    query_processor_->export_to_CSV(ad, filename);
    storage_manager_->close_array(ad);
  } else { // Mulutple fragments
    std::vector<const StorageManager::ArrayDescriptor*> ad;
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      ad.push_back(storage_manager_->open_array(array_name + std::string("_") +
                                                fragment_suffixes[i]));
    query_processor_->export_to_CSV(ad, filename);
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      storage_manager_->close_array(ad[i]);
  }
}

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
    const StorageManager::ArrayDescriptor* ad = 
        storage_manager_->open_array(array_name + std::string("_")  
                                                + fragment_suffixes[0]);
    query_processor_->filter(ad, expression, result_array_name + "_0_0");
    storage_manager_->close_array(ad);
  } else { // Mulutple fragments
    std::vector<const StorageManager::ArrayDescriptor*> ad;
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      ad.push_back(storage_manager_->open_array(array_name + std::string("_") +
                                                fragment_suffixes[i]));
    query_processor_->filter(ad, expression, result_array_name + "_0_0");
    for(unsigned int i=0; i<fragment_suffixes.size(); ++i) 
      storage_manager_->close_array(ad[i]);
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = 
      array_schema.clone(result_array_name);
  update_fragment_info(result_array_schema);
}

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
    const StorageManager::ArrayDescriptor* ad_A = 
        storage_manager_->open_array(array_name_A + std::string("_")  
                                                + fragment_suffixes_A[0]);
    const StorageManager::ArrayDescriptor* ad_B = 
        storage_manager_->open_array(array_name_B + std::string("_")  
                                                + fragment_suffixes_B[0]);
    try {
      query_processor_->join(ad_A, ad_B, result_array_name + "_0_0");
    } catch(QueryProcessorException& qe) {
      storage_manager_->close_array(ad_A);
      storage_manager_->close_array(ad_B);
      throw ExecutorException(qe.what());
    }
    storage_manager_->close_array(ad_A);
    storage_manager_->close_array(ad_B);
  } else { // Mulutple fragments TODO
    throw ExecutorException("[Executor] Cannot join arrays: "
                            "query over multiple array fragments currently not "
                            "supported.");
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = ArraySchema::create_join_result_schema(
                                   array_schema_A, 
                                   array_schema_B, 
                                   result_array_name);
  update_fragment_info(result_array_schema);
}

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
    const StorageManager::ArrayDescriptor* ad = 
        storage_manager_->open_array(array_name + std::string("_")  
                                                + fragment_suffixes[0]);
    query_processor_->nearest_neighbors(ad, q, k, result_array_name + "_0_0");
    storage_manager_->close_array(ad);
  } else { // Mulutple fragments TODO
    throw ExecutorException("[Executor] Cannot perform nearest neighbor search:"
                            " query over multiple array fragments currently not"
                            " supported.");
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = 
      array_schema.clone(result_array_name);
  update_fragment_info(result_array_schema);
}

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
    const StorageManager::ArrayDescriptor* ad = 
        storage_manager_->open_array(array_name + std::string("_")  
                                                + fragment_suffixes[0]);
    query_processor_->subarray(ad, range, result_array_name + "_0_0");
    storage_manager_->close_array(ad);
  } else { // Mulutple fragments TODO
    throw ExecutorException("[Executor] Cannot perform subarray: "
                            "query over multiple array fragments currently not "
                            "supported.");
  }

  // Update the fragment information of result array at the consolidator
  ArraySchema result_array_schema = 
      array_schema.clone(result_array_name);
  update_fragment_info(result_array_schema);
}

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

std::vector<std::string> Executor::get_fragment_suffixes(
    const ArraySchema& array_schema) const {
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema);
  std::vector<std::string> fragment_suffixes = 
      consolidator_->get_all_fragment_suffixes(ad);  
  assert(fragment_suffixes.size() > 0);
  consolidator_->close_array(ad);

  return fragment_suffixes;
}

bool Executor::path_exists(const std::string& path) const {
  struct stat st;
  stat(path.c_str(), &st);
  return S_ISDIR(st.st_mode);
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
  assert(path_exists(workspace_));
 
  workspace_ += "/Executor";
}

void Executor::update_fragment_info(const ArraySchema& array_schema) const {
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema);
  consolidator_->add_fragment(ad);  
  consolidator_->close_array(ad);
}

