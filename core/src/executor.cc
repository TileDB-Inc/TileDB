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
****************** PRIVATE METHODS ********************
******************************************************/

// TODO ============== Handle all other queries similarly to export

// TODO ============== Check query processor examples

// TODO ============== New examples


void Executor::export_to_CSV(const std::string& filename, 
                             const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  
  // Check with the consolidator if the array exists (the consolidator keeps
  // book-keeping info about each existing array).
  if(!consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot export array: "
                            "array does not exist.");

  // Get fragment suffixes
  const Consolidator::ArrayDescriptor* c_ad = 
      consolidator_->open_array(array_schema);
  std::vector<std::string> fragment_suffixes = 
      consolidator_->get_all_fragment_suffixes(c_ad);  
  assert(fragment_suffixes.size() > 0);
  consolidator_->close_array(c_ad);

  // Temporary check
  if(fragment_suffixes.size() == 1)  { 
    const StorageManager::ArrayDescriptor* s_ad = 
        storage_manager_->open_array(array_name + std::string("_")  
                                                + fragment_suffixes[0]);
    query_processor_->export_to_CSV(s_ad, filename);
    storage_manager_->close_array(s_ad);
  } else {
    throw ExecutorException("[Executor] Cannot export array: "
                            "query over multiple array fragments currently not "
                            "supported.");
  }
}

void Executor::load(const std::string& filename, 
                    const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name(); 

  // Check with the consolidator if the array exists (the consolidator keeps
  // book-keeping info about each existing array).
  if(consolidator_->array_exists(array_name))
    throw ExecutorException("[Executor] Cannot load array: "
                            "array already exists.");

  // Create from the CSV file the 0-th (i.e., first) array fragment
  // (see class Consolidator for details about the array fragments)
  ArraySchema fragment_schema = array_schema.clone(array_name + "_0_0");
  loader_->load(filename, fragment_schema);

  // Update the fragment information of the array at the consolidator
  const Consolidator::ArrayDescriptor* ad = 
      consolidator_->open_array(array_schema);
  consolidator_->add_fragment(ad);  
  consolidator_->close_array(ad);
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
  uint64_t n = consolidator_->get_next_fragment_seq(ad);
  std::stringstream fragment_schema_name;
  fragment_schema_name << array_name << "_" << n << "_" << n;
  ArraySchema fragment_schema = array_schema.clone(fragment_schema_name.str());
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


