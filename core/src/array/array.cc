/**
 * @file   array.cc
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
 * This file implements the Array class.
 */

#include "array.h"
#include "string.h"
#include "utils.h"
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <sstream>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#ifdef GNU_PARALLEL
  #include <parallel/algorithm>
  #define SORT(first, last, comp) __gnu_parallel::sort((first), (last), (comp))
#else
  #include <algorithm>
  #define SORT(first, last, comp) std::sort((first), (last), (comp))
#endif

// Macro for printing error messages in stderr in VERBOSE mode
#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << "[TileDB::Array] Error: " << x << ".\n"
#  define PRINT_WARNING(x) std::cerr << "[TileDB::Array] Warning: " \
                                     << x  << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Array::Array(
    const std::string& dirname,
    const ArraySchema* array_schema, 
    Mode mode) 
    : array_schema_(array_schema), 
      dirname_(dirname), 
      mode_(mode) {
  finalized_ = false;
  created_successfully_ = false;

  if(array_schema_ == NULL) {
    PRINT_ERROR("Cannot construct array; Invalid array schema");
    return;
  }

  if(mode_ == READ || mode_ == CONSOLIDATE) {
    // Raname fragments with temporary names starting with "__"
    if(mode_ == CONSOLIDATE && fragments_rename_tmp())
        return;
    // Open fragments
    if(fragments_open())
      return;
  } else {
    // Initialize new fragment
    new_fragment_init();
  }

  // Success
  created_successfully_ = true;
}

bool Array::created_successfully() const {
  return created_successfully_;
}

int Array::finalize() {
  if(fragments_.size() == 0)
    return 0;

  if(mode_ == WRITE) {
    // Commit (the only) fragment by deleting it
    std::string fragment_dirname = fragments_[0]->dirname();
    delete fragments_[0];
    fragments_.pop_back();

    // If fragment is empty, delete its directory
    if(directory_is_empty(fragment_dirname) && 
       directory_delete(fragment_dirname))
      return -1;

    // Rename new fragment directory
    std::string new_dirname = dirname_ + "/" + 
                              fragment_dirname.substr(dirname_.size() + 2);

    if(rename(fragment_dirname.c_str(), new_dirname.c_str())) {
      PRINT_ERROR(std::string("Cannot rename new fragment directory - ") +
                  strerror(errno));
      return -1;
    }
  }

  // Clean up
  fragments_close();
  if(array_schema_ != NULL) {
    delete array_schema_;
    array_schema_ = NULL;
  }

  finalized_ = true;

  // Success
  return 0;
}

Array::~Array() {
  if(!finalized_) {
    PRINT_WARNING("Array not finalized. Finalizing now.");
    finalize();
  }
}

/******************************************************
******************** ACCESSORS ************************
******************************************************/

const std::string& Array::array_name() const {
  return array_schema_->array_name();
}

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

const std::string& Array::dirname() const {
  return dirname_;
}

bool Array::is_empty() const {
  return !fragments_.size();
}

int Array::fragment_num() const {
  return fragments_.size();
}

Array::Mode Array::mode() const {
  return mode_;
}

/******************************************************
******************** ITERATORS ************************
******************************************************/

FragmentConstTileIterator* Array::begin(
    int fragment_id, int attribute_id) const {
  if(mode_ != READ && mode_ != CONSOLIDATE) {
    PRINT_ERROR("Failed to begin tile iterator because of invalid array mode.");
    return NULL;
  }

  return fragments_[fragment_id]->begin(attribute_id);
}

FragmentConstReverseTileIterator* Array::rbegin(
    int fragment_id, int attribute_id) const {
  if(mode_ != READ && mode_ != CONSOLIDATE) {
    PRINT_ERROR("Failed to begin tile iterator because of invalid array mode.");
    return NULL;
  }

  return fragments_[fragment_id]->rbegin(attribute_id);
}

/******************************************************
********************* MUTATORS ************************
******************************************************/

template<class T>
int Array::cell_write(const void* cell) const {
  if(mode_ != WRITE) {
    PRINT_ERROR("Failed to write to array because of invalid array mode.");
    return -1;
  }

  assert(fragments_.size() == 1);

  return fragments_[0]->cell_write<T>(cell);
}

template<class T>
int Array::cell_write_sorted(const void* cell, bool without_coords) {
  if(mode_ != WRITE) {
    PRINT_ERROR("Failed to write to array because of invalid array mode.");
    return -1;
  }

  assert(fragments_.size() == 1);

  return fragments_[0]->cell_write_sorted<T>(cell, without_coords);
}

int Array::close_forced() {
  // If in WRITE mode, clean up and delete the created (last) fragment
  if(mode_ == WRITE || mode_ == CONSOLIDATE) 
    if(directory_delete(fragments_.back()->dirname()))
      return -1;

  // Close the fragments
  fragments_close();

  // Delete array schema
  if(array_schema_ != NULL) {
    delete array_schema_;
    array_schema_ = NULL;
  }

  return 0;
}

int Array::consolidate() {
  // Error
  if(fragments_.size() == 0) {
    PRINT_ERROR("No fragments to consolidate");
    return -1;
  }

  // Trivial case - no consolidation needed
  if(fragments_.size() == 1)
    return 0;

  std::vector<std::pair<int,int> > fragments_to_consolidate;
  std::vector<std::string> new_fragment_names; 

  // Identify what fragments need to be consolidated, along with their
  // new names
  if(what_fragments_to_consolidate(
         fragments_to_consolidate, new_fragment_names) == -1)
    return -1;

  // Consolidate the fragments
  const std::type_info* type = array_schema_->coords_type();
  if(type == &typeid(int)) {
    for(int i=0; i<fragments_to_consolidate.size(); ++i)
      if(consolidate<int>(
             fragments_to_consolidate[i], new_fragment_names[i]))
        return -1;
  } else if(type == &typeid(int64_t)) {
    for(int i=0; i<fragments_to_consolidate.size(); ++i)
      if(consolidate<int64_t>(
             fragments_to_consolidate[i], new_fragment_names[i]))
        return -1;
  } else if(type == &typeid(float)) {
    for(int i=0; i<fragments_to_consolidate.size(); ++i)
      if(consolidate<float>(
             fragments_to_consolidate[i], new_fragment_names[i]))
        return -1;
  } else if(type == &typeid(double)) {
    for(int i=0; i<fragments_to_consolidate.size(); ++i)
      if(consolidate<double>(
             fragments_to_consolidate[i], new_fragment_names[i]))
        return -1;
  }

  // Close and reopen the fragments
  for(int i=0; i<fragments_.size(); ++i)
    delete fragments_[i];
  fragments_.clear(); 

  // Raname fragments at level 0 of the merge tree
  if(fragments_rename_level_0()) 
    return -1;

  if(fragments_open()) 
    return -1;

  // If consolidation resulted in a single empty fragment, 
  // delete the fragment directory
  if(fragments_.size() == 1 && directory_is_empty(fragments_[0]->dirname()))
    if(directory_delete(fragments_[0]->dirname()))
      return -1;

  return 0;
}

int Array::new_fragment_init() {
  // TODO: Error messages

  // Create a temporary fragment name of the form "__pid_timestamp"
  std::stringstream fragment_name;
  struct timeval tp;
  gettimeofday(&tp, NULL);
  uint64_t ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;
  fragment_name << ".__" << getpid() << "_" << ms; 

  // Check if dense
  bool dense = false;
  if(!has_fragments() && array_schema_->dense())
    dense = true;

  // Create a new Fragment object
  fragments_.push_back(
      new Fragment(
          dirname_ + "/" + fragment_name.str(), 
          array_schema_, 
          fragment_name.str(),
          dense));

  return 0;
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

template<class T>
int Array::consolidate(
    const std::pair<int,int>& fragments_to_consolidate,
    const std::string& new_fragment_name) {
  // TODO: Handle close_forced and delete created fragments

  // Determine whether the resulting fragment will contain
  // deletions (i.e., if there are more than one fragments
  // eventually.
  bool return_del = (fragments_to_consolidate.first == 0 &&
                     fragments_to_consolidate.second == fragments_.size()-1) 
                         ? false : true;

  // Prepare list of fragment ids
  std::vector<int> fragment_ids;
  for(int i=fragments_to_consolidate.first; 
      i<=fragments_to_consolidate.second; ++i)
    fragment_ids.push_back(i);  

  // Initialize cell iterator
  ArrayConstCellIterator<T> cell_it =
      ArrayConstCellIterator<T>(this, fragment_ids, return_del);

  // Create a new fragment for the cells of the consolidated fragments
  Fragment* new_fragment = 
      new Fragment(
          dirname_ + "/" + new_fragment_name,
          array_schema_, 
          new_fragment_name);

  // TODO: Check correct construction here

  // Write cells into the new fragment
  for(; !cell_it.end(); ++cell_it) { 
    if(new_fragment->cell_write_sorted<T>(*cell_it))
      return -1;
  }

  // Delete directories of consolidated fragments 
  for(int i=fragments_to_consolidate.first; 
      i<=fragments_to_consolidate.second; ++i) { 
    if(directory_delete(fragments_[i]->dirname()))
      return -1;
  }

  // Delete the new fragment (this commits also the created fragment data)
  delete new_fragment;

  return 0;
}

int Array::fragment_name_parse(
    const std::string& fragment_name,
    std::pair<int,int>& x_y) const {
  std::string x_str, y_str; 

  // Get the "x" and "y" string parts of fragment name "x_y"
  for(int i=0; i<fragment_name.size(); ++i) {
    if(fragment_name[i] == '_') {
      x_str = fragment_name.substr(0, i);
      y_str = fragment_name.substr(i+1,fragment_name.size()-i);
      break;
    }
  }

  // Check fragment name correctness
  if(!is_non_negative_integer(x_str.c_str()) || 
     !is_non_negative_integer(y_str.c_str())) {
    PRINT_ERROR("Invalid fragment name");
    return -1;
  }

  // Convert name parts to integer pair
  sscanf(x_str.c_str(), "%d", &(x_y.first)); 
  sscanf(y_str.c_str(), "%d", &(x_y.second)); 
 
  return 0;
}

void Array::fragments_close() {
  for(int i=0; i<fragments_.size(); ++i)
    delete fragments_[i];
  fragments_.clear(); 
}

bool Array::has_fragments() const {
  bool ret = false;

  struct dirent *next_file;
  DIR* dir = opendir(dirname_.c_str());
  if(dir == NULL) 
    return false;

  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") &&
       strcmp(next_file->d_name, "..") &&
       is_dir(dirname_ + "/" + next_file->d_name) &&
       !metadata_exists(dirname_ + "/" + next_file->d_name)) { 
      ret = true;
      break;
    }
  } 
 
  // Close directory 
  if(closedir(dir)) 
    return false;

  return ret;
}

int Array::fragment_names_get(std::vector<std::string>& fragment_names) const {
  fragment_names.clear(); 
  struct dirent *next_file;
  DIR* dir = opendir(dirname_.c_str());
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory: ") + 
                strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") &&
       strcmp(next_file->d_name, "..") &&
       is_dir(dirname_ + "/" + next_file->d_name) &&
       !metadata_exists(dirname_ + "/" + next_file->d_name))  
      fragment_names.push_back(next_file->d_name);
  } 
 
  // Close directory 
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close directory: ") + 
                strerror(errno));
    return -1;
  }

  return 0;
}

int Array::fragments_open() {
  // Get fragment names
  std::vector<std::string> fragment_names;
  if(fragment_names_get(fragment_names))
    return -1;

  // Sort the fragment names based on their names 
  if(fragment_names_sort(fragment_names))
    return -1;

  // Create a new Fragment object for each fragment name
  // TODO: check for errors in initialization of fragments
  // NOTE: In the case of dense arrays, the first fragment is
  // always dense
  for(int i=0; i<fragment_names.size(); ++i) { 
    fragments_.push_back(
        new Fragment(dirname_ + "/" + fragment_names[i],
                     array_schema_, 
                     fragment_names[i],
                     fragment_is_dense(dirname_ + "/" + fragment_names[i])));
  }

  return 0;
}

bool Array::fragment_is_dense(const std::string& fragment) const {
  if(array_schema_->dense() && 
     !is_file(fragment + "/" + AS_COORDINATES_NAME + TILE_DATA_FILE_SUFFIX))
    return true;
  else
    return false;
}

bool Array::metadata_exists(
    const std::string& metadata_name,
    bool real_path) const {
  // Get real metadata directory name
  std::string metadata_name_real;
  if(real_path)
    metadata_name_real = metadata_name;
  else
    metadata_name_real = ::real_path(metadata_name);

  // Check if metadata schema file exists
  std::string filename = metadata_name_real + "/" +
                         METADATA_SCHEMA_FILENAME;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

int Array::fragment_names_sort(std::vector<std::string>& fragment_names) const {
  // The fragment names are in the form "x_y", where "x" is the tree level
  // and "y" is the node in that level. The following vector stores a list
  // of the following tuples: (x,(y,name))
  std::vector<std::pair<int,std::pair<int,std::string> > > 
      fragment_names_sorted;

  // The following stores the (x,y) of fragment name "x_y" as integers
  std::pair<int,int> x_y;

  // The following stores the (y,name) of fragment name "x_y" 
  std::pair<int,std::string> y_name; 

  // The following stores the (x,(y,name)) of fragment name "x_y" 
  std::pair<int,std::pair<int,std::string> > x_y_name;

  // Vector for the unconsolidated fragments (starting with "__")
  std::vector<std::string> unconsolidated_fragments;

  // Parse the names into a new structure, which is "sortable"
  for(int i=0; i<fragment_names.size(); ++i) {
    if(metadata_exists(dirname_ + "/" + fragment_names[i])) {
      continue;
    } else if(starts_with(fragment_names[i], "__")) {
      unconsolidated_fragments.push_back(fragment_names[i]);
      continue;
    } else {
      if(fragment_name_parse(fragment_names[i], x_y))
        return -1;
      y_name = std::pair<int,std::string>(x_y.second, fragment_names[i]);
      x_y_name = std::pair<int,std::pair<int,std::string> >(x_y.first, y_name);
      fragment_names_sorted.push_back(x_y_name); 
    }
  }

  // Sort
  SORT(fragment_names_sorted.begin(), fragment_names_sorted.end(), Array::greater_smaller);

  // Write the sorted fragment names back to the input argument
  fragment_names.clear();
  for(int i=0; i<fragment_names_sorted.size(); ++i)
    fragment_names.push_back(fragment_names_sorted[i].second.second); 
  for(int i=0; i<unconsolidated_fragments.size(); ++i)
    fragment_names.push_back(unconsolidated_fragments[i]); 

  return 0;
}

bool Array::greater_smaller(
    const std::pair<int, std::pair<int, std::string> >& a,
    const std::pair<int, std::pair<int, std::string> >& b) {
  if(a.first > b.first) {
    return true;
  } else if(a.first < b.first) {
    return false;
  } else {
    if(a.second.first < b.second.first)
      return true;
    else
      return false;
  }
}

int Array::fragments_rename_level_0() const {
  // Find the smallest node id for level 0 in the current merge tree,
  // as well as all fragment names starting with "0_"
  std::vector<std::pair<int, std::string> > fragment_names_level_0;
  int c = INT_MAX; 
  struct dirent *next_file;
  DIR* dir = opendir(dirname_.c_str());
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory: ") + 
                strerror(errno));
    return -1;
  }

  std::pair<int,int> x_y;
  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") &&
       strcmp(next_file->d_name, "..") &&
       is_dir(dirname_ + "/" + next_file->d_name) &&
       starts_with(next_file->d_name, "0_")) {
      if(fragment_name_parse(next_file->d_name, x_y)) {
        closedir(dir);
        return -1;
      }
      if(x_y.second < c)
        c = x_y.second;
      fragment_names_level_0.push_back(
          std::pair<int, std::string>(x_y.second, next_file->d_name));
    }
  } 
 
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close directory: ") + 
                strerror(errno));
    return -1;
  }

  // No fragments at level 0
  if(c == INT_MAX)
    return 0;

  // Sort the fragment names based on their corresponding node id
  SORT(fragment_names_level_0.begin(), fragment_names_level_0.end(), 
       (std::less<std::pair<int,std::string> >()));

  // Rename the fragment names at level 0
  for(int i=0; i<fragment_names_level_0.size(); ++i) {
    std::string dirname_old = dirname_ + "/" + fragment_names_level_0[i].second; 
    std::stringstream dirname_new_ss;
    dirname_new_ss << dirname_ + "/0_" << (fragment_names_level_0[i].first - c + 1);
    if(rename(dirname_old.c_str(), dirname_new_ss.str().c_str())) {
      PRINT_ERROR("Cannot rename level-0 fragment name");
      return -1;
    }
  }

  return 0;
}


int Array::fragments_rename_tmp() const {
  // Find the largest node id for level 0 in the current merge tree
  int c = -1;
  struct dirent *next_file;
  DIR* dir = opendir(dirname_.c_str());
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory: ") + 
                strerror(errno));
    return -1;
  }

  std::pair<int,int> x_y;
  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") &&
       strcmp(next_file->d_name, "..") &&
       is_dir(dirname_ + "/" + next_file->d_name) &&
       starts_with(next_file->d_name, "0_")) {
      if(fragment_name_parse(next_file->d_name, x_y)) {
        closedir(dir);
        return -1;
      }
      if(x_y.second > c)
        c = x_y.second;
    }
  } 

  // Set next node id
  if(c == -1)
    c = 1;
  else
    ++c;
 
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close directory: ") + 
                strerror(errno));
    return -1;
  }

  // Rename the temporary fragment names
  dir = opendir(dirname_.c_str());
  if(dir == NULL) {
    PRINT_ERROR(std::string("Cannot open array directory: ") + 
                strerror(errno));
    return -1;
  }

  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") &&
       strcmp(next_file->d_name, "..") &&
       is_dir(dirname_ + "/" + next_file->d_name) &&
       starts_with(next_file->d_name, "__")) {
      std::stringstream dirname_new_ss; 
      std::string dirname_old;
      dirname_new_ss << dirname_ << "/0_" << (c++);
      dirname_old = dirname_ + "/" + next_file->d_name;
      if(rename(dirname_old.c_str(), dirname_new_ss.str().c_str())) {
        PRINT_ERROR("Cannot rename temporary fragment name");
        return -1;
      }
    }
  } 
 
  if(closedir(dir)) {
    PRINT_ERROR(std::string("Cannot close directory: ") + 
                strerror(errno));
    return -1;
  }

  return 0;
}

int Array::what_fragments_to_consolidate( 
    std::vector<std::pair<int,int> >& fragments_to_consolidate,
    std::vector<std::string>& new_fragment_names) const{
  // Init input
  fragments_to_consolidate.clear();
  new_fragment_names.clear();

  // Auxiliary variables
  int consolidation_step = array_schema_->consolidation_step();
  int total_batch_num = 0;

  // Get the integer (x,y) pairs of all fragment names "x_y"
  std::vector<std::pair<int,int> > x_y;
  x_y.resize(fragments_.size());
  for(int i=0; i<fragments_.size(); ++i) {
    if(fragment_name_parse(fragments_[i]->fragment_name(), x_y[i]))
      return -1;
  }

  // ***************** consolidation step == 1 ***************** //
  if(consolidation_step == 1) {
    // Compute the number of total batches that have ever been loaded   
    for(int i=0; i<x_y.size(); ++i) {
      if(x_y[i].first == 1)  // Fragment encompasses multiple batches
        total_batch_num += x_y[i].second;      
      else                   // Fragment encompasses single batch 
        ++total_batch_num;
    }

    // All fragments must be consolidated
    fragments_to_consolidate.push_back(
        std::pair<int,int>(0,fragments_.size()-1));
    // Compute new fragment name of the form "1_<total_batch_num>"
    std::stringstream new_fragment_name_ss; 
    new_fragment_name_ss << "1_" << total_batch_num;
    new_fragment_names.push_back(new_fragment_name_ss.str());
  } else { 
  // ***************** consolidation step != 1 ***************** //
    // Compute the total number of batches that have ever been loaded   
    for(int i=0; i<x_y.size(); ++i) 
      total_batch_num += pow(double(consolidation_step), double(x_y[i].first));
    // Decompose into multiple potential consolidations
    // For instance, if batch_num=16 and consolidation_step=3, then
    // the decomposition into powers of consolidation_step is 
    // 9 + 3 + 3 + 1, which implies that there should be up to 4 consolidations:
    // first fragments that collectively encompass 9 batches (if such 
    // a single consolidated fragment does not exist), then fragments
    // that encompass 3, then 3 again, and then a single fragment (trivial case,
    // no consolidation needed here). Instead of keeping track of the batches,
    // we keep track of (level, node id) pairs, e.g., in the above example,
    // (2,1), (1,1), (1,2), (0,1). This reflects the nodes that will be
    // created in the final merge tree and, thus, we store it in a structure
    // called "merge_tree". Note also that these pairs imply the fragment
    // directory names that will be created, i.e., "2_1", "1_1", etc.
    std::vector<std::pair<int, int> > merge_tree;
    int max_log, max_power, node_id, current_level = -1;
    while(total_batch_num != 0) {
      max_log = floor(log(double(total_batch_num)) / log(double(consolidation_step))); 
      if(max_log != current_level) {
        current_level = max_log;
        node_id = 1;
      }
      merge_tree.push_back(std::pair<int,int>(max_log,node_id++));
      max_power = floor(pow(double(consolidation_step), double(max_log)));
      total_batch_num -= max_power; 
      assert(total_batch_num >= 0);
    }
    // For each merge tree node, find the fragment ids that will comprise it
    // after their consolidation
    int current_batch_num = 0;
    int merge_tree_node_batch_num = (int) pow(double(consolidation_step), 
                                              double(merge_tree[0].first));
    int merge_tree_i=0; // Index over merge_tree 
    // The following orient the range of fragment ids that will to be
    // consolidated for each merge tree node
    int fragment_range_first = 0, fragment_range_second = -1;
    for(int i=0; i<x_y.size();++i) {
      // Update current batch number
      current_batch_num += 
          (int) pow(double(consolidation_step), double(x_y[i].first));
      // Update the output lists
      if(current_batch_num <= merge_tree_node_batch_num) 
        ++fragment_range_second;
       
      if(current_batch_num >= merge_tree_node_batch_num) { 
        // We need to close this consolidation, but only if there are more than
        // one fragments involved 
        if(fragment_range_first != fragment_range_second) {
          fragments_to_consolidate.push_back(
              std::pair<int,int>(fragment_range_first,fragment_range_second));
          std::stringstream new_fragment_name_ss; 
          new_fragment_name_ss << merge_tree[merge_tree_i].first << "_" 
                               << merge_tree[merge_tree_i].second;
          new_fragment_names.push_back(new_fragment_name_ss.str()); 
        }
        fragment_range_first = fragment_range_second + 1; 
        ++merge_tree_i;
        merge_tree_node_batch_num = 
            (int) pow(double(consolidation_step), 
                      double(merge_tree[merge_tree_i].first));
        current_batch_num = 0;
      }
    }
  }
 
  return 0; 
}

// Explicit template instantiations
template int Array::cell_write<int>(const void* cell) const;
template int Array::cell_write<int64_t>(const void* cell) const;
template int Array::cell_write<float>(const void* cell) const;
template int Array::cell_write<double>(const void* cell) const;

template int Array::cell_write_sorted<int>(const void* cell, bool without_coords);
template int Array::cell_write_sorted<int64_t>(const void* cell, bool without_coords);
template int Array::cell_write_sorted<float>(const void* cell, bool without_coords);
template int Array::cell_write_sorted<double>(const void* cell, bool without_coords);
