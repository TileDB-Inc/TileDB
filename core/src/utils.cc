/**
 * @file   tile.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements useful (global) functions.
 */

#include "utils.h"
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <iostream>

std::string absolute_path(const std::string& path) {
  if(path[0] == '~') 
    return std::string(getenv("HOME")) + path.substr(1, path.size()-1);
  else
    return path;
} 

void create_directory(const std::string& dirname) {
  struct stat st;
  bool dir_exists = stat(dirname.c_str(), &st) == 0 && S_ISDIR(st.st_mode);

  // If the directory does not exist, create it
  if(!dir_exists) { 
    int dir_flag = mkdir(dirname.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

void delete_directory(const std::string& dirname)  {
  std::string filename; 

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    filename = dirname + "/" + next_file->d_name;
    remove(filename.c_str());
  } 
  
  closedir(dir);
  rmdir(dirname.c_str());
}

void expand_buffer(void*& buffer, size_t size) {
  void* temp = malloc(2*size);
  memcpy(temp, buffer, size);
  free(buffer);
  buffer = temp;
}

void expand_mbr(const ArraySchema* array_schema,
                const void* coords, void* mbr) {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const std::type_info* type = array_schema->type(attribute_num);

  if(*type == typeid(int)) 
    expand_mbr(static_cast<const int*>(coords), 
               static_cast<int*>(mbr), dim_num);
  else if(*type == typeid(int64_t)) 
    expand_mbr(static_cast<const int64_t*>(coords), 
               static_cast<int64_t*>(mbr), dim_num);
  else if(*type == typeid(float)) 
    expand_mbr(static_cast<const float*>(coords), 
               static_cast<float*>(mbr), dim_num);
  else if(*type == typeid(double)) 
    expand_mbr(static_cast<const double*>(coords), 
               static_cast<double*>(mbr), dim_num);
}

template<class T>
void expand_mbr(const T* coords, T* mbr, int dim_num) {
  if(mbr == NULL) { 
    mbr = new T[2*dim_num];

    for(int i=0; i<dim_num; ++i) {
      mbr[2*i] = coords[i]; 
      mbr[2*i+1] = coords[i]; 
    }
  } else {
    for(int i=0; i<dim_num; ++i) {
      // Update lower bound on dimension i
      if(mbr[2*i] > coords[i])
        mbr[2*i] = coords[i];

      // Update upper bound on dimension i
      if(mbr[2*i+1] < coords[i])
        mbr[2*i+1] = coords[i];   
    }	
  }
}

bool file_exists(const std::string& filename) {
  std::string abs_filename = absolute_path(filename);

  int fd = open(abs_filename.c_str(), O_RDONLY);
  if(fd == -1)
    return false; 
   
  close(fd);

  return true;
}

void init_mbr(const ArraySchema* array_schema,
              const void* coords, void*& mbr) {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const std::type_info* type = array_schema->type(attribute_num);

  mbr = malloc(2*array_schema->cell_size(attribute_num));

  if(*type == typeid(int)) 
    init_mbr(static_cast<const int*>(coords), 
             static_cast<int*>(mbr), dim_num);
  else if(*type == typeid(int64_t)) 
    init_mbr(static_cast<const int64_t*>(coords), 
             static_cast<int64_t*>(mbr), dim_num);
  else if(*type == typeid(float)) 
    init_mbr(static_cast<const float*>(coords), 
             static_cast<float*>(mbr), dim_num);
  else if(*type == typeid(double)) 
    init_mbr(static_cast<const double*>(coords), 
             static_cast<double*>(mbr), dim_num);
}

template<class T>
void init_mbr(const T* coords, T* mbr, int dim_num) {
  for(int i=0; i<dim_num; ++i) {
    mbr[2*i] = coords[i]; 
    mbr[2*i+1] = coords[i]; 
  }
}

template<class T>
bool inside_range(const T* point, const T* range, int dim_num) {
  for(int i=0; i<dim_num; ++i) {
    if(point[i] < range[2*i] || point[i] > range[2*i+1])
      return false;
  }
  return true;
}

bool path_exists(const std::string& path) {
  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

template<class T>
std::pair<bool, bool> overlap(const T* r1, const T* r2, int dim_num) {
  // Overlap type per dimension
  bool* partial = new bool[dim_num];
  bool* full = new bool[dim_num];

  bool overlap = true; // True if the inputs overlap (partially or fully)
  bool full_overlap = true; // True if the inputs fully overlap   

  // Determine overlap per dimension
  for(int j=0; j<dim_num; ++j) {
    partial[j] = false;
    full[j] = false;
    if(r1[2*j] >= r2[2*j] && r1[2*j+1] <= r2[2*j+1])
      full[j] = true;
    else if(r2[2*j] >= r1[2*j] && r2[2*j] <= r1[2*j+1] || 
            r2[2*j+1] >= r1[2*j] && r2[2*j+1] <= r1[2*j+1])
      partial[j] = true;
  }

  // Determine overlap
  for(int j=0; j<dim_num; j++) {
    if(!partial[j] && !full[j]) {
      overlap = false;
      break;
    }
  
    if(partial[j])
      full_overlap = false;
  }

  delete [] partial;
  delete [] full;

  return std::pair<bool, bool>(overlap, full_overlap);
}

// Explicit template instantiations
template void expand_mbr<int>(
    const int* coords, int* mbr, int dim_num);
template void expand_mbr<int64_t>(
    const int64_t* coords, int64_t* mbr, int dim_num);
template void expand_mbr<float>(
    const float* coords, float* mbr, int dim_num);
template void expand_mbr<double>(
    const double* coords, double* mbr, int dim_num);

template void init_mbr<int>(
    const int* coords, int* mbr, int dim_num);
template void init_mbr<int64_t>(
    const int64_t* coords, int64_t* mbr, int dim_num);
template void init_mbr<float>(
    const float* coords, float* mbr, int dim_num);
template void init_mbr<double>(
    const double* coords, double* mbr, int dim_num);

template std::pair<bool, bool> overlap<int>(
    const int* r1, const int* r2, int dim_num);
template std::pair<bool, bool> overlap<int64_t>(
    const int64_t* r1, const int64_t* r2, int dim_num);
template std::pair<bool, bool> overlap<float>(
    const float* r1, const float* r2, int dim_num);
template std::pair<bool, bool> overlap<double>(
    const double* r1, const double* r2, int dim_num);

template bool inside_range<int>(
    const int* point, const int* range, int dim_num);
template bool inside_range<int64_t>(
    const int64_t* point, const int64_t* range, int dim_num);
template bool inside_range<float>(
    const float* point, const float* range, int dim_num);
template bool inside_range<double>(
    const double* point, const double* range, int dim_num);
