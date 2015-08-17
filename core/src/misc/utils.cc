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
#include <algorithm>
#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

bool ends_with(const std::string& value, const std::string& ending) {
  if (ending.size() > value.size())
    return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

std::string absolute_path(const std::string& path) {
  if(path[0] == '~') 
    return std::string(getenv("HOME")) + path.substr(1, path.size()-1);
  else
    return path;
}

template<class T>
void convert(const double* a, T* b, int size) {
  for(int i=0; i<size; ++i) {
    b[i] = T(a[i]);
  }
}

int create_directory(const std::string& dirname) {
  // If the directory does not exist, create it
  if(!is_dir(dirname))  
    return mkdir(dirname.c_str(), S_IRWXU);
}

void delete_directory(const std::string& dirname)  {
  std::string filename; 

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    filename = dirname + "/" + next_file->d_name;
    remove(filename.c_str());
  } 
  
  closedir(dir);
  rmdir(dirname.c_str());
}

template<class T>
bool duplicates(const std::vector<T>& v) {
  std::set<T> s(v.begin(), v.end());

  return s.size() != v.size(); 
}

void expand_buffer(void*& buffer, size_t size) {
  void* temp = malloc(2*size);
  memcpy(temp, buffer, size);
  free(buffer);
  buffer = temp;
}

bool empty_directory(const std::string& dirname)  {
  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return true;

  int n = 0;

  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    n = 1;
    break;
  } 
  
  closedir(dir);

  return (n == 0) ? true : false;
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

size_t file_size(const std::string& filename) {
  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Get size
  struct stat st;
  fstat(fd, &st);
  size_t size = st.st_size;

  // Close file
  close(fd);

  return size;
}

std::string get_date() {
  time_t timer;
  char buffer[26];
  struct tm* tm_info;
  
  time(&timer);
  tm_info = localtime(&timer);

  strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm_info);

  return std::string(buffer);
}

std::vector<std::string> get_filenames(const std::string& dirname) {
  std::vector<std::string> filenames; 
  std::string filename;

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return filenames;

  while((next_file = readdir(dir))) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    filename = dirname + "/" + next_file->d_name;
    if(is_file(filename))
      filenames.push_back(next_file->d_name);
  } 
  
  closedir(dir);

  return filenames;
}

void init_mbr(const ArraySchema* array_schema,
              const void* coords, void*& mbr) {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const std::type_info* type = array_schema->type(attribute_num);

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

template<class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2) {
  std::set<T> s1(v1.begin(), v1.end());
  std::set<T> s2(v2.begin(), v2.end());
  std::vector<T> intersect;
  std::set_intersection(s1.begin(), s1.end(),
                        s2.begin(), s2.end(),
                        std::back_inserter(intersect));

  return intersect.size() != 0; 
}

template<>
bool is_del(char v) {
  return v == DEL_CHAR;
}

template<>
bool is_del(int v) {
  return v == DEL_INT;
}

template<>
bool is_del(int64_t v) {
  return v == DEL_INT64_T;
}

template<>
bool is_del(float v) {
  return v == DEL_FLOAT;
}

template<>
bool is_del(double v) {
  return v == DEL_DOUBLE;
}

bool is_dir(const std::string& dirname) {
  std::string path = absolute_path(dirname);

  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool is_file(const std::string& filename) {
  std::string path = absolute_path(filename);

  struct stat st;
  return (stat(path.c_str(), &st) == 0)  && !S_ISDIR(st.st_mode);
}

bool is_integer(const char* s) {
  int i=0;
  if(s[0] == '+' || s[0] == '-')
    i = 1; // Skip the first character if it is the sign

  for(; s[i] != '\0'; ++i) {
    if(!isdigit(s[i]))
      return false;
  }

  return true;
}

bool is_non_negative_integer(const char* s) {
  int i=0;

  if(s[0] == '-')
    return false;

  if(s[0] == '+')
    i = 1; // Skip the first character if it is the sign

  for(; s[i] != '\0'; ++i) {
    if(!isdigit(s[i]))
      return false;
  }

  return true;
}

bool is_positive_integer(const char* s) {
  int i=0;

  if(s[0] == '-') // negative
    return false;

  if(s[0] == '0' && s[1] == '\0') // equal to 0
    return false; 

  if(s[0] == '+')
    i = 1; // Skip the first character if it is the + sign

  for(; s[i] != '\0'; ++i) {
    if(!isdigit(s[i]))
      return false;
  }

  return true;
}

template<>
bool is_null(char v) {
  return v == NULL_CHAR;
}

template<>
bool is_null(int v) {
  return v == NULL_INT;
}

template<>
bool is_null(int64_t v) {
  return v == NULL_INT64_T;
}

template<>
bool is_null(float v) {
  return v == NULL_FLOAT;
}

template<>
bool is_null(double v) {
  return v == NULL_DOUBLE;
}

bool is_real(const char* s) {
  bool decimal_point_seen = false;
  int i=0;
  if(s[0] == '+' || s[0] == '-')
    i = 1; // Skip the first character if it is the sign

  for(; s[i] != '\0'; ++i) {
    if(s[i] == '.') {
      if(!decimal_point_seen) 
        decimal_point_seen = true;
      else
        return false;
    } else if(!isdigit(s[i]))
      return false;
  }

  return true;
}

bool is_valid_name(const char* s) {
  for(int i=0; s[i] != '\0'; ++i) {
    if(!isalnum(s[i]) && s[i] != '_')
      return false;
  }

  return true;
}

void log_error(const std::string& err_msg) {
  int fd = open(ERROR_LOG_FILENAME, O_WRONLY | O_APPEND | O_CREAT | O_SYNC,  
                S_IRWXU);
  assert(fd != -1);
  if(fd == -1)
    return;

  std::string date = get_date();
  std::string log = std::string("[") + date + "] " + err_msg + "\n";
  write(fd, log.c_str(), log.size());

  close(fd);
}

template<class T>
bool no_duplicates(const std::vector<T>& v) {
  std::set<T> s(v.begin(), v.end());

  return s.size() == v.size(); 
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

template<class T>
std::vector<T> rdedup(const std::vector<T>& v) {
  std::vector<T> deduped_v;
  int v_size = v.size();
  std::set<T> s;
  bool* dup = new bool[v_size];

  for(int i=v_size-1; i>=0; --i) {
    dup[i] = false;
    if(s.find(v[i]) == s.end()) 
      s.insert(v[i]);
    else
      dup[i] = true;
  }

  for(int i=0; i<v_size; ++i) 
    if(!dup[i])
      deduped_v.push_back(v[i]);

  delete [] dup; 

  return deduped_v; 
}

template<class T>
std::vector<T> sort_dedup(const std::vector<T>& v) {
  std::set<T> s(v.begin(), v.end());
  std::vector<T> deduped_v(s.begin(), s.end());

  return deduped_v; 
}

// Explicit template instantiations
template void convert<int>(const double* a, int* b, int size);
template void convert<int64_t>(const double* a, int64_t* b, int size);
template void convert<float>(const double* a, float* b, int size);
template void convert<double>(const double* a, double* b, int size);

template bool duplicates<char>(const std::vector<char>& v);
template bool duplicates<int>(const std::vector<int>& v);
template bool duplicates<int64_t>(const std::vector<int64_t>& v);
template bool duplicates<float>(const std::vector<float>& v);
template bool duplicates<double>(const std::vector<double>& v);
template bool duplicates<std::string>(const std::vector<std::string>& v);

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

template bool intersect<std::string>(
    const std::vector<std::string>& v1,
    const std::vector<std::string>& v2);

template bool no_duplicates<char>(const std::vector<char>& v);
template bool no_duplicates<int>(const std::vector<int>& v);
template bool no_duplicates<int64_t>(const std::vector<int64_t>& v);
template bool no_duplicates<float>(const std::vector<float>& v);
template bool no_duplicates<double>(const std::vector<double>& v);
template bool no_duplicates<std::string>(const std::vector<std::string>& v);

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

template std::vector<char> rdedup(const std::vector<char>& v);
template std::vector<int> rdedup(const std::vector<int>& v);
template std::vector<int64_t> rdedup(const std::vector<int64_t>& v);
template std::vector<float> rdedup(const std::vector<float>& v);
template std::vector<double> rdedup(const std::vector<double>& v);

template std::vector<char> sort_dedup(const std::vector<char>& v);
template std::vector<int> sort_dedup(const std::vector<int>& v);
template std::vector<int64_t> sort_dedup(const std::vector<int64_t>& v);
template std::vector<float> sort_dedup(const std::vector<float>& v);
template std::vector<double> sort_dedup(const std::vector<double>& v);

