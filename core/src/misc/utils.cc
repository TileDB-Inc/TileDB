/**
 * @file   utils.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <set>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::utils] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::utils] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

void adjacent_slashes_dedup(std::string& value) {
  value.erase(std::unique(value.begin(), value.end(), both_slashes),
              value.end()); 
}

bool both_slashes(char a, char b) {
  return a == '/' && b == '/';
}

int create_dir(const std::string& dir) {
  // Get real directory path
  std::string real_dir = ::real_dir(dir);

  // If the directory does not exist, create it
  if(!is_dir(real_dir)) { 
    if(mkdir(real_dir.c_str(), S_IRWXU)) {
      PRINT_ERROR(std::string("Cannot create directory '") + real_dir + "'; " + 
                  strerror(errno));
      return TILEDB_UT_ERR;
    } else {
      return TILEDB_UT_OK;
    }
  } else {
    PRINT_ERROR(std::string("Cannot create directory '") + real_dir +
                "'; Directory already exists"); 
    return TILEDB_UT_ERR;
  }
}

std::string current_dir() {
  std::string dir = "";
  char* path = getcwd(NULL,0);

  if(path != NULL) {
    dir = path;
    free(path);
  }

  return dir; 
}

ssize_t gzip(
    unsigned char* in, 
    size_t in_size,
    unsigned char* out, 
    size_t out_size) {

  ssize_t ret;
  unsigned have;
  z_stream strm;
 
  // Allocate deflate state
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  ret = deflateInit(&strm, Z_DEFAULT_COMPRESSION);

  if(ret != Z_OK) {
    PRINT_ERROR("Cannot compress with GZIP");
    (void)deflateEnd(&strm);
    return -1;
  }

  // Compress
  strm.next_in = in;
  strm.next_out = out;
  strm.avail_in = in_size;
  strm.avail_out = out_size;
  ret = deflate(&strm, Z_FINISH);

  // Clean up
  (void)deflateEnd(&strm);

  // Return 
  if(ret == Z_STREAM_ERROR || strm.avail_in == 0) {
    PRINT_ERROR("Cannot compress with GZIP");
    return -1;
  } else {
    // Return size of compressed data
    return out_size - strm.avail_out; 
  }
}

template<class T>
bool has_duplicates(const std::vector<T>& v) {
  std::set<T> s(v.begin(), v.end());

  return s.size() != v.size(); 
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

bool is_dir(const std::string& dir) {
  struct stat st;
  return stat(dir.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool is_file(const std::string& file) {
  struct stat st;
  return (stat(file.c_str(), &st) == 0)  && !S_ISDIR(st.st_mode);
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

std::string parent_dir(const std::string& dir) {
  // Get real dir
  std::string real_dir = ::real_dir(dir);

  // Start from the end of the string
  int pos = real_dir.size() - 1;

  // Skip the potential last '/'
  if(real_dir[pos] == '/')
    --pos;

  // Scan backwords until you find the next '/'
  while(pos > 0 && real_dir[pos] != '/')
    --pos;

  return real_dir.substr(0, pos); 
}

void purge_dots_from_path(std::string& path) {
  // For easy reference
  size_t path_size = path.size(); 

  // Trivial case
  if(path_size == 0 || path == "/")
    return;

  // It expects an absolute path
  assert(path[0] == '/');

  // Tokenize
  const char* token_c_str = path.c_str() + 1;
  std::vector<std::string> tokens, final_tokens;
  std::string token;

  for(int i=1; i<path_size; ++i) {
    if(path[i] == '/') {
      path[i] = '\0';
      token = token_c_str;
      if(token != "")
        tokens.push_back(token); 
      token_c_str = path.c_str() + i + 1;
    }
  }
  token = token_c_str;
  if(token != "")
    tokens.push_back(token); 

  // Purge dots
  for(int i=0; i<tokens.size(); ++i) {
    if(tokens[i] == ".") { // Skip single dots
      continue;
    } else if(tokens[i] == "..") {
      if(final_tokens.size() == 0) {
        // Invalid path
        path = "";
        return;
      } else {
        final_tokens.pop_back();
      }
    } else {
      final_tokens.push_back(tokens[i]);
    }
  } 

  // Assemble final path
  path = "/";
  for(int i=0; i<final_tokens.size(); ++i) 
    path += ((i != 0) ? "/" : "") + final_tokens[i]; 
}

std::string real_dir(const std::string& dir) {
  // Initialize current, home and root
  std::string current = current_dir();
  std::string home = getenv("HOME");
  std::string root = "/";

  // Easy cases
  if(dir == "" || dir == "." || dir == "./")
    return current;
  else if(dir == "~")
    return home;
  else if(dir == "/")
    return root; 

  // Other cases
  std::string ret_dir;
  if(starts_with(dir, "/"))
    ret_dir = root + dir;
  else if(starts_with(dir, "~/"))
    ret_dir = home + dir.substr(1, dir.size()-1);
  else if(starts_with(dir, "./"))
    ret_dir = current + dir.substr(1, dir.size()-1);
  else 
    ret_dir = current + "/" + dir;

  adjacent_slashes_dedup(ret_dir);
  purge_dots_from_path(ret_dir);

  return ret_dir;
}

bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

int write_to_file(
    const char* filename,
    const void* buffer,
    size_t buffer_size) {
  // Open file
  int fd = open(filename, O_WRONLY | O_APPEND | O_CREAT | O_SYNC, S_IRWXU);
  if(fd == -1) {
    PRINT_ERROR(std::string("Cannot write to file '") + filename + "'");
    return TILEDB_UT_ERR;
  }

  // Append attribute data to the file
  ssize_t bytes_written = ::write(fd, buffer, buffer_size);
  if(bytes_written != buffer_size) {
    PRINT_ERROR(std::string("Cannot write to file '") + filename + "'");
    return TILEDB_UT_ERR;
  }

  // Close file
  if(close(fd)) {
    PRINT_ERROR(std::string("Cannot write to file '") + filename + "'");
    return TILEDB_UT_ERR;
  }

  // Success 
  return TILEDB_UT_OK;
}

// Explicit template instantiations
template bool has_duplicates<std::string>(const std::vector<std::string>& v);

template bool intersect<std::string>(
    const std::vector<std::string>& v1,
    const std::vector<std::string>& v2);
