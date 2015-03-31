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
#include <stdlib.h>
#include <string.h>

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

void expand_buffer(void*& buffer, size_t size) {
  void* temp = malloc(2*size);
  memcpy(temp, buffer, size);
  free(buffer);
  buffer = temp;
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
