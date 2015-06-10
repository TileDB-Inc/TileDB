/**
 * @file   type_converter.cc
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
 * This file implements the class TypeConverter.
 */

#include "type_converter.h"
#include <inttypes.h>

/******************************************************
************** CONSTRUCTORS & DESTRUCTORS *************
******************************************************/

TypeConverter::TypeConverter(const void* value) 
    : value_(value) {
}

/******************************************************
********************* OPERATORS ***********************
******************************************************/

template<>
TypeConverter::operator const char*() const {
  return static_cast<const char*>(value_); 
}

template<>
TypeConverter::operator const int*() const {
  return static_cast<const int*>(value_); 
}

template<>
TypeConverter::operator const int64_t*() const {
  return static_cast<const int64_t*>(value_); 
}

template<>
TypeConverter::operator const float*() const {
  return static_cast<const float*>(value_); 
}

template<>
TypeConverter::operator const double*() const {
  return static_cast<const double*>(value_); 
}
