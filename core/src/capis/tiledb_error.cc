/**
 * @file   tiledb.cc
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
 * This file implements the error functions of TileDB.
 */

#include "tiledb_error.h"

const char* tiledb_strerror(int err) {
  if(err == TILEDB_OK)                   // 0
    return TILEDB_OK_STR;
  else if(err == TILEDB_EPARSE)          // -1
    return TILEDB_EPARSE_STR;           
  else if(err == TILEDB_ENDEFARR)        // -2
    return TILEDB_ENDEFARR_STR;
  else if(err == TILEDB_EFILE)           // -3
    return TILEDB_EFILE_STR;
  else if(err == TILEDB_ENSMCREAT)       // -4
    return TILEDB_ENSMCREAT_STR;
  else if(err == TILEDB_ENLDCREAT)       // -5
    return TILEDB_ENLDCREAT_STR;
  else if(err == TILEDB_ENQPCREAT)       // -6
    return TILEDB_ENQPCREAT_STR;
  else if(err == TILEDB_EINIT)           // -7
    return TILEDB_EINIT_STR;
  else if(err == TILEDB_EFIN)            // -8
    return TILEDB_EFIN_STR;
  else if(err == TILEDB_EPARRSCHEMA)     // -9
    return TILEDB_EPARRSCHEMA_STR;
  else if(err == TILEDB_EDNEXIST)        // -10
    return TILEDB_EDNEXIST_STR;
  else if(err == TILEDB_EDNCREAT)        // -11
    return TILEDB_EDNCREAT_STR;
  else if(err == TILEDB_ERARRSCHEMA)     // -12
    return TILEDB_ERARRSCHEMA_STR;
  else if(err == TILEDB_EDEFARR)         // -13
    return TILEDB_EDEFARR_STR;
  else if(err == TILEDB_EOARR)           // -14
    return TILEDB_EOARR_STR;
  else if(err == TILEDB_ECARR)           // -15
    return TILEDB_EIARG_STR;
  else if(err == TILEDB_ECARR)           // -16
    return TILEDB_EIARG_STR;
  else
    return "Unknown error";
}

