/**
 * @file   distributed_executor.h
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
 * This file defines class DistributedExecutor which is the distributed version
 * of class Executor.
 */

#ifndef DISTRIBUTED_EXECUTOR_H
#define DISTRIBUTED_EXECUTOR_H

#include "executor.h"
#include "mpi_module.h"

/** This class implements a distributed TileDB executor. */
class DistributedExecutor {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
  /** Simple constructor. */
  DistributedExecutor(const std::string& workspace, MPIModule* mpi_module);
  /** Simple destructor. */
  ~DistributedExecutor();

  // QUERIES
  /** 
   * Defines a TileDB array that will model the matrix specified by the input.
   * This means that a proper TileDB array schema is created and stored locally.
   */
  void define_matrix(const std::string& matrix_name, 
                     int64_t row_num, int64_t col_num) const;
  /** Loads a CSV file into a distributed matrix. */
  void load(const std::string& filename, 
            const std::string& array_name) const;
  /** 
   * Transposes the input matrix and writes the result into a newly created 
   * matrix. 
   */
  void transpose(const std::string& matrix_name, 
                 const std::string& result_matrix_name) const;

 private:
  // PRIVATE ATTRIBUTES
  /** A local TileDB executor. */
  Executor* executor_;
  /** The MPI state. */
  MPIModule* mpi_module_;
  /** The worksapce (i.e., local folder where all data are stored. */
  std::string workspace_;
};

#endif
