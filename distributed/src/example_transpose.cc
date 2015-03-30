/**
 * @file   example_transpose.cc
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
 * This file implements a simple example that computes the transpose of a 
 * distributed matrix A.
 */

#include "distributed_executor.h"
#include "distributed_matrix.h"
#include <sstream>

int main() {
  /****** MPI initialization ******/
  // TODO Jeff to implement this 
  // TODO The MPI state must be stored in object mpi_module
  MPIModule mpi_module;

  /****** System parameters ******/
  // The folder where all the TileDB data are stored at each machine
  std::stringstream ss_workspace;
  ss_workspace << "./" << mpi_module.world_rank() << "/example_transpose/";
  std::string workspace =  ss_workspace.str();
  // The input and output matrix names
  std::string input_matrix_name = "A";
  std::string output_matrix_name = "A_t";
  // The input matrix is MxN, the output matrix will be NxM
  int64_t M = 1000;
  int64_t N = 1000; 
  // The raw data file. We assume that every machine contains its own
  // copy of this file. Moreover, we assume that each machine will 
  // get a partition of the rows of the input. For instance, if there
  // are 5 machines, the first machine (rank 0) will get rows 0-199,
  // the second (rank 1) will get rows 200-399, and so on. We will
  // assume that the raw data are already partitioned in this manner
  // to the machines, i.e., no data shuffling is required for the initial
  // load of the input matrix. 
  std::stringstream ss_input_filename;
  ss_input_filename << "A_" << mpi_module.world_rank() << ".csv";
  std::string input_filename = ss_input_filename.str();

  // Initialize a distributed TileDB executor
  // The first argument is the workspace folder: all the TileDB files on each
  // machine will be stored in this directory. 
  // NOTE: This directory must exist (i.e., you must created beforehand)
  DistributedExecutor dist_executor(workspace, &mpi_module); 

  // Define and load a distributed matrix
  // We assume that every machine 
  dist_executor.define_matrix(input_matrix_name, N, M);
  dist_executor.load(input_filename, input_matrix_name);

  // Compute the transpose of the input matrix (A), and store it into a new
  // matrix named output_matrix_name (A_t)
  dist_executor.transpose(input_matrix_name, output_matrix_name);

  /****** MPI finalization ******/
  // TODO Jeff to implement this 

  return 0;
}

