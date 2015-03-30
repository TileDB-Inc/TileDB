/**
 * @file   MPI_module.h
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
 * This file defines class MPIModule, which is implements the network
 * infrastructure via MPI.
 */

#ifndef MPI_MODULE_H
#define MPI_MODULE_H

class MPIModule {
// TODO: Jeff to implement this and check everything I have done here.

 public:
  /** MPI environment constructor. */
  MPIModule();
  /** MPI environment destructor. */
  ~MPIModule();
  /** Returns the MPI rank of the machine. */
  int rank() { return rank_; }
  /** Returns the MPI size. */
  int size() { return size_; }

 private:
  // PRIVATE ATTRIBUTES
  MPI_Comm comm_
  /** The MPI world rank of the machine. */ 
  int rank_;
  /** The MPI world size. */ 
  int size_;
};

#endif
