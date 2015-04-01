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
  // TYPE DEFINITIONS
  /** An distributed array can be opened either in READ or WRITE mode. */
  enum Mode {READ, WRITE};

  // CONSTRUCTORS AND DESTRUCTORS
  /** Simple constructor. */
  DistributedExecutor(const std::string& workspace, MPIModule* mpi_module);
  /** Simple destructor. */
  ~DistributedExecutor();

  // CLASSES
  class ArrayDescriptor {
    // DistributedExecutor can manipulate the private members of this class.
    friend class DistributedExecutor;

   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** 
     * Constructor. If the array is opened in READ mode, then the constructor
     * receives an array descriptor from the storage manager.    
     */
    ArrayDescriptor(const ArraySchema* array_schema,
                    const StorageManager::ArrayDescriptor* ad,
                    int world_size, int world_rank);
    /** 
     * Constructor. If the array is opened in WRITE mode, then the constructor
     * receives a fragment descriptor from the storage manager. This is because
     * arrays are created and updated on a fragment-by-fragment basis.
     */
    ArrayDescriptor(const ArraySchema* array_schema,
                    StorageManager::FragmentDescriptor* fd, 
                    int world_size, int world_rank);
    /** Empty destructor. */
    ~ArrayDescriptor() {}

   private:
    // PRIVATE ATTRIBUTES
    /** A local array descriptor from the storage manager. */
    const StorageManager::ArrayDescriptor* ad_;
    /** The array schema. */
    const ArraySchema* array_schema_;
    /** A local fragment descriptor from the storage manager. */
    StorageManager::FragmentDescriptor* fd_;
    /** 
     * The local dimension domains of the array. Format: 
     * <(first_row, last_row), (first_col, last_col)>
     */
    std::vector<std::pair<double, double> > local_dim_domains_;
    
    // PRIVATE METHODS
    /** 
     * Override the delete operator so that only DistributedExecutor can
     * delete dynamically created ArrayDescriptor objects.
     */
    void operator delete(void* ptr) { ::operator delete(ptr); }
    /** 
     * Override the delete[] operator so that only a StorageManager object can
     * delete dynamically created FragmentDescriptor objects.
     */
    void operator delete[](void* ptr) { ::operator delete[](ptr); }
    /** 
     * Computes the local dimension domains, by partitioning evenly the array
     * rows.
     */
    void compute_local_dim_domains(const ArraySchema* array_schema,
                                   int world_size, int world_rank);
  };

  // QUERIES
  /** Closes an array. */
  void close_array(const ArrayDescriptor* ad) const;
  /** 
   * Defines a TileDB array that will model the array specified by the input.
   * This means that a proper TileDB array schema is created and stored locally.
   */
  void define_array(const std::string& array_name, 
                    int64_t row_num, int64_t col_num) const;
  /** Loads a CSV file into a distributed array. */
  void load(const std::string& filename, 
            const std::string& array_name) const;
  /** Returns the local dimension domains. */
  const std::vector<std::pair<double, double> >& local_dim_domains(
      const ArrayDescriptor* ad) const; 
  /** Opens an array in the input mode. */
  const ArrayDescriptor* open_array(const ArraySchema* array_schema, 
                                    Mode mode) const;
  /** 
   * Takes as input an array descriptor and a 2D range, and returns
   * the coordinates of the non-empty cells falling in the range (in
   * buffer coords) and their corresponding values (in buffer values).
   */
  void read(const ArrayDescriptor* ad, const void* range, 
            void*& coords, size_t& coords_size,
            void*& values, size_t& values_size) const;
  /** 
   * Transposes the input array and writes the result into a newly created 
   * array 
   */
  void transpose(const std::string& array_name, 
                 const std::string& result_array_name) const;
  /** 
   * Writes the input coordinates and values to the array, respecting
   * the global cell order of the array. 
   * NOTE: The input buffers will be freed by the function. 
   */
  void write_sorted(const ArrayDescriptor* ad, 
                    const void* coords, size_t coords_size,
                    const void* values, size_t values_size) const;
  /** 
   * Writes the input coordinates and values to the array, without respecting
   * the global cell order of the array. 
   * NOTE: The input buffers will be freed by the function. 
   */
  void write(const ArrayDescriptor* ad, 
             const void* coords, size_t coords_size,
             const void* values, size_t values_size) const;

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
