/**   
 * @file   executor.h 
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
 * This file defines class Executor, as well as ExecutorException thrown by 
 * Executor.
 */

#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "storage_manager.h"
#include "loader.h"
#include "consolidator.h"
#include "query_processor.h"
#include <string>

/** 
 * The Executor is responsible for receiving the user queries and dispatching
 * them to the appropriate modules (e.g., the Loader, the Consolidator, and the
 * QueryProcessor).
 */
class Executor {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Simple constructor. */ 
  Executor(std::string workspace);
  /** The destructor deletes all the created modules. */
  ~Executor();

  // QUERIES
  /** Deletes all the fragments of the array. */
  void clear_array(const std::string& array_name) const;
  /** Closes an array. */
  void close_array(const StorageManager::ArrayDescriptor* ad) const;
  /** Closes a fragment. */
  void close_fragment(StorageManager::FragmentDescriptor* fd) const;
  /** Defines an array (stores its array schema at the storage manager. */
  void define_array(const ArraySchema* array_schema) const;
  /** Deletes an array. */
  void delete_array(const std::string& array_name) const;
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array.
   */
  void export_to_csv(const std::string& array_name,
                     const std::string& filename) const;
  /** Returns true if the input file exists. */
  bool file_exists(const std::string& filename) const;
  /** 
   * A filter query creates a new array from the input array, 
   * containing only the cells whose attribute values satisfy the input 
   * expression. The new array will have the input result name.
   */
  void filter(const std::string& array_name,
              const std::string& expression,
              const std::string& result_array_name) const;
  /** 
   * Joins the two input arrays (say, A and B). The result contains a cell only
   * if both the corresponding cells in A and B are non-empty. The input arrays
   * must be join-compatible (see ArraySchema::join_compatible). Moreover,
   * see ArraySchema::create_join_result_schema to see the schema of the
   * output array.
   */
  void join(const std::string& array_name_A,
            const std::string& array_name_B,
            const std::string& result_array_name) const;
  /** Loads a CSV file into an array. */
  void load(const std::string& filename, const std::string& array_name) const;
  /** Returns an array schema. */
  const ArraySchema* load_array_schema(const std::string& array_name) const;
  /** 
   * Returns the k nearest neighbors from query point q. The results (along with
   * all their attribute values) are stored in a new array. The distance metric
   * used to calculate proximity is the Euclidean distance.
   */
  void nearest_neighbors(const std::string& array_name,
                         const std::vector<double>& q,
                         int64_t k,
                         const std::string& result_array_name) const;
  /** Opens an array in READ mode and returns the corresponding descriptor. */
  const StorageManager::ArrayDescriptor* open_array(
      const ArraySchema* array_schema) const;
  /** 
   * Opens an array fragment in WRITE mode and returns the corresponding 
   * descriptor. 
   */
  StorageManager::FragmentDescriptor* open_fragment(
      const ArraySchema* array_schema) const;
  /** 
   * Copies the coordinates of the non-empty cells of the input array
   * falling inside the input range into the coords buffer, and their
   * corresponding attribute values on the input attribute into buffer values,
   * setting properly the buffer sizes in bytes. 
   */
  void read(const StorageManager::ArrayDescriptor* ad, 
            int attribute_id, const void* range,
            void*& coords, size_t& coords_size, 
            void*& attrs, size_t& attrs_size) const; 
  /** 
   * Retiles an array based on the inputs. If tile extents are provided
   * (i) in the case of regular tiles, if the extents differ from those in the
   * array schema, retiling occurs, (ii) in the case of irregular tiles, the
   * array is retiled so that it has regular tiles. If tile extents are not
   * provided for the case of regular tiles, the array is retiled to one with
   * irregular tiles. If order is provided (different from the existing order)
   * retiling occurs. If a capacity is provided, (i) in the case of regular
   * tiles it has no effect (only the schema changes), (ii) in the case of
   * irregular tiles, only the book-keeping structures and array schema
   * are altered to accommodate the change.
   */
  void retile(const std::string& array_name,
              int64_t capacity,
              ArraySchema::CellOrder cell_order,
              const std::vector<double>& tile_extents) const;
  /** 
   * A subarray query creates a new array from the input array, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name.
   */
  void subarray(const std::string& array_name,
                const double* range,                             // TODO: template on range?
                const std::string& result_array_name) const; 
  /** Updates an array with the data in the input CSV file. */
  void update(const std::string& filename, 
              const std::string& array_name) const;
  /**  
   * Writes the input coordinates and attributess into an array. The input 
   * cells do not respect the global cell order.
   * NOTE: The input buffers will be freed by the function.
   */
  void write(StorageManager::FragmentDescriptor* fd,
             void* coords, size_t coords_size, 
             void* attrs, size_t attrs_size) const; 

 private:
  // PRIVATE ATTRIBUTES
  /** The Consolidator module. */
  Consolidator* consolidator_;
  /** The Loader module. */
  Loader* loader_;
  /** The QueryProcessor module. */
  QueryProcessor* query_processor_;
  /** The StorageManager module. */
  StorageManager* storage_manager_;
  /** A folder in the disk where the Executor creates all its data. */
  std::string workspace_;
  
  // PRIVATE METHODS
  /** Returns the names of all fragments in the array. */
  std::vector<std::string> get_all_fragment_names(
      const ArraySchema* array_schema) const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** Updates the fragment information (adding one fragment) of an array. */
  void update_fragment_info(const ArraySchema* array_schema) const;
};

/** This exception is thrown by Executor. */
class ExecutorException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  ExecutorException(const std::string& msg) 
      : msg_(msg) {}
  /** Empty destructor. */
  ~ExecutorException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }

 private:
  /** The exception message. */
  std::string msg_;
};

#endif
