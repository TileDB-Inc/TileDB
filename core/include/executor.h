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
  Executor(const std::string& workspace);
  /** The destructor deletes all the created modules. */
  ~Executor();

  // QUERIES
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array.
   */
  void export_to_CSV(const std::string& filename, 
                     const ArraySchema& array_schema) const;
  /** Loads a CSV file into an array with the input schema. */
  void load(const std::string& filename, const ArraySchema& array_schema) const;
  /** Updates an array with the data in the input CSV file. */
  void update(const std::string& filename, const 
              ArraySchema& array_schema) const;

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
  /** Creates the workspace folder. */
  void create_workspace() const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
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
