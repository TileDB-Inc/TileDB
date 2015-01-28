/**
 * @file   consolidator.h
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
 * This file defines class Consolidator.
 */

#ifndef CONSOLIDATOR_H
#define CONSOLIDATOR_H

#include "storage_manager.h"
#include <string>

/** 
 * A batch of updates (insertions/deletions) initially lead to the creation of a
 * new 'array fragment'. A TileDB array is comprised of potentially multiple
 * array fragments. The consolidator is responsibled for merging a set of 
 * array fragments into a single one, based on a parameter called 'consolidation
 * step. The consolidation takes place in hierarchical way, conceptually 
 * visualized as a merge tree. Conslolidation with step greater than 1 leads to 
 * an amortized logarithmic update cost.
 */
class Consolidator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Empty constructor. */
  Consolidator();
  /** Simple constructor. */ 
  Consolidator(const std::string& workspace, StorageManager& storage_manager);

 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the Consolidator will be interfacing with. */
  StorageManager& storage_manager_;
  /** A folder in the disk where the Consolidator creates all its data. */
  std::string workspace_;
  
  // PRIVATE ATTRIBUTES
  /** Creates the workspace folder. */
  void create_workspace() const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
};

/** This exception is thrown by Consolidator. */
class ConsolidatorException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  ConsolidatorException(const std::string& msg) 
      : msg_(msg) {}
  /** Empty destructor. */
  ~ConsolidatorException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }

 private:
  /** The exception message. */
  std::string msg_;
};

#endif
