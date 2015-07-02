/**
 * @file   query_processor.h
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
 * This file defines class QueryProcessor. 
 */

#ifndef QUERY_PROCESSOR_H
#define QUERY_PROCESSOR_H

#include "array_schema.h"
#include "csv_file.h"
#include "expression_tree.h"
#include "storage_manager.h"
#include "tiledb_error.h"

/** 
 * This class implements the query processor module, which is responsible
 * for processing the various queries. 
 */
class QueryProcessor {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
  /** 
   * Simple constructor. The storage manager is the module the query processor
   * interefaces with.
   */
  QueryProcessor(StorageManager* storage_manager);

  // ACCESSORS
  /* Returns the current error code. */
  int err() const;

  // QUERY FUNCTIONS
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array.
   * The input dimension and attribute names allow for selective exporting
   * of coordinates and attribute values respectively. Argument reverse
   * allows for optionally exporting the cells in reverse order (if it
   * is true).
   */
  int export_csv(
      const std::string& array_name,
      const std::string& filename,
      const std::vector<std::string>& dim_names, 
      const std::vector<std::string>& attribute_names,
      bool reverse) const;
  /** 
   * A subarray query creates a new array from the input array, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name. The fourth argument
   * specifies which attributes from the input array will be written
   * to the output array. If this list is empty, then all attributes
   * of the input array are written to the output array. The last argument
   * allows for optionally writing the cells to the output in reverse order (if
   * it is true).
   */
  int subarray(
      const std::string& array_name, 
      const std::vector<double>& range,
      const std::string& result_array_name,
      const std::vector<std::string>& attribute_names) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the QueryProcessor will be interfacing with. */
  StorageManager* storage_manager_;
  /** The current error code. It is 0 on success. */
  int err_;

  // PRIVATE METHODS
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array.
   * The function is templated on the coordinates type. It takes as input an
   * array descriptor instead of its name. The input dimension and attribute ids
   * allow for selective exporting of coordinates and attribute values
   * respectively. Note that the attribute ids must NOT contain the id of the
   * coordinates.
   */
  template<class T>
  void export_csv(
      int ad, const ArraySchema* array_schema,
      const std::string& filename,
      const std::vector<int>& dim_ids,
      const std::vector<int>& attribute_ids) const;
  /** 
   * Same as QueryProcessor::export_to_csv, but the cells are exporting to the
   * CSV file in reverse order.
   */
  template<class T>
  void export_csv_reverse(
      int ad, const ArraySchema* array_schema,
      const std::string& filename,
      const std::vector<int>& dim_ids,
      const std::vector<int>& attribute_ids) const;

  /** 
   * Parses the attribute names and returns the corresponding attribute ids,
   * which will be used to initialize a cell iterator.
   */
  int parse_attribute_names(
      const std::vector<std::string>& attribute_names,
      const ArraySchema* array_schema,
      std::vector<int>& attribute_ids, std::string& err_msg) const;
  /** Parses the dimension names and returns the corresponding dimension ids. */
  int parse_dim_names(
      const std::vector<std::string>& dim_names,
      const ArraySchema* array_schema,
      std::vector<int>& dim_ids, std::string& err_msg) const;
  /** 
   * A subarray query creates a new array from the input array, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name. The function is templated on
   * the coordinates type. It takes as input array descriptors instead of names.
   * The input attribute ids allow for selective writing of attribute values to
   * the output array, respectively. Note that the attribute ids must NOT
   * contain the id of the coordinates. Also the attribute ids can be in an
   * arbitrary order (not necessarily the one specified in the input array).
   */
  template<class T>
  void subarray(
      int ad, const T* range, int result_ad,
      const std::vector<int>& attribute_ids) const;
};

#endif
