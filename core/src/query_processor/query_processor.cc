/**
 * @file   query_processor.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the QueryProcessor class.
 */
  
#include "bin_file.h"
#include "query_processor.h"
#include "utils.h"
#include "cell.h"
#include <stdio.h>
#include <typeinfo>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/uio.h>
#include <assert.h>
#include <algorithm>
#include <functional>
#include <math.h>
#include <string.h>

// Macro for printing error message in VERBOSE mode
#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << "[TileDB::QueryProcessor] Error: " << x \
                                   << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::QueryProcessor] Warning: " \
                                     << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

QueryProcessor::QueryProcessor(StorageManager* storage_manager) 
    : storage_manager_(storage_manager) {
  finalized_ = false;
  created_successfully_ = true;
}


bool QueryProcessor::created_successfully() const {
  return created_successfully_;
}

int QueryProcessor::finalize() {
  finalized_ = true;

  return 0;
}

QueryProcessor::~QueryProcessor() {
  if(!finalized_) {
    PRINT_WARNING("QueryProcessor not finalized. Finalizing now.");
    finalize();
  }
}

/* ****************************** */
/*         QUERY FUNCTIONS        */
/* ****************************** */

int QueryProcessor::array_export(
    const std::string& workspace, 
    const std::string& group, 
    const std::string& array_name, 
    const std::string& filename, 
    const std::string& format, 
    const std::vector<std::string>& dim_names,
    const std::vector<std::string>& attribute_names,
    const std::vector<double>& range,
    char delimiter,
    int precision) const {
  // Open array in read mode
  int ad = storage_manager_->array_open(workspace, group, array_name, "r");
  if(ad == -1) 
    return -1;

  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema)) {
    storage_manager_->array_close(ad); 
    return -1;
  }

  // Get dimension and attribute ids
  std::vector<int> dim_ids;
  if(array_schema->get_dim_ids(dim_names, dim_ids)) {
    storage_manager_->array_close(ad); 
    return -1;
  }
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names, attribute_ids)) {
    storage_manager_->array_close(ad); 
    return -1;
  }

  // Check range
  int range_size = (int) range.size();
  if(range_size != 0 && range_size != 2*array_schema->dim_num()) {
    PRINT_ERROR("Invalid range");
    storage_manager_->array_close(ad); 
    return -1;
  }

  // Resolve CSV or BIN
  bool csv = false;
  bool bin = false;
  if(ends_with(format, "csv") || ends_with(format, "csv.gz")) {
    csv = true;
  } else if(ends_with(format, "bin") || ends_with(format, "bin.gz")) {
    bin = true; 
  } else  {
    PRINT_ERROR("Invalid file name");
    storage_manager_->array_close(ad); 
    return -1;
  }

  // Resolve dense
  bool dense;
  if(starts_with(format, "dense") || starts_with(format, "reverse.dense")) 
    dense = true;
  else
    dense = false;

  // Resolve reverse
  bool reverse;
  if(starts_with(format, "reverse")) 
    reverse = true;
  else
    reverse = false;

  // Resolve compression type
  CompressionType compression;
  if(ends_with(format, ".gz"))
    compression = CMP_GZIP;
  else
    compression = CMP_NONE;

  // Export (2^3 = 8 possible combinations)
  int rc;
  if(csv && reverse && dense) 
    rc = array_export_csv_reverse_dense(
             ad, filename, dim_ids, attribute_ids, range, 
             compression, delimiter, precision);
  else if(csv && reverse && !dense) 
    rc = array_export_csv_reverse_sparse(
             ad, filename, dim_ids, attribute_ids, range, 
             compression, delimiter, precision);
  else if(csv && !reverse && dense) 
    rc = array_export_csv_normal_dense(
             ad, filename, dim_ids, attribute_ids, range, 
             compression, delimiter, precision);
  else if(csv && !reverse && !dense) 
    rc = array_export_csv_normal_sparse(
             ad, filename, dim_ids, attribute_ids, range, 
             compression, delimiter, precision);
  else if(bin && reverse && dense) 
    rc = array_export_bin_reverse_dense(
             ad, filename, dim_ids, attribute_ids, range, compression);
  else if(bin && reverse && !dense) 
    rc = array_export_bin_reverse_sparse(
             ad, filename, dim_ids, attribute_ids, range, compression);
  else if(bin && !reverse && dense) 
    rc = array_export_bin_normal_dense(
             ad, filename, dim_ids, attribute_ids, range, compression);
  else if(bin && !reverse && !dense) 
    rc = array_export_bin_normal_sparse(
             ad, filename, dim_ids, attribute_ids, range, compression);

  // Clean up
  if(rc == -1) { 
    storage_manager_->array_close_forced(ad); 
  } else {
    if(storage_manager_->array_close(ad))
      rc = -1;
  }

  // 0 for success and -1 for error 
  return rc;
}

int QueryProcessor::subarray(
    const std::string& workspace, 
    const std::string& workspace_sub, 
    const std::string& group, 
    const std::string& group_sub, 
    const std::string& array_name, 
    const std::string& array_name_sub, 
    const std::vector<double>& range,
    const std::vector<std::string>& attribute_names) const {
  // Open input array in read mode
  int ad = storage_manager_->array_open(
               workspace, group, array_name, "r");
  if(ad == -1)
    return -1;

  // Get input array schema
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema)) {
    storage_manager_->array_close(ad);
    return -1;
  }
  const std::type_info* coords_type = array_schema->coords_type();
  int dim_num = array_schema->dim_num();

  // Check range 
  int range_size = (int) range.size();
  if(range_size != 2*dim_num) {
    PRINT_ERROR("Invalid range");
    storage_manager_->array_close(ad);
    return -1;
  }

  // Get attribute ids
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names, attribute_ids)) {
    storage_manager_->array_close(ad);
    return -1;
  }

  // Create result array schema
  ArraySchema* array_schema_sub = 
      array_schema->clone(array_name_sub, attribute_ids); 

  // Store result array schema
  if(storage_manager_->array_schema_store(
                           workspace_sub, group_sub, array_schema_sub)) {
    storage_manager_->array_close(ad);
    delete array_schema_sub;
    return -1;
  }

  // Open result array in write mode
  int ad_sub = storage_manager_->array_open(
                   workspace_sub, group_sub, array_name_sub, "w");
  if(ad_sub == -1) {
    storage_manager_->array_close(ad);
    delete array_schema_sub;
    return -1;
  }

  // Invoke the proper function, templated on the array coordinates type
  int rc;
  if(coords_type == &typeid(int)) { 
    int* new_range = new int[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray<int>(ad, ad_sub, new_range, attribute_ids);
    delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range = new int64_t[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray<int64_t>(ad, ad_sub, new_range, attribute_ids);
    delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range = new float[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray<float>(ad, ad_sub, new_range, attribute_ids);
    delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range = new double[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray<double>(ad, ad_sub, new_range, attribute_ids);
    delete [] new_range;
  }

  // Clean up
  if(rc == -1) {
    storage_manager_->array_close_forced(ad); 
    storage_manager_->array_close_forced(ad_sub); 
    storage_manager_->array_delete(workspace_sub, group_sub, array_name_sub);
  } else {
    if(storage_manager_->array_close(ad))
      rc = -1;
    if(storage_manager_->array_close(ad_sub))
      rc = -1;
  }

  // 0 for success and -1 for error
  return rc;
}

int QueryProcessor::subarray_buf(
    int ad,
    const std::vector<double>& range,
    const std::vector<std::string>& attribute_names,
    void* buffer,
    size_t& buffer_size) const {
  // Get input array schema
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema)) {
    storage_manager_->array_close(ad);
    return -1;
  }
  const std::type_info* coords_type = array_schema->coords_type();
  int dim_num = array_schema->dim_num();

  // Check range 
  int range_size = (int) range.size();
  if(range_size != 2*dim_num) {
    PRINT_ERROR("Invalid range");
    storage_manager_->array_close(ad);
    return -1;
  }

  // Get attribute ids
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names, attribute_ids)) {
    storage_manager_->array_close(ad);
    return -1;
  }

  // Invoke the proper function, templated on the array coordinates type
  int rc;
  if(coords_type == &typeid(int)) { 
    int* new_range = new int[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray_buf<int>(ad, new_range, attribute_ids, buffer, buffer_size);
    delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range = new int64_t[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray_buf<int64_t>(ad, new_range, attribute_ids, 
                               buffer, buffer_size);
    delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range = new float[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray_buf<float>(ad, new_range, attribute_ids, buffer, buffer_size);
    delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range = new double[2*dim_num]; 
    convert(&range[0], new_range, 2*dim_num);
    rc = subarray_buf<double>(ad, new_range, attribute_ids, 
                              buffer, buffer_size);
    delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int QueryProcessor::array_export_csv_reverse_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the array coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_csv_reverse_dense<int>(ad, filename, dim_ids,
                                             attribute_ids, new_range,
                                             compression, delimiter, 
                                             precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_csv_reverse_dense<int64_t>(ad, filename, dim_ids,
                                                 attribute_ids, new_range,
                                                 compression, delimiter,
                                                 precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_csv_reverse_dense<float>(ad, filename, dim_ids,
                                               attribute_ids, new_range,
                                               compression, delimiter,
                                               precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_csv_reverse_dense<double>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression, delimiter,
                                                precision);
    if(new_range != NULL)
      delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::array_export_csv_reverse_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // TODO
  PRINT_ERROR("This export mode is not supported yet");
  exit(-1);
}

int QueryProcessor::array_export_csv_reverse_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the array coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_csv_reverse_sparse<int>(ad, filename, dim_ids,
                                              attribute_ids, new_range,
                                              compression, delimiter,
                                              precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_csv_reverse_sparse<int64_t>(ad, filename, dim_ids,
                                                  attribute_ids, new_range,
                                                  compression, delimiter,
                                                  precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_csv_reverse_sparse<float>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression, delimiter,
                                                precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_csv_reverse_sparse<double>(ad, filename, dim_ids,
                                                 attribute_ids, new_range,
                                                 compression, delimiter,
                                                 precision);
    if(new_range != NULL)
      delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::array_export_csv_reverse_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // Prepare CSV file
  CSVFile csv_file(compression);
  csv_file.open(filename, "w");

  // Prepare cell iterators
  ArrayConstReverseCellIterator<T>* cell_it;
  if(range == NULL) 
    cell_it = storage_manager_->rbegin<T>(ad, attribute_ids);
  else
    cell_it = storage_manager_->rbegin<T>(ad, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);

  // Write cells into the CSV file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    if(csv_file << cell.csv_line<T>(dim_ids, attribute_ids, 
                                    delimiter, precision)) {
      csv_file.close();
      cell_it->finalize();
      delete cell_it;
      return -1;
    }
  }

  // Clean up 
  csv_file.close();
  int rc = cell_it->finalize();
  delete cell_it;

  // 0 for success and -1 for error
  return rc;
}

int QueryProcessor::array_export_csv_normal_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the array coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_csv_normal_dense<int>(ad, filename, dim_ids,
                                            attribute_ids, new_range,
                                            compression, delimiter,
                                            precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_csv_normal_dense<int64_t>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression, delimiter,
                                                precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_csv_normal_dense<float>(ad, filename, dim_ids,
                                              attribute_ids, new_range,
                                              compression, delimiter,
                                              precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_csv_normal_dense<double>(ad, filename, dim_ids,
                                               attribute_ids, new_range,
                                               compression, delimiter,
                                               precision);
    if(new_range != NULL)
      delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::array_export_csv_normal_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // Prepare CSV file
  CSVFile csv_file(compression);
  csv_file.open(filename, "w");

  // Prepare cell iterators
  ArrayConstDenseCellIterator<T>* cell_it;
  if(range == NULL) 
    cell_it = storage_manager_->begin_dense<T>(ad, attribute_ids);
  else
    cell_it = storage_manager_->begin_dense<T>(ad, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);

  // Write cells into the CSV file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    if(csv_file << cell.csv_line<T>(dim_ids, attribute_ids, 
                                    delimiter, precision)) {
      csv_file.close();
      cell_it->finalize();
      delete cell_it;
      return -1;
    } 
  }

  // Clean up 
  csv_file.close();
  int rc = cell_it->finalize();
  delete cell_it;

  // 0 for success and -1 for error
  return rc;
}

int QueryProcessor::array_export_csv_normal_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the array coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_csv_normal_sparse<int>(ad, filename, dim_ids,
                                             attribute_ids, new_range,
                                             compression, delimiter,
                                             precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_csv_normal_sparse<int64_t>(ad, filename, dim_ids,
                                                 attribute_ids, new_range,
                                                 compression, delimiter,
                                                 precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_csv_normal_sparse<float>(ad, filename, dim_ids,
                                               attribute_ids, new_range,
                                               compression, delimiter,
                                               precision);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_csv_normal_sparse<double>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression, delimiter,
                                                precision);
    if(new_range != NULL)
      delete [] new_range;
  }

  return rc;
}

template<class T>
int QueryProcessor::array_export_csv_normal_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression,
    char delimiter,
    int precision) const {
  // Prepare CSV file
  CSVFile csv_file(compression);
  csv_file.open(filename, "w");

  // Prepare cell iterators
  ArrayConstCellIterator<T>* cell_it;
  if(range == NULL) 
    cell_it = storage_manager_->begin<T>(ad, attribute_ids);
  else
    cell_it = storage_manager_->begin<T>(ad, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);

  // Write cells into the CSV file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    if(csv_file << cell.csv_line<T>(dim_ids, attribute_ids, 
                                    delimiter, precision)) {
      csv_file.close();
      cell_it->finalize();
      delete cell_it;
      return -1;
    } 
  }

  // Clean up 
  csv_file.close();
  int rc = cell_it->finalize();
  delete cell_it;

  //0 for success and -1 for error
  return rc;
}

int QueryProcessor::array_export_bin_reverse_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the array coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_bin_reverse_dense<int>(ad, filename, dim_ids,
                                             attribute_ids, new_range,
                                             compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_bin_reverse_dense<int64_t>(ad, filename, dim_ids,
                                                 attribute_ids, new_range,
                                                 compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_bin_reverse_dense<float>(ad, filename, dim_ids,
                                               attribute_ids, new_range,
                                               compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_bin_reverse_dense<double>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression);
    if(new_range != NULL)
      delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::array_export_bin_reverse_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression) const {
  // TODO
  PRINT_ERROR("This export mode is not supported yet");
  exit(-1);
}

int QueryProcessor::array_export_bin_reverse_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_bin_reverse_sparse<int>(ad, filename, dim_ids,
                                              attribute_ids, new_range,
                                              compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_bin_reverse_sparse<int64_t>(ad, filename, dim_ids,
                                                  attribute_ids, new_range,
                                                  compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_bin_reverse_sparse<float>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_bin_reverse_sparse<double>(ad, filename, dim_ids,
                                                 attribute_ids, new_range,
                                                 compression);
    if(new_range != NULL)
      delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::array_export_bin_reverse_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression) const {
  // Prepare BIN file
  BINFile bin_file(compression);
  bin_file.open(filename, "w");

  // Prepare cell iterators
  ArrayConstReverseCellIterator<T>* cell_it;
  if(range == NULL) 
    cell_it = storage_manager_->rbegin<T>(ad, attribute_ids);
  else
    cell_it = storage_manager_->rbegin<T>(ad, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);
  
  // Prepare C-style cell to hold the actual cell to be written in the file
  void* cell_c = NULL;
  size_t cell_c_capacity = 0, cell_c_size = 0;

  // Write cells into the BIN file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    cell.cell<T>(dim_ids, attribute_ids, cell_c, cell_c_capacity, cell_c_size);
    if(bin_file.write(cell_c, cell_c_size) < 0) {
      bin_file.close();
      cell_it->finalize();
      delete cell_it;
      if(cell_c != NULL)
        free(cell_c);
      return -1;
    }
  }

  // Clean up 
  bin_file.close();
  int rc = cell_it->finalize();
  delete cell_it;
  if(cell_c != NULL)
    free(cell_c);

  // 0 for success and -1 for error
  return rc;
}

int QueryProcessor::array_export_bin_normal_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the array coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_bin_normal_dense<int>(ad, filename, dim_ids,
                                            attribute_ids, new_range,
                                            compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_bin_normal_dense<int64_t>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_bin_normal_dense<float>(ad, filename, dim_ids,
                                              attribute_ids, new_range,
                                              compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_bin_normal_dense<double>(ad, filename, dim_ids,
                                               attribute_ids, new_range,
                                               compression);
    if(new_range != NULL)
      delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::array_export_bin_normal_dense(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression) const {
  // Prepare BIN file
  BINFile bin_file(compression);
  bin_file.open(filename, "w");

  // Prepare cell iterators
  ArrayConstDenseCellIterator<T>* cell_it;
  if(range == NULL) 
    cell_it = storage_manager_->begin_dense<T>(ad, attribute_ids);
  else
    cell_it = storage_manager_->begin_dense<T>(ad, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);
  
  // Prepare C-style cell to hold the actual cell to be written in the file
  void* cell_c = NULL;
  size_t cell_c_capacity = 0, cell_c_size = 0;

  // Write cells into the BIN file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    cell.cell<T>(dim_ids, attribute_ids, cell_c, cell_c_capacity, cell_c_size);
    if(bin_file.write(cell_c, cell_c_size) < 0) {
      bin_file.close();
      cell_it->finalize();
      delete cell_it;
        if(cell_c != NULL)
          free(cell_c);
      return -1;
    }
  }

  // Clean up 
  bin_file.close();
  int rc = cell_it->finalize();
  delete cell_it;
  if(cell_c != NULL)
    free(cell_c);

  // 0 for success and -1 for error
  return rc;
}

int QueryProcessor::array_export_bin_normal_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const std::vector<double>& range,
    CompressionType compression) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();
  int rc;

  // Invoke the proper function, templated on the coordinates type
  if(coords_type == &typeid(int)) { 
    int* new_range;
    calculate_new_range<int>(range, new_range); 
    rc = array_export_bin_normal_sparse<int>(ad, filename, dim_ids,
                                             attribute_ids, new_range,
                                             compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) {
    int64_t* new_range;
    calculate_new_range<int64_t>(range, new_range); 
    rc = array_export_bin_normal_sparse<int64_t>(ad, filename, dim_ids,
                                                 attribute_ids, new_range,
                                                 compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(float)) {
    float* new_range;
    calculate_new_range<float>(range, new_range); 
    rc = array_export_bin_normal_sparse<float>(ad, filename, dim_ids,
                                               attribute_ids, new_range,
                                               compression);
    if(new_range != NULL)
      delete [] new_range;
  } else if(coords_type == &typeid(double)) {
    double* new_range;
    calculate_new_range<double>(range, new_range); 
    rc = array_export_bin_normal_sparse<double>(ad, filename, dim_ids,
                                                attribute_ids, new_range,
                                                compression);
    if(new_range != NULL)
      delete [] new_range;
  }

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::array_export_bin_normal_sparse(
    int ad,
    const std::string& filename,
    const std::vector<int>& dim_ids,
    const std::vector<int>& attribute_ids,
    const T* range,
    CompressionType compression) const {
  // Prepare BIN file
  BINFile bin_file(compression);
  bin_file.open(filename, "w");

  // Prepare cell iterators
  ArrayConstCellIterator<T>* cell_it;
  if(range == NULL) 
    cell_it = storage_manager_->begin<T>(ad, attribute_ids);
  else
    cell_it = storage_manager_->begin<T>(ad, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);
  
  // Prepare C-style cell to hold the actual cell to be written in the file
  void* cell_c = NULL;
  size_t cell_c_capacity = 0, cell_c_size = 0;

  // Write cells into the BIN file
  for(; !cell_it->end(); ++(*cell_it)) { 
    cell.set_cell(**cell_it);
    cell.cell<T>(dim_ids, attribute_ids, cell_c, cell_c_capacity, cell_c_size);
    if(bin_file.write(cell_c, cell_c_size) < 0) {
      bin_file.close();
      cell_it->finalize();
      delete cell_it;
      if(cell_c != NULL)
        free(cell_c);
      return -1;
    }
  }

  // Clean up 
  bin_file.close();
  int rc = cell_it->finalize();
  delete cell_it;
  if(cell_c != NULL)
    free(cell_c);

  // 0 for success and -1 for error
  return rc;
}

template<class T>
void QueryProcessor::calculate_new_range(
    const std::vector<double>& old_range,
    T*& new_range) const {
  // For easy reference
  int range_size = old_range.size();

  // Convert range
  if(range_size == 0) {
    new_range = NULL; 
  } else {
    new_range = new T[range_size]; 
    convert<T>(&old_range[0], new_range, range_size);
  }
}

template<class T>
int QueryProcessor::subarray(
    int ad, 
    int ad_sub, 
    const T* range,
    const std::vector<int>& attribute_ids) const { 
  // Prepare cell iterator
  ArrayConstCellIterator<T>* cell_it = 
      storage_manager_->begin<T>(ad, range, attribute_ids);

  // Write cells into the new array
  for(; !cell_it->end(); ++(*cell_it)) { 
    if(storage_manager_->cell_write_sorted<T>(ad_sub, **cell_it)) {
      cell_it->finalize();
      delete cell_it;
      return -1;
    }
  }

  // Clean up
  int rc = cell_it->finalize();
  delete cell_it;

  // 0 for success and -1 for error
  return rc;
}

template<class T>
int QueryProcessor::subarray_buf(
    int ad,
    const T* range,
    const std::vector<int>& attribute_ids,
    void* buffer,
    size_t& buffer_size) const {
  // Initialization
  size_t cells_size = 0; 

  // Prepare cell iterator
  ArrayConstCellIterator<T>* cell_it = 
      storage_manager_->begin<T>(ad, range, attribute_ids);
  if(cell_it == NULL)
    return -1;

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);
  
  // Prepare C-style cell to hold the actual cell to be written in the buffer
  void* cell_c = NULL;
  size_t cell_c_capacity = 0, cell_c_size = 0;

  // Write cells into the buffer
  for(; !cell_it->end(); ++(*cell_it)) { 
    // Write a cell
    cell.set_cell(**cell_it);
    cell.cell<T>(
        cell_it->array_schema()->get_dim_ids(), 
        attribute_ids, 
        cell_c, 
        cell_c_capacity,  
        cell_c_size);
    if(cells_size + cell_c_size > buffer_size) {
      PRINT_ERROR("Cannot write cell - buffer overflow");
      buffer_size = -1;
      return -1;
    }
    memcpy(static_cast<char*>(buffer) + cells_size, **cell_it, cell_c_size);
    cells_size += cell_c_size;
  }

  // Success
  buffer_size = cells_size;
  return 0;
}
