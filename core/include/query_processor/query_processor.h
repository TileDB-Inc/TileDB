/**
 * @file   query_processor.h
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
 * This file defines class QueryProcessor. 
 */

#ifndef __QUERY_PROCESSOR_H__
#define __QUERY_PROCESSOR_H__

#include "array_schema.h"
#include "csv_file.h"
#include "expression_tree.h"
#include "storage_manager.h"

/** 
 * This class implements the query processor module, which is responsible
 * for processing the various TileDB queries. 
 *
 * For better understanding of this class, some useful information is 
 * summarized below:
 *
 * - **Workspace** <br>
 *   This is the main place where the arrays persist on the disk. It is
 *   implemented as a directory in the underlying file system. 
 * - **Group** <br>
 *   Groups enable hierarchical organization of the arrays. They are 
 *   implemented as sub-directories inside the workspace directory. Even
 *   the workspace directory is regarded as a group (i.e., the root
 *   group of all groups in the workspace). Note that a group path inserted
 *   by the user is translated with respect to the workspace, i.e., all
 *   home ("~/"), current ("./") and root ("/") refer to the workspace.
 *   For instance, if the user gives "W1" as a workspace, and "~/G1" as
 *   a group, then the directory in which the array directory will be
 *   stored is "W1/G1".  
 * - **Canonicalized absolute workspace/group paths** <br>
 *   Most of the functions of this class take as arguments a workspace and
 *   a group path. These paths may be given in relative format (e.g., "W1") and 
 *   potentially including strings like "../". The canonicalized
 *   absolute format of a path is an absolute path that does not contain 
 *   "../" or mulitplicities of slashes. Moreover, the canonicalized absolute
 *   format of the group is the *full* path of the group in the disk. For
 *   instance, suppose the current working directory is "/stavros/TileDB",
 *   and the user provided "W1" as the workspace, and "~/G1/G2/../" as the 
 *   group. The canonicalized absolute path of the workspace is 
 *   "/stavros/TileDB/W1" and that of the group is 
 *   "/stavros/TileDB/W1/G2". Most functions take an extra argument called
 *   *real_path* or *real_paths*, which indicates whether the input workspace
 *   and group path(s) are already in canonicalized absolute (i.e., real) 
 *   format, so that the function avoids redundant conversions. Finally, note 
 *   that an empty ("") workspace refers to the current working directory, 
 *   whereas an empty group refers to the default workspace group.
 * - <b>%Array</b> <br>
 *   A TileDB array. All the data of the array are stored in a directory 
 *   named after the array, which is placed in a certain group inside
 *   a workspace. See class Array for more information.
 * - <b>%Array descriptor</b> <br> 
 *   When an array is opened, an array descriptor is returned. This 
 *   descriptor is used in all subsequent operations with this array.
 * - <b>%Array schema</b> <br>
 *   An array consists of *dimensions* and *attributes*. The dimensions have a
 *   specific domain that orients the *coordinates* of the array cells. The
 *   attributes and coordinates have potentially different data types. Each
 *   array specifies a *global cell order*. This determines the order in which
 *   the cells are stored on the disk. See ArraySchema for more information.
 */
class QueryProcessor {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Constructor. 
   *
   * @param storage_manager The storage manager is the module the query 
   * processor interefaces with.
   */
  QueryProcessor(StorageManager* storage_manager);

  /**
   * Checks if the constructor of the object was executed successfully.
   * Always check this function after creating a QueryProcessor object.
   *
   * @return *True* for successfull creation and *false* otherwise. 
   */
  bool created_successfully() const;

  /** 
   * Finalizes a QueryProcessor object. Always execute this function before 
   * deleting a QueryProcessor object (otherwise a warning will be printed in 
   * the destructor, if compiled in VERBOSE mode). 
   *
   * @return **0** for success and <b>-1</b> for error.
   */
  int finalize();

  /** Destructor. */
  ~QueryProcessor();

  /* ********************************* */
  /*              METADATA             */
  /* ********************************* */

  // TODO
  int metadata_export(
      const std::string& metadata,
      const std::string& file,
      const std::string& key,
      const std::vector<std::string>& attributes,
      const std::string& format,
      char delimiter,
      int precision) const;

  /* ********************************* */
  /*          QUERY FUNCTIONS          */
  /* ********************************* */

  /** 
   * Exports the data of an array into a CSV or binary file. The documentation
   * on the exported CSV or binary format can be found in 
   * tiledb_array_load(). Also more information is included in 
   * tilebd_array_export(). 
   *
   * @param workspace The workspace where the array is defined.
   * @param group The group inside the workspace where the array is defined.
   * @param array_name The name of the array whose data will be exported.
   * @param filename The name of the exported file. 
   * @param format It can be one of the following: 
   * - **csv** (CSV format)
   * - **csv.gz** (GZIP-compressed CSV format)
   * - **dense.csv** (dense CSV format)
   * - **dense.csv.gz** (GZIP-compressed dense CSV format)
   * - **reverse.csv** (CSV format in reverse order)
   * - **reverse.csv.gz** (GZIP-compressed CSV format in reverse order)
   * - **reverse.dense.csv** (dense CSV format in reverse order)
   * - **reverse.dense.csv.gz** (dense GZIP-compressed CSV format in reverse 
   *   order)
   * - **bin** (binary format)
   * - **bin.gz** (GZIP-compressed binary format)
   * - **dense.bin** (dense binary format)
   * - **dense.bin.gz** (GZIP-compressed dense binary format)
   * - **reverse.bin** (binary format in reverse order)
   * - **reverse.bin.gz** (GZIP-compressed binary format in reverse order)
   * - **reverse.dense.bin** (dense binary format in reverse order)
   * - **reverse.dense.bin.gz** (dense GZIP-compressed binary format in reverse 
   *   order).
   * @param dim_names A vector holding the dimension names to be exported. If it
   * is empty, then all the coordinates will be exported. If it contains special
   * name "__hide", then no coordinates will be exported.
   * @param attribute_names A vector holding the attribute names to be exported.
   * If it is empty, then all the attribute values will be exported. If it 
   * contains special name "__hide", then no attribute values will be
   * exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param delimiter This is meaningful only for CSV format. It stands for the 
   * delimiter which separates the values in a CSV line in the CSV file. If not 
   * given, the default is CSV_DELIMITER. The delimiter is ignored in the case
   * of loading binary data.    
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export(
      const std::string& workspace,
      const std::string& group,
      const std::string& array_name,
      const std::string& filename,
      const std::string& format,
      const std::vector<std::string>& dim_names, 
      const std::vector<std::string>& attribute_names,
      const std::vector<double>& range,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  int array_export(
      const std::string& array_name,
      const std::string& filename,
      const std::string& format,
      const std::vector<std::string>& dim_names, 
      const std::vector<std::string>& attribute_names,
      const std::vector<double>& range,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Creates a new array with the same schema as the input array (or including a
   * subset of the attributes in a potentially different order), 
   * conataining only the cells that lie in the input range. The range must be a
   * hyper-rectangle that is completely contained in the dimension space. It is
   * also given as a sequence of [low,high] pairs across each dimension. 
   *
   * @param workspace The workspace where the array is defined.
   * @param workspace_sub The path to the workspace where the subarray result
   * will be stored. If *workspace_sub* is "", then the input array workspace
   * is set as the result workspace by default.
   * @param group The group inside the workspace where the array is defined.
   * @param group_sub The path to the group where the subarray result is stored.
   * If *group_sub* is "", then *workspace_sub* is set as the group by default.
   * @param array_name The name of the array the subarray will be applied on.
   * @param array_name_sub The name of the output array.
   * @param range The range of the subarray. 
   * @param attribute_names An array holding the attribute names to be included
   * in the schema of the result array. If it is empty, then all the attributes
   * of the input array will appear in the output array.
   * @return **0** for success and <b>-1</b> for error.
   */
  int subarray(
      const std::string& workspace, 
      const std::string& workspace_sub, 
      const std::string& group, 
      const std::string& group_sub, 
      const std::string& array_name, 
      const std::string& array_name_sub, 
      const std::vector<double>& range,
      const std::vector<std::string>& attribute_names) const;

  int subarray(
      const std::string& array_name, 
      const std::string& array_name_sub, 
      const std::vector<double>& range,
      const std::vector<std::string>& attribute_names) const;

  /** 
   * This is very similar to subarray(). The difference is that the result
   * cells are written in the provided buffer, serialized one after the other.
   * See tiledb_array_load() for more information on the binary cell format.
   * The function fails if the provided buffer size is not sufficient to hold
   * all the cells, and the buffer size is set to -1.
   *
   * @param ad The descriptor of the array where the subarray is applied.
   * @param range The range of the subarray. 
   * @param dim_names A vector holding the names of the dimensions whose 
   * coordinates will appear in the result cells. If it is NULL, then *all* the 
   * coordinates will appear in the result cells. If it contains special 
   * name <b>"__hide"</b>, then *no* coordinates will appear.
   * @param attribute_names A vector holding the names of the attribute whose
   * values will be included in the result cells. If it is NULL, then *all* the 
   * attributes of the input array will appear in the result cells. If there is 
   * only one special attribute name "__hide", then *no* attribute value will be
   * included in the result cells.
   * @param buffer The buffer where the result cells are written.
   * @param buffer_size The size of the input buffer. If the function succeeds,
   * it is set to the actual size occupied by the result cells. If the function
   * fails because the size of the result cells exceeds the buffer size, it is 
   * set to -1.
   * @return **0** for success and <b>-1</b> for error.
   */
  int subarray_buf(
      int ad,
      const std::vector<double>& range,
      const std::vector<std::string>& dim_names,
      const std::vector<std::string>& attribute_names,
      void* buffer,
      size_t& buffer_size) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** *True* if the constructor succeeded, or *false* otherwise. */
  bool created_successfully_;
  /** *True* if the object was finalized, or *false* otherwise. */
  bool finalized_;
  /** The StorageManager object the query processor will be interfacing with. */
  StorageManager* storage_manager_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * reverse order of the native cell order of the array. Also they are exported
   * in dense form, i.e., even the empty cells will be exported (which will
   * have 0 in the numerical attributes, and NULL_CHAR in the char attributes).
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_csv_reverse_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * reverse order of the native cell order of the array. Also they are exported
   * in dense form, i.e., even the empty cells will be exported (which will
   * have 0 in the numerical attributes, and NULL_CHAR in the char attributes).
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_csv_reverse_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * reverse order of the native cell order of the array. Also they are exported
   * in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_csv_reverse_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER, 
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * reverse order of the native cell order of the array. Also they are exported
   * in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_csv_reverse_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * native cell order of the array. Also they are exported
   * in dense form, i.e., even the empty cells will be exported.
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_csv_normal_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * native cell order of the array. Also they are exported
   * in dense form, i.e., even the empty cells will be exported.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_csv_normal_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * native cell order of the array. Also they are exported
   * in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_csv_normal_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a CSV file. The data are exported in the
   * native cell order of the array. Also they are exported
   * in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported CSV file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @param delimiter It stands for the delimiter which separates the values in 
   * a CSV line in the CSV file. If not given, the default is CSV_DELIMITER.
   * @param precision This only applies to exporting to CSV files (it is ignored
   * in the case of binary files). It indicates the number of decimal digits
   * to print for the case of real values. If not given, the default is 
   * PRECISION.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_csv_normal_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression,
      char delimiter = CSV_DELIMITER,
      int precision = PRECISION) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in 
   * the reverse order of the native cell order of the array. Also they are
   * exported in dense form, i.e., even the empty cells will be exported.
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_bin_reverse_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in 
   * the reverse order of the native cell order of the array. Also they are
   * exported in dense form, i.e., even the empty cells will be exported.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_bin_reverse_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in
   * the reverse order of the native cell order of the array. Also they are
   * exported in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_bin_reverse_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in
   * the reverse order of the native cell order of the array. Also they are
   * exported in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_bin_reverse_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in
   * the native cell order of the array. Also they are exported
   * in dense form, i.e., even the empty cells will be exported.
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_bin_normal_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in
   * the native cell order of the array. Also they are exported
   * in dense form, i.e., even the empty cells will be exported.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_bin_normal_dense(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in
   * the native cell order of the array. Also they are exported
   * in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_export_bin_normal_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const std::vector<double>& range,
      CompressionType compression) const;

  /** 
   * Exports the data of an array into a binary file. The data are exported in
   * the native cell order of the array. Also they are exported
   * in sparse form, i.e., the empty cells will *not* be exported.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param filename The name of the exported binary file. 
   * @param dim_ids A vector holding the ids of the dimensions to be exported.
   * If empty, no dimension is exported.
   * @param attribute_ids A vector holding the attribute ids to be exported.
   * If empty, no attribute is exported.
   * @param range A range given as a sequence of [low,high] bounds across each 
   * dimension. Each range bound must be a real number. The range constrains the
   * exported cells into a subarray.
   * @param compression The compression type of the exported file.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int array_export_bin_normal_sparse(
      int ad,
      const std::string& filename,
      const std::vector<int>& dim_ids, 
      const std::vector<int>& attribute_ids,
      const T* range,
      CompressionType compression) const;

  /** 
   * Converts the input *old_range* (which is always double), to the 
   * appropriate templated type.
   *
   * @tparam T The target type of the new range.
   * @param old_range The input range to be converted.
   * @param new_range The new range in the templated type.
   */
  template<class T>
  void calculate_new_range(
      const std::vector<double>& old_range,
      T*& new_range) const;

  /** 
   * Performs the subarray query on the first array, storing the result on
   * the second array, using the input range and focusing on the input
   * attribute ids. The result is stored in sparse format, i.e., the empty
   * cells are not stored. 
   *
   * @tparam T The coordinates type (of both arrays).
   * @param ad The descriptor of the input array.
   * @param ad_sub The descriptor of the result array.
   * @param range The range of the subarray.
   * @param attribute_ids A vector holding the ids of the attributes
   * in the schema of the result array. 
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int subarray(
      int ad, 
      int ad_sub, 
      const T* range,
      const std::vector<int>& attribute_ids) const;

  /** 
   * This is very similar to subarray(). The difference is that the result
   * cells are written in the provided buffer, serialized one after the other.
   * See tiledb_array_load() for more information on the binary cell format.
   * The function fails if the provided buffer size is not sufficient to hold
   * all the cells, and the buffer size is set to -1.
   *
   * @tparam T The array coordinates type.
   * @param ad The descriptor of the array where the subarray is applied.
   * @param range The range of the subarray. 
   * @param dim_ids A vector holding the ids of the dimension coordinates
   * to be included in the result cells. 
   * @param attribute_ids A vector holding the ids of the attributes
   * to be included in the result cells. 
   * @param buffer The buffer where the result cells are written.
   * @param buffer_size The size of the input buffer. If the function succeeds,
   * it is set to the actual size occupied by the result cells. If the function
   * fails because the size of the result cells exceeds the buffer size, it is 
   * set to -1.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int subarray_buf(
      int ad,
      const T* range,
      const std::vector<int>& dim_ids,
      const std::vector<int>& attribute_ids,
      void* buffer,
      size_t& buffer_size) const;



  // TODO
  int metadata_export_csv(
      int md,
      const std::string& file,
      const std::string& key,
      const std::vector<int>& attribute_ids,
      CompressionType compression,
      char delimiter,
      int precision) const;

  // TODO
  template<class T>
  int metadata_export_csv(
      int md,
      const std::string& file,
      const std::string& key,
      const std::vector<int>& attribute_ids,
      CompressionType compression,
      char delimiter,
      int precision) const;

  // TODO
  int metadata_export_bin(
      int md,
      const std::string& file,
      const std::string& key,
      const std::vector<int>& attribute_ids,
      CompressionType compression) const;

  // TODO
  template<class T>
  int metadata_export_bin(
      int md,
      const std::string& file,
      const std::string& key,
      const std::vector<int>& attribute_ids,
      CompressionType compression) const;
};

#endif
