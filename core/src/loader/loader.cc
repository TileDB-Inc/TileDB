/**
 * @file   loader.cc
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
 * This file implements the Loader class.
 */

#include "bin_file.h"
#include "bin_file_collection.h"
#include "cell.h"
#include "csv_file.h"
#include "csv_file_collection.h"
#include "loader.h"
#include "progress_bar.h"
#include "utils.h"
#include <assert.h>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <utils.h>

// Macro for printing error message in VERBOSE mode
#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << "[TileDB::Loader] Error: " << x \
                                   << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::Loader] Warning: " \
                                     << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Loader::Loader(StorageManager* storage_manager)
    : storage_manager_(storage_manager) {
  finalized_ = false;
  created_successfully_ = true;
}

bool Loader::created_successfully() const {
  return created_successfully_;
}

int Loader::finalize() {
  finalized_ = true;

  // Success
  return 0;
}

Loader::~Loader() {
  if(!finalized_) {
    PRINT_WARNING("Loader not finalized. Finalizing now");
    finalize();
  }
}

/* ****************************** */
/*        LOADING FUNCTIONS       */
/* ****************************** */

int Loader::array_load(
    const std::string& workspace, 
    const std::string& group, 
    const std::string& array_name, 
    const std::string& path,
    const std::string& format,
    char delimiter,
    bool update,
    bool real_paths) const {
  // Get real paths
  std::string workspace_real, group_real, path_real; 
  if(storage_manager_->real_paths_get(
         workspace,  
         group, 
         workspace_real, 
         group_real, 
         real_paths))
    return -1;
  if(real_paths) 
    path_real = path;
  else 
    path_real = real_path(path);
  if(path_real == "") {
    PRINT_ERROR("Invalid path for loading");
    return -1;
  }

  // Resolve CSV or BIN
  bool bin = 
      (format == "bin" || format == "sorted.bin" || format == "bin.gz" || format == "sorted.bin.gz");
  bool csv = 
      (format == "csv" || format == "sorted.csv" || format == "csv.gz" || format == "sorted.csv.gz");
  if(!bin && !csv) {
    PRINT_ERROR("Invalid collection format");
    return -1;
  }

  // Resolve compression type
  CompressionType compression;
  if(ends_with(format, ".gz"))
    compression = CMP_GZIP;
  else
    compression = CMP_NONE;

  // Open array
  int ad = storage_manager_->array_open(
               workspace_real, 
               group_real, 
               array_name, 
               (update) ? "a" : "w", 
               true);
  if(ad == -1) 
    return -1;

  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* coords_type = array_schema->coords_type();

  // Resolve if sorted or not
  bool sorted;
  if(starts_with(format, "sorted"))
    sorted = true;
  else
    sorted = false;

  // Load
  int rc; 
  if(coords_type == &typeid(int)) {
    if(bin)
      rc = array_load_bin<int>(ad, path, sorted, compression);
    else if(csv)
      rc = array_load_csv<int>(ad, path, sorted, compression, delimiter);
  } else if(coords_type == &typeid(int64_t)) {
    if(bin)
      rc = array_load_bin<int64_t>(ad, path, sorted, compression);
    else if(csv)
      rc = array_load_csv<int64_t>(ad, path, sorted, compression, delimiter);
  } else if(coords_type == &typeid(float)) {
    if(bin)
      rc = array_load_bin<float>(ad, path, sorted, compression);
    else if(csv)
      rc = array_load_csv<float>(ad, path, sorted, compression, delimiter);
  } else if(coords_type == &typeid(double)) {
    if(bin)
      rc = array_load_bin<double>(ad, path, sorted, compression);
    else if(csv)
      rc = array_load_csv<double>(ad, path, sorted, compression, delimiter);
  } else {
    PRINT_ERROR("Invalid array coordinates type");
    rc = -1;
  }

  // Close array 
  if(rc == -1) {
    storage_manager_->array_close_forced(ad);
    storage_manager_->array_delete(
        workspace_real, group_real, array_name, true);
  } else {
    if(storage_manager_->array_close(ad))
      rc = -1;
  }

  return rc;
}

int Loader::array_load(
    const std::string& array_name, 
    const std::vector<std::string>& files,
    const std::string& format,
    char delimiter) const {
  // Resolve CSV or BIN
  bool bin = 
      (format == "bin" || format == "sorted.bin" || 
       format == "dense.bin" || format == "dense.bin.gz" ||
       format == "bin.gz" || format == "sorted.bin.gz");
  bool csv = 
      (format == "csv" || format == "sorted.csv" || 
       format == "dense.csv" || format == "dense.csv.gz" ||
       format == "csv.gz" || format == "sorted.csv.gz");
  if(!bin && !csv) {
    PRINT_ERROR("Invalid collection format");
    return -1;
  }

  // Resolve compression type
  CompressionType compression;
  if(ends_with(format, ".gz"))
    compression = CMP_GZIP;
  else
    compression = CMP_NONE;

  // Resolve dense
  bool dense;
  if(starts_with(format, "dense"))
    dense = true;
  else
    dense = false;

  // In case of dense, the input should be a single file
  if(dense && files.size() != 1) {
    PRINT_ERROR("The input when loading in dense format should "
                "be a single file");
    return -1;
  }

  // Resolve if sorted or not
  bool sorted;
  if(dense || starts_with(format, "sorted"))
    sorted = true;
  else
    sorted = false;
 
  // Open array
  int ad = storage_manager_->array_open(array_name, "w");
  if(ad == -1) 
    return TILEDB_LD_ERR;

  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return TILEDB_LD_ERR;
  const std::type_info* coords_type = array_schema->coords_type();

  // Load
  int rc; 
  if(coords_type == &typeid(int)) {
    if(bin) {
      if(dense)
        rc = array_load_bin_dense<int>(ad, files[0], compression);
      else
        rc = array_load_bin<int>(ad, files, sorted, compression);
    } else if(csv) {
      if(dense)
        rc = array_load_csv_dense<int>(ad, files[0], compression, delimiter);
      else
        rc = array_load_csv<int>(ad, files, sorted, compression, delimiter);
    }
  } else if(coords_type == &typeid(int64_t)) {
    if(bin) {
      if(dense)
        rc = array_load_bin_dense<int64_t>(ad, files[0], compression);
      else
        rc = array_load_bin<int64_t>(ad, files, sorted, compression);
    } else if(csv) {
      if(dense)
        rc = array_load_csv_dense<int64_t>(ad, files[0], compression, delimiter);
      else
        rc = array_load_csv<int64_t>(ad, files, sorted, compression, delimiter);
    }
  } else if(coords_type == &typeid(float)) {
    assert(!dense);
    if(bin)
      rc = array_load_bin<float>(ad, files, sorted, compression);
    else if(csv)
      rc = array_load_csv<float>(ad, files, sorted, compression, delimiter);
  } else if(coords_type == &typeid(double)) {
    assert(!dense);
    if(bin)
      rc = array_load_bin<double>(ad, files, sorted, compression);
    else if(csv)
      rc = array_load_csv<double>(ad, files, sorted, compression, delimiter);
  } else {
    PRINT_ERROR("Invalid array coordinates type");
    rc = -1;
  }

  // Close array 
  if(rc == -1) {
    storage_manager_->array_close_forced(ad);
  } else {
    if(storage_manager_->array_close(ad))
      rc = -1;
  }

  return rc;
}

int Loader::metadata_load(
    const std::string& metadata_name, 
    const std::vector<std::string>& files,
    const std::string& format,
    char delimiter) const {
  // Open array
  int md = storage_manager_->metadata_open(metadata_name, "w");
  if(md == -1) 
    return TILEDB_LD_ERR;

  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->metadata_schema_get(md, array_schema))
    return TILEDB_LD_ERR;
  const std::type_info* coords_type = array_schema->coords_type();

  // Resolve format 
  if(format != "csv" && format != "csv.gz") {
    PRINT_ERROR("Invalid collection format");
    return TILEDB_LD_ERR;
  }

  // Resolve compression type
  CompressionType compression;
  if(ends_with(format, ".gz"))
    compression = CMP_GZIP;
  else
    compression = CMP_NONE;

  // Load
  int rc; 
  if(coords_type == &typeid(int)) {
    rc = metadata_load_csv<int>(md, files, false, compression, delimiter);
  } else if(coords_type == &typeid(int64_t)) {
    rc = metadata_load_csv<int64_t>(
             md, 
             files, 
             false, 
             compression, 
             delimiter);
  } else if(coords_type == &typeid(float)) {
    rc = metadata_load_csv<float>(md, files, false, compression, delimiter);
  } else if(coords_type == &typeid(double)) {
    rc = metadata_load_csv<double>(
             md, 
             files, 
             false, 
             compression, 
             delimiter);
  } else {
    PRINT_ERROR("Invalid array coordinates type");
    rc = TILEDB_LD_ERR;
  }

  // Close array 
  if(rc == TILEDB_LD_ERR) {
    storage_manager_->metadata_close_forced(md);
  } else {
    if(storage_manager_->metadata_close(md))
      rc = TILEDB_LD_ERR;
  }

  return rc;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template<class T>
int Loader::array_load_bin(
    int ad, 
    const std::string& path,
    bool sorted,
    CompressionType compression) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;

  // Open the BIN file collection 
  BINFileCollection<T> bin_file_collection(compression);
  if(bin_file_collection.open(array_schema, 0, path, sorted))
    return -1;

  // Prepare a cell
  Cell cell(array_schema);

  // Load
  while(bin_file_collection >> cell) { 
    if(sorted) {
      if(storage_manager_->cell_write_sorted<T>(ad, cell.cell()))
        return -1;
    } else {
      if(storage_manager_->cell_write<T>(ad, cell.cell()))
        return -1;
    }
  }

  // Clean up
  if(bin_file_collection.close())
    return -1;

  return 0;
}

template<class T>
int Loader::array_load_bin(
    int ad, 
    const std::vector<std::string>& files,
    bool sorted,
    CompressionType compression) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;

  // Open the BIN file collection 
  BINFileCollection<T> bin_file_collection(compression);
  if(bin_file_collection.open(array_schema, 0, files, sorted))
    return -1;

  // Prepare a cell
  Cell cell(array_schema);

  // Load
  while(bin_file_collection >> cell) { 
    if(sorted) {
      if(storage_manager_->cell_write_sorted<T>(ad, cell.cell()))
        return -1;
    } else {
      if(storage_manager_->cell_write<T>(ad, cell.cell()))
        return -1;
    }
  }

  // Clean up
  if(bin_file_collection.close())
    return -1;

  return 0;
}

template<class T>
int Loader::array_load_bin_dense(
    int ad, 
    const std::string& file,
    CompressionType compression) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;

  // Open the BIN file collection 
  BINFile bin_file(array_schema, compression, true);
  if(bin_file.open(file, "r", true))
    return -1;

  // Prepare a cell
  Cell cell(array_schema, 0, false, true);

  // Load
  while(bin_file >> cell) { 
    if(storage_manager_->cell_write_sorted<T>(ad, cell.cell(), true))
      return -1;
  }

  // Clean up
  if(bin_file.close())
    return -1;

  return 0;
}

template<class T>
int Loader::array_load_csv_dense(
    int ad, 
    const std::string& file,
    CompressionType compression,
    char delimiter) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;

  // Open the BIN file collection 
  CSVFile csv_file(array_schema, compression, delimiter, true);
  if(csv_file.open(file, "r"))
    return -1;

  // Prepare a cell
  Cell cell(array_schema, 0, false, true);

  // Load
  while(csv_file >> cell) { 
    if(storage_manager_->cell_write_sorted<T>(ad, cell.cell(), true))
      return -1;
  }

  // Clean up
  csv_file.close();

  return 0;
}

template<class T>
int Loader::array_load_csv(
    int ad, 
    const std::string& path,
    bool sorted,
    CompressionType compression,
    char delimiter) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;

  // Open the CSV file collection 
  CSVFileCollection<T> csv_file_collection(compression, delimiter);
  if(csv_file_collection.open(array_schema, path, sorted))
    return -1;

  // Prepare a cell
  Cell cell(array_schema);

  // Load
  while(csv_file_collection >> cell) { 
    if(sorted) {
      if(storage_manager_->cell_write_sorted<T>(ad, cell.cell()))
        return -1;
    } else {
      if(storage_manager_->cell_write<T>(ad, cell.cell()))
        return -1;
    }
  }

  // Clean up
  if(csv_file_collection.close())
    return -1;

  return 0;
}

template<class T>
int Loader::array_load_csv(
    int ad, 
    const std::vector<std::string>& files,
    bool sorted,
    CompressionType compression,
    char delimiter) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->array_schema_get(ad, array_schema))
    return -1;

  // Open the CSV file collection 
  CSVFileCollection<T> csv_file_collection(compression, delimiter);
  if(csv_file_collection.open(array_schema, files, sorted))
    return -1;

  // Prepare a cell
  Cell cell(array_schema);

  // Load
  while(csv_file_collection >> cell) { 
    if(sorted) {
      if(storage_manager_->cell_write_sorted<T>(ad, cell.cell()))
        return -1;
    } else {
      if(storage_manager_->cell_write<T>(ad, cell.cell()))
        return -1;
    }
  }

  // Clean up
  if(csv_file_collection.close())
    return -1;

  return 0;
}

template<class T>
int Loader::metadata_load_csv(
    int md, 
    const std::vector<std::string>& files,
    bool sorted,
    CompressionType compression,
    char delimiter) const {
  // For easy reference
  const ArraySchema* array_schema;
  if(storage_manager_->metadata_schema_get(md, array_schema))
    return TILEDB_LD_ERR;

  // Open the CSV file collection 
  CSVFileCollection<T> csv_file_collection(
      compression, 
      delimiter,
      true);
  if(csv_file_collection.open(array_schema, files, sorted))
    return TILEDB_LD_ERR;

  // Prepare a cell
  Cell cell(array_schema);

  // Load
  // TODO: change the while condition to check for errors
  while(csv_file_collection >> cell) { 
    if(sorted) {
      if(storage_manager_->metadata_write_sorted<T>(md, cell.cell()))
        return TILEDB_LD_ERR;
    } else {
      if(storage_manager_->metadata_write<T>(md, cell.cell()))
        return TILEDB_LD_ERR;
    }
  }

  // Clean up
  if(csv_file_collection.close())
    return TILEDB_LD_ERR;

  // Success
  return TILEDB_LD_OK;
}
