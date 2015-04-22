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
 * distributed matrix A using TileDB.
 */

#include "storage_manager.h"
#include "loader.h"
#include "query_processor.h"
#include "mpi_handler.h"
#include <sstream>
#include <iostream>
#include <string.h>
#include <stdlib.h>

// Creates the tranpose matrix A_t of A
void transpose(StorageManager* storage_manager, MPIHandler* mpi_handler, 
               const std::string& A, const std::string& A_t,
               const ArraySchema* array_schema_A) {
  // For easy reference
  size_t cell_size = array_schema_A->cell_size();

  // Open array A in read mode
  int ad_A = storage_manager->open_array(A, "r");
  if(ad_A == -1)
    throw (std::string("Cannot open array ") + A + ".").c_str(); 

  // Open array A_t in write mode
  int ad_A_t = storage_manager->open_array(A_t, "w");
  if(ad_A_t == -1)
    throw (std::string("Cannot open array ") + A_t + ".").c_str(); 

  // The number of rows that must be gathered by each process.
  // These rows will be the columns of the tranpose result.
  // We assume an even distribution of the result columns across processes.
  int64_t rows_range = array_schema_A->dim_domains()[0].second - 
                       array_schema_A->dim_domains()[0].first + 1; 
  int64_t rows_per_proc = rows_range / mpi_handler->proc_num(); 

  // Range of cells that needs to be fetched by each process.
  // Note that the columns to be retrieved remain fixed across proceses,
  // as all of them must be retrieved - only the rows subsets vary.
  int64_t range[4];
  range[2] = array_schema_A->dim_domains()[1].first;  // cols low
  range[3] = array_schema_A->dim_domains()[1].second; // cols high
 
  // Gather rows
  void* cells;
  int64_t cell_num; 
  for(int i=0; i<mpi_handler->proc_num(); ++i) {
    // Calculate set of rows that must be received from each process
    range[0] = i * rows_per_proc;         // rows low
    range[1] = (i+1) * rows_per_proc - 1; // rows high 
    // Retrieve rows
    storage_manager->read_cells(ad_A, range, cells, cell_num, i);
  } 

  // Transpose retrieved cells.
  size_t offset = 0;
  int64_t temp;
  for(int64_t i=0; i<cell_num; ++i) {
    memcpy(&temp, 
           static_cast<char*>(cells) + offset, 
           sizeof(int64_t));
    memcpy(static_cast<char*>(cells) + offset,
           static_cast<char*>(cells) + offset + sizeof(int64_t), 
           sizeof(int64_t));
    memcpy(static_cast<char*>(cells) + offset + sizeof(int64_t),
           &temp, 
           sizeof(int64_t));
    offset += cell_size;
  }

  // Write tranposed cells to result array A_t
  storage_manager->write_cells(ad_A_t, cells, cell_num);

  // Clean up
  free(cells);
  storage_manager->close_array(ad_A); 
  storage_manager->close_array(ad_A_t); 
}

// Returns an (ad hoc) schema for a matrix
const ArraySchema* get_array_schema() {
  // Array name
  std::string array_name = "A";

  // Attribute name
  std::vector<std::string> attribute_names;
  attribute_names.push_back("values");

  // Dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("rows");
  dim_names.push_back("columns");

  // Dimension domains
  std::vector<std::pair<double, double> > dim_domains;
  dim_domains.push_back(std::pair<double, double>(0, 59));
  dim_domains.push_back(std::pair<double, double>(0, 99));

  // Types
  // The values are double, (both) the matrix indices are int64_t
  std::vector<const std::type_info*> types;
  types.push_back(&typeid(double)); 
  types.push_back(&typeid(int64_t)); 

  // Cell order (column-major order)
  ArraySchema::CellOrder cell_order = ArraySchema::CO_COLUMN_MAJOR;

  return new ArraySchema(array_name, attribute_names, dim_names, dim_domains,
                         types, cell_order); 
} 

int main(int argc, char** argv) {
  // Create an MPI handler, which initializes the MPI wolrd
  MPIHandler* mpi_handler = new MPIHandler(&argc, &argv);

  // Create a storage manager module
  StorageManager* storage_manager = 
      new StorageManager("~/stavrospapadopoulos/TileDB/example_transpose/",
                         mpi_handler);

  // Create a loader module
  Loader* loader = new Loader(storage_manager);

  // Create a query processor module
  QueryProcessor* query_processor = new QueryProcessor(storage_manager);

  // Define a matrix A with some ad hoc schema
  const ArraySchema* array_schema_A = get_array_schema();
  storage_manager->define_array(array_schema_A);

  // Define the transpose of A, A_t
  const ArraySchema* array_schema_A_t = array_schema_A->transpose("A_t");
  storage_manager->define_array(array_schema_A_t);

  // Load a CSV file into A
  try {
    std::cout << "Proc " << mpi_handler->rank() 
              << ": Loading CSV file to array A...\n";
    std::stringstream csv_filename;
    csv_filename << "~/stavrospapadopoulos/TileDB/data/A_" 
                 << mpi_handler->rank() << ".csv";
    loader->load_csv(csv_filename.str(), "A");
  } catch(LoaderException& le) {
    std::cout << "[Proc_" << mpi_handler->rank() 
              << "::TileDB::Loader::fatal_error]: " << le.what() << "\n";
  }

  // Export A (for debugging) 
  try {
    std::cout << "Proc " << mpi_handler->rank() 
              << ": Exporting array A...\n";
    std::stringstream csv_filename;
    csv_filename << "export_A_" << mpi_handler->rank() << ".csv";
    query_processor->export_to_csv("A", csv_filename.str());
  } catch(QueryProcessorException& qpe) {
    std::cout << "[Proc_" << mpi_handler->rank() 
              << "::TileDB::QueryProcessor::fatal_error]: " 
              << qpe.what() << "\n";
  }

  // Compute transpose 
  try {
    std::cout << "Proc " << mpi_handler->rank() 
              << ": Computing the tranpose A_t of array A...\n";
    transpose(storage_manager, mpi_handler, "A", "A_t", array_schema_A);
  } catch(const char* msg)  {
    std::cout << "[Proc_" << mpi_handler->rank() 
              << "::TileDB::transpose::fatal_error]: " 
              << msg << "\n";
  }

  // Export A_t (for debugging) 
   try {
    std::cout << "Proc " << mpi_handler->rank() 
              << ": Exporting array A_t...\n";
    std::stringstream csv_filename;
    csv_filename << "export_A_t_" << mpi_handler->rank() << ".csv";
    query_processor->export_to_csv("A_t", csv_filename.str());
  } catch(QueryProcessorException& qpe) {
    std::cout << "[Proc_" << mpi_handler->rank() 
              << "::TileDB::QueryProcessor::fatal_error]: " 
              << qpe.what() << "\n";
  }

  // Clean up
  delete mpi_handler;
  delete storage_manager;
  delete array_schema_A;
  delete array_schema_A_t;

  return 0;
}

