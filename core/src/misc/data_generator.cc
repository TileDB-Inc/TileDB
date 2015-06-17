/**
 * @file   data_generator.cc
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
 * This file implements the DataGenerator class.
 */

#include "csv_file.h"
#include "data_generator.h"
#include "utils.h"
#include <assert.h>
#include <iostream>
#include <sstream>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

DataGenerator::DataGenerator(const ArraySchema* array_schema)
    : array_schema_(array_schema) {
}

DataGenerator::~DataGenerator() {
}

/******************************************************
************* DATA GENERATION FUNCTIONS ***************
******************************************************/

int DataGenerator::generate_sorted_uniform_bin(
    unsigned seed, const std::string& filename, 
    int64_t cell_num, std::string& err_msg) const {

  // TODO
  err_msg = "Under construction...";
  return -1;
}

int DataGenerator::generate_sorted_uniform_bin(
    unsigned seed, const std::string& filename, 
    size_t file_size, std::string& err_msg) const {
  // TODO
  err_msg = "Under construction...";
  return -1;
}

int DataGenerator::generate_sorted_uniform_csv(
    unsigned seed, const std::string& filename, 
    int64_t cell_num, std::string& err_msg) const {
  // TODO
  err_msg = "Under construction...";
  return -1;
}

int DataGenerator::generate_sorted_uniform_csv(
    unsigned seed, const std::string& filename, 
    size_t file_size, std::string& err_msg) const {
  // TODO
  err_msg = "Under construction...";
  return -1;
}

int DataGenerator::generate_uniform_bin(
    unsigned seed, const std::string& filename, 
    int64_t cell_num, std::string& err_msg) const {
  // TODO
  err_msg = "Under construction...";
  return -1;
}

int DataGenerator::generate_uniform_bin(
    unsigned seed, const std::string& filename, 
    size_t file_size, std::string& err_msg) const {
  // TODO
  err_msg = "Under construction...";
  return -1;
}

int DataGenerator::generate_uniform_csv(
    unsigned seed, const std::string& filename, 
    int64_t cell_num, std::string& err_msg) const {
  assert(cell_num >= 0);

  // Initialization of output CSV file
  CSVFile csv_file(filename, "w");
  CSVLine csv_line;

  // Initialization of generators
  std::default_random_engine generator(seed);

  for(int64_t i=0; i<cell_num; ++i) {
    generate_uniform_coordinates(generator, csv_line);
    generate_uniform_attributes(generator, csv_line);
    csv_file << csv_line;
    csv_line.clear();
  }

  // Clean up
  csv_file.close();

  return 0;
}

int DataGenerator::generate_uniform_csv(
    unsigned seed, const std::string& filename, 
    size_t file_size, std::string& err_msg) const {
  // TODO
  err_msg = "Under construction...";
  return -1;
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

void DataGenerator::generate_uniform_attributes(
    std::default_random_engine& generator, CSVLine& csv_line) const {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  for(int i=0; i<attribute_num; ++i) {
    const std::type_info* attr_type = array_schema_->type(i);
    int val_num = array_schema_->val_num(i);

    // Determine random number of values for variable-sized cells 
    if(val_num == VAR_SIZE) {
      std::uniform_int_distribution<int> distribution(1,10);
      val_num = distribution(generator);
      if(attr_type != &typeid(char))
        csv_line << val_num;
    }

    // Generate value(s) for each attribute
    if(attr_type == &typeid(char)) {
      std::stringstream s;
      std::uniform_int_distribution<char> distribution(45,126);
      for(int i=0; i<val_num; ++i) 
        s << distribution(generator); 
      csv_line << s.str();
    } else if(attr_type == &typeid(int)) {
      std::uniform_int_distribution<int> distribution;
      for(int i=0; i<val_num; ++i) 
        csv_line << distribution(generator);
    } else if(attr_type == &typeid(int64_t)) {
      std::uniform_int_distribution<int64_t> distribution;
      for(int i=0; i<val_num; ++i) 
        csv_line << distribution(generator);
    } else if(attr_type == &typeid(float)) {
      std::uniform_real_distribution<float> distribution;
      for(int i=0; i<val_num; ++i) 
        csv_line << distribution(generator);
    } else if(attr_type == &typeid(double)) {
      std::uniform_real_distribution<double> distribution;
      for(int i=0; i<val_num; ++i) 
        csv_line << distribution(generator);
    }
  }
}

void DataGenerator::generate_uniform_coordinates(
    std::default_random_engine& generator, CSVLine& csv_line) const {
  // For easy referemce
  int dim_num = array_schema_->dim_num();
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  const ArraySchema::DimDomains& dim_domains = array_schema_->dim_domains();

  if(coords_type == &typeid(int)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_int_distribution<int> distribution(
          int(dim_domains[i].first), int(dim_domains[i].second));
      int c = distribution(generator);
      csv_line << c;
    }
  } else if(coords_type == &typeid(int64_t)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_int_distribution<int64_t> distribution(
          int64_t(dim_domains[i].first), int64_t(dim_domains[i].second));
      int64_t c = distribution(generator);
      csv_line << c;
    }
  } else if(coords_type == &typeid(float)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_real_distribution<float> distribution(
          float(dim_domains[i].first), float(dim_domains[i].second));
      float c = distribution(generator);
      csv_line << c;
    }
  } else if(coords_type == &typeid(double)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_real_distribution<double> distribution(
          double(dim_domains[i].first), double(dim_domains[i].second));
      double c = distribution(generator);
      csv_line << c;
    }
  }
}


