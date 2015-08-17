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

#include "bin_file.h"
#include "csv_file.h"
#include "data_generator.h"
#include "progress_bar.h"
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

int DataGenerator::generate_bin(
    unsigned seed, 
    const std::string& filename, 
    int64_t cell_num) const {
// TODO: error messages
  assert(cell_num >= 0);

  // For easy reference
  bool var_size = (array_schema_->cell_size() == VAR_SIZE);
  size_t coords_size = array_schema_->coords_size();
  int err;

  // Intialize a progress bar
  ProgressBar bar(cell_num);

  // Initialization of output BIN file
  BINFile bin_file(filename, "w");

  // Initialization of generators
  std::default_random_engine generator(seed);

  // Buffer initialization
  char* buffer = new char[SEGMENT_SIZE];
  size_t offset; 

  for(int64_t i=0; i<cell_num; ++i) {
    bar.load(1);
    generate_uniform_coordinates(generator, buffer);
    offset = coords_size + ((var_size) ? sizeof(size_t) : 0);
    offset += generate_uniform_attributes(generator, buffer+offset);
    if(var_size)
      memcpy(buffer+coords_size, &offset, sizeof(size_t));
    err = bin_file.write(buffer, offset);
    assert(err != -1);
  }

  // Clean up
  bin_file.close();
  delete [] buffer;

  return 0;
}

int DataGenerator::generate_csv(
    unsigned seed, 
    const std::string& filename, 
    int64_t cell_num) const {
// TODO: error messages
  assert(cell_num >= 0);

  // Initialization of output CSV file
  CSVFile csv_file(filename, "w");
  CSVLine csv_line;

  // Intialize a progress bar
  ProgressBar bar(cell_num);

  // Initialization of generators
  std::default_random_engine generator(seed);

  for(int64_t i=0; i<cell_num; ++i) {
    bar.load(1);
    generate_uniform_coordinates(generator, csv_line);
    generate_uniform_attributes(generator, csv_line);
    csv_file << csv_line;
    csv_line.clear();
  }

  // Clean up
  csv_file.close();

  return 0;
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
      if(array_schema_->val_num(i) == VAR_SIZE) {
        std::stringstream s;
        std::uniform_int_distribution<char> distribution(45,126);
        for(int i=0; i<val_num; ++i) 
          s << distribution(generator); 
        csv_line << s.str();
      } else {
        std::uniform_int_distribution<char> distribution(45,126);
        for(int i=0; i<val_num; ++i) 
          csv_line << distribution(generator);
      }
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

size_t DataGenerator::generate_uniform_attributes(
    std::default_random_engine& generator, char* buffer) const {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t offset = 0;

  for(int i=0; i<attribute_num; ++i) {
    const std::type_info* attr_type = array_schema_->type(i);
    int val_num = array_schema_->val_num(i);

    // Determine random number of values for variable-sized cells 
    if(val_num == VAR_SIZE) {
      std::uniform_int_distribution<int> distribution(1,10);
      val_num = distribution(generator);
      if(attr_type != &typeid(char))
        memcpy(buffer + offset, &val_num, sizeof(int));
        offset += sizeof(int);
    }

    // Generate value(s) for each attribute
    if(attr_type == &typeid(char)) {
      if(array_schema_->val_num(i) == VAR_SIZE) {
        std::stringstream s;
        std::uniform_int_distribution<char> distribution(45,126);
        for(int i=0; i<val_num; ++i) 
          s << distribution(generator); 
        memcpy(buffer + offset, s.str().c_str(), sizeof(s.str().size()));
        offset += s.str().size();
      } else {
        std::uniform_int_distribution<char> distribution(45,126);
        for(int i=0; i<val_num; ++i) { 
          char c = distribution(generator);
          memcpy(buffer + offset, &c, sizeof(char));
          offset += sizeof(char);
        }
      }
    } else if(attr_type == &typeid(int)) {
      std::uniform_int_distribution<int> distribution;
      for(int i=0; i<val_num; ++i) { 
        int c = distribution(generator);
        memcpy(buffer + offset, &c, sizeof(int));
        offset += sizeof(int);
      }
    } else if(attr_type == &typeid(int64_t)) {
      std::uniform_int_distribution<int64_t> distribution;
      for(int i=0; i<val_num; ++i) { 
        int64_t c = distribution(generator);
        memcpy(buffer + offset, &c, sizeof(int64_t));
        offset += sizeof(int64_t);
      }
    } else if(attr_type == &typeid(float)) {
      std::uniform_real_distribution<float> distribution;
      for(int i=0; i<val_num; ++i) { 
        float c = distribution(generator);
        memcpy(buffer + offset, &c, sizeof(float));
        offset += sizeof(float);
      }
    } else if(attr_type == &typeid(double)) {
      std::uniform_real_distribution<double> distribution;
      for(int i=0; i<val_num; ++i) { 
        double c = distribution(generator);
        memcpy(buffer + offset, &c, sizeof(double));
        offset += sizeof(double);
      }
    }
  }

  return offset;
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

void DataGenerator::generate_uniform_coordinates(
    std::default_random_engine& generator, char* buffer) const {
  // For easy referemce
  int dim_num = array_schema_->dim_num();
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  const ArraySchema::DimDomains& dim_domains = array_schema_->dim_domains();
  size_t offset = 0;

  if(coords_type == &typeid(int)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_int_distribution<int> distribution(
          int(dim_domains[i].first), int(dim_domains[i].second));
      int c = distribution(generator);
      memcpy(buffer + offset, &c, sizeof(int));
      offset += sizeof(int);
    }
  } else if(coords_type == &typeid(int64_t)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_int_distribution<int64_t> distribution(
          int64_t(dim_domains[i].first), int64_t(dim_domains[i].second));
      int64_t c = distribution(generator);

      memcpy(buffer + offset, &c, sizeof(int64_t));
      offset += sizeof(int64_t);
    }
  } else if(coords_type == &typeid(float)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_real_distribution<float> distribution(
          float(dim_domains[i].first), float(dim_domains[i].second));
      float c = distribution(generator);
      memcpy(buffer + offset, &c, sizeof(float));
      offset += sizeof(float);
    }
  } else if(coords_type == &typeid(double)) {
    for(int i=0; i<dim_num; ++i) {
      std::uniform_real_distribution<double> distribution(
          double(dim_domains[i].first), double(dim_domains[i].second));
      double c = distribution(generator);
      memcpy(buffer + offset, &c, sizeof(double));
      offset += sizeof(double);
    }
  }
}

