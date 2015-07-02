/**
 * @file   data_generator.h
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
 * This file defines class DataGenerator. 
 */

#ifndef DATA_GENERATOR_H
#define DATA_GENERATOR_H

#include "array_schema.h"
#include "csv_line.h"
#include <random>
#include <string>

/** Enables creating synthetic datasets for a particular array schema. */
class DataGenerator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  DataGenerator(const ArraySchema* array_schema);
  /** Destructor. */
  ~DataGenerator();

  // DATA GENERATION FUNCTIONS
  /**
   * Generates a uniform binary file for the stored array schema. Takes as input
   * a seed used to generate the randomness, the name of the file to store the
   * the generated cells, and the number of generated cells.
   */
  int generate_bin(
      unsigned seed, 
      const std::string& filename, 
      int64_t cell_num) const;
   /**
   * Generates a uniform CSV file for the stored array schema. Takes as input
   * a seed used to generate the randomness, the name of the file to store the
   * the generated cells, and the number of generated cells.
   */
  int generate_csv(
      unsigned seed, 
      const std::string& filename, 
      int64_t cell_num) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The array schema. */
  const ArraySchema* array_schema_;

  // PRIVATE METHODS
  /** Generates and appends uniformly drawn attribute values to the CSV line. */
  void generate_uniform_attributes(
      std::default_random_engine& generator, CSVLine& csv_line) const;
  /** 
   * Generates and stores uniformly drawn attribute values to buffer. 
   * Returns the total size in bytes written to buffer.
   */
  size_t generate_uniform_attributes(
      std::default_random_engine& generator, char* buffer) const;
  /** Generates and appends uniformly drawn coordinates to the CSV line. */
  void generate_uniform_coordinates(
      std::default_random_engine& generator, CSVLine& csv_line) const;
  /** Generates and stores uniformly drawn coordinates to buffer. */
  void generate_uniform_coordinates(
      std::default_random_engine& generator, char* buffer) const;
};

#endif
