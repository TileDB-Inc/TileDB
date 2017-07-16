/**
 * @file   configurator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file implements the Configurator class.
 */

#include "configurator.h"

namespace tiledb {

/* ****************************** */
/*           CONSTANTS            */
/* ****************************** */

const uint64_t Configurator::CELL_VAR_OFFSET_SIZE(sizeof(size_t));
const uint64_t Configurator::CONSOLIDATION_BUFFER_SIZE(10000000);
const unsigned Configurator::NAME_MAX_LEN(256);
const char* Configurator::FILE_SUFFIX(".tdb");
const char* Configurator::GZIP_SUFFIX(".gz");
const uint64_t Configurator::INTERNAL_BUFFER_SIZE(10000000);
const char* Configurator::BOOKKEEPING_FILENAME("__book_keeping");
const char* Configurator::KEY_DIM1_NAME("__key_dim_1");
const char* Configurator::KEY_DIM2_NAME("__key_dim_2");
const char* Configurator::KEY_DIM3_NAME("__key_dim_3");
const char* Configurator::KEY_DIM4_NAME("__key_dim_4");
const char* Configurator::ARRAY_SCHEMA_FILENAME("__array_schema.tdb");
const char* Configurator::METADATA_SCHEMA_FILENAME("__metadata_schema.tdb");
const char* Configurator::CONSOLIDATION_FILELOCK_NAME(".__consolidation_lock");
const char* Configurator::BOOK_KEEPING_FILENAME("__book_keeping");
const char* Configurator::FRAGMENT_FILENAME("__tiledb_fragment.tdb");
const char* Configurator::GROUP_FILENAME("__tiledb_group.tdb");
const uint64_t Configurator::CAPACITY(10000);
const uint64_t Configurator::SORTED_BUFFER_SIZE(10000000);
const uint64_t Configurator::SORTED_BUFFER_VAR_SIZE(10000000);
const int Configurator::EMPTY_INT32 = INT_MAX;
const int64_t Configurator::EMPTY_INT64 = INT64_MAX;
const float Configurator::EMPTY_FLOAT32 = FLT_MAX;
const double Configurator::EMPTY_FLOAT64 = DBL_MAX;
const char Configurator::EMPTY_CHAR = CHAR_MAX;
const int8_t Configurator::EMPTY_INT8 = INT8_MAX;
const uint8_t Configurator::EMPTY_UINT8 = UINT8_MAX;
const int16_t Configurator::EMPTY_INT16 = INT16_MAX;
const uint16_t Configurator::EMPTY_UINT16 = UINT16_MAX;
const uint32_t Configurator::EMPTY_UINT32 = UINT32_MAX;
const uint64_t Configurator::EMPTY_UINT64 = UINT64_MAX;
const uint64_t Configurator::MAX_WRITE_BYTES(1500000000);
const unsigned int Configurator::VAR_NUM = UINT_MAX;
const uint64_t Configurator::VAR_SIZE = (size_t)-1;
const char* Configurator::COORDS = "__coords";
const char* Configurator::KEY = "__key";
const char* Configurator::NO_COMPRESSION_STR("NO_COMPRESSION");
const char* Configurator::GZIP_STR("GZIP");
const char* Configurator::ZSTD_STR("ZSTD");
const char* Configurator::LZ4_STR("LZ4");
const char* Configurator::BLOSC_STR("BLOSC");
const char* Configurator::BLOSC_LZ4_STR("BLOSC_LZ4");
const char* Configurator::BLOSC_LZ4HC_STR("BLOSC_LZ4HC");
const char* Configurator::BLOSC_SNAPPY_STR("BLOSC_SNAPPY");
const char* Configurator::BLOSC_ZLIB_STR("BLOSC_ZLIB");
const char* Configurator::BLOSC_ZSTD_STR("BLOSC_ZSTD");
const char* Configurator::RLE_STR("RLE");
const char* Configurator::BZIP2_STR("BZIP2");
const char* Configurator::INT32_STR("INT32");
const char* Configurator::INT64_STR("INT64");
const char* Configurator::FLOAT32_STR("FLOAT32");
const char* Configurator::FLOAT64_STR("FLOAT64");
const char* Configurator::CHAR_STR("CHAR");
const char* Configurator::INT8_STR("INT8");
const char* Configurator::UINT8_STR("UINT8");
const char* Configurator::INT16_STR("INT16");
const char* Configurator::UINT16_STR("UINT16");
const char* Configurator::UINT32_STR("UINT32");
const char* Configurator::UINT64_STR("UINT64");
const char* Configurator::DENSE_STR("dense");
const char* Configurator::SPARSE_STR("sparse");
const char* Configurator::COL_MAJOR_STR("col-major");
const char* Configurator::ROW_MAJOR_STR("row-major");
const char* Configurator::NULL_STR("null");

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Configurator::Configurator() {
  // Default values
  read_method_ = IOMethod::MMAP;
  write_method_ = IOMethod::WRITE;
#ifdef HAVE_MPI
  mpi_comm_ = NULL;
#endif
}

Configurator::Configurator(Configurator* config) {
  if (config == nullptr) {  // Default
#ifdef HAVE_MPI
    mpi_comm_ = NULL;
#endif
    read_method_ = IOMethod::MMAP;
    write_method_ = IOMethod::WRITE;
  } else {  // Clone
#ifdef HAVE_MPI
    mpi_comm_ = config->mpi_comm();
#endif
    read_method_ = config->read_method();
    write_method_ = config->write_method();
  }
}

Configurator::~Configurator() = default;

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

void Configurator::init(
#ifdef HAVE_MPI
    MPI_Comm* mpi_comm,
#endif
    IOMethod read_method,
    IOMethod write_method) {
#ifdef HAVE_MPI
  // Initialize MPI communicator
  mpi_comm_ = mpi_comm;
#endif

  // Initialize read method
  read_method_ = read_method;
  if (read_method_ != IOMethod::READ && read_method_ != IOMethod::MMAP &&
      read_method_ != IOMethod::MPI)
    read_method_ = IOMethod::MMAP;  // Use default

  // Initialize write method
  write_method_ = write_method;
  if (write_method_ != IOMethod::WRITE && write_method_ != IOMethod::MPI)
    write_method_ = IOMethod::WRITE;  // Use default
}

#ifdef HAVE_MPI
void Configurator::set_mpi_comm(MPI_Comm* mpi_comm) {
  mpi_comm_ = mpi_comm;
}
#endif

void Configurator::set_read_method(IOMethod read_method) {
  read_method_ = read_method;
}

void Configurator::set_write_method(IOMethod write_method) {
  write_method_ = write_method;
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const char* Configurator::array_schema_filename() {
  return ARRAY_SCHEMA_FILENAME;
}

uint64_t Configurator::capacity() {
  return CAPACITY;
}

const char* Configurator::bookkeeping_filename() {
  return BOOKKEEPING_FILENAME;
}

uint64_t Configurator::cell_var_offset_size() {
  return CELL_VAR_OFFSET_SIZE;
}

const char* Configurator::coords() {
  return COORDS;
}

uint64_t Configurator::consolidation_buffer_size() {
  return CONSOLIDATION_BUFFER_SIZE;
}

const char* Configurator::consolidation_filelock_name() {
  return CONSOLIDATION_FILELOCK_NAME;
}

int Configurator::empty_int32() {
  return EMPTY_INT32;
}

int64_t Configurator::empty_int64() {
  return EMPTY_INT64;
}

float Configurator::empty_float32() {
  return EMPTY_FLOAT32;
}

double Configurator::empty_float64() {
  return EMPTY_FLOAT64;
}

char Configurator::empty_char() {
  return EMPTY_CHAR;
}

int8_t Configurator::empty_int8() {
  return EMPTY_INT8;
}

uint8_t Configurator::empty_uint8() {
  return EMPTY_UINT8;
}

int16_t Configurator::empty_int16() {
  return EMPTY_INT16;
}

uint16_t Configurator::empty_uint16() {
  return EMPTY_UINT16;
}

uint32_t Configurator::empty_uint32() {
  return EMPTY_UINT32;
}

uint64_t Configurator::empty_uint64() {
  return EMPTY_UINT64;
}

const char* Configurator::file_suffix() {
  return FILE_SUFFIX;
}

const char* Configurator::fragment_filename() {
  return FRAGMENT_FILENAME;
}

const char* Configurator::group_filename() {
  return GROUP_FILENAME;
}

const char* Configurator::gzip_suffix() {
  return GZIP_SUFFIX;
}

uint64_t Configurator::internal_buffer_size() {
  return INTERNAL_BUFFER_SIZE;
}

const char* Configurator::key() {
  return KEY;
}

const char* Configurator::key_dim1_name() {
  return KEY_DIM1_NAME;
}

const char* Configurator::key_dim2_name() {
  return KEY_DIM2_NAME;
}

const char* Configurator::key_dim3_name() {
  return KEY_DIM3_NAME;
}

const char* Configurator::key_dim4_name() {
  return KEY_DIM4_NAME;
}

uint64_t Configurator::max_write_bytes() {
  return MAX_WRITE_BYTES;
}

const char* Configurator::metadata_schema_filename() {
  return METADATA_SCHEMA_FILENAME;
}

#ifdef HAVE_MPI
MPI_Comm* Configurator::mpi_comm() const {
  return mpi_comm_;
}
#endif

unsigned Configurator::name_max_len() {
  return NAME_MAX_LEN;
}

IOMethod Configurator::read_method() const {
  return read_method_;
}

uint64_t Configurator::sorted_buffer_size() {
  return SORTED_BUFFER_SIZE;
}

uint64_t Configurator::sorted_buffer_var_size() {
  return SORTED_BUFFER_VAR_SIZE;
}

int Configurator::var_num() {
  return VAR_NUM;
}

uint64_t Configurator::var_size() {
  return VAR_SIZE;
}

IOMethod Configurator::write_method() const {
  return write_method_;
}

const char* Configurator::no_compression_str() {
  return NO_COMPRESSION_STR;
}

const char* Configurator::gzip_str() {
  return GZIP_STR;
}

const char* Configurator::zstd_str() {
  return ZSTD_STR;
}

const char* Configurator::lz4_str() {
  return LZ4_STR;
}

const char* Configurator::blosc_str() {
  return BLOSC_STR;
}

const char* Configurator::blosc_lz4_str() {
  return BLOSC_LZ4_STR;
}

const char* Configurator::blosc_lz4hc_str() {
  return BLOSC_LZ4HC_STR;
}

const char* Configurator::blosc_snappy_str() {
  return BLOSC_SNAPPY_STR;
}

const char* Configurator::blosc_zlib_str() {
  return BLOSC_ZLIB_STR;
}

const char* Configurator::blosc_zstd_str() {
  return BLOSC_ZSTD_STR;
}

const char* Configurator::rle_str() {
  return RLE_STR;
}

const char* Configurator::bzip2_str() {
  return BZIP2_STR;
}

const char* Configurator::int32_str() {
  return INT32_STR;
}

const char* Configurator::int64_str() {
  return INT64_STR;
}

const char* Configurator::float32_str() {
  return FLOAT32_STR;
}

const char* Configurator::float64_str() {
  return FLOAT64_STR;
}

const char* Configurator::char_str() {
  return CHAR_STR;
}

const char* Configurator::int8_str() {
  return INT8_STR;
}

const char* Configurator::uint8_str() {
  return UINT8_STR;
}

const char* Configurator::int16_str() {
  return INT16_STR;
}

const char* Configurator::uint16_str() {
  return UINT16_STR;
}

const char* Configurator::uint32_str() {
  return UINT32_STR;
}

const char* Configurator::uint64_str() {
  return UINT64_STR;
}
const char* Configurator::dense_str() {
  return DENSE_STR;
}

const char* Configurator::sparse_str() {
  return SPARSE_STR;
}

const char* Configurator::col_major_str() {
  return COL_MAJOR_STR;
}

const char* Configurator::row_major_str() {
  return ROW_MAJOR_STR;
}

const char* Configurator::null_str() {
  return NULL_STR;
}

};  // namespace tiledb
