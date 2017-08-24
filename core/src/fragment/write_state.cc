/**
 * @file   write_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the WriteState class.
 */

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>

#include "../../include/vfs/filesystem.h"
#include "comparators.h"
#include "const_buffer.h"
#include "logger.h"
#include "tile.h"
#include "utils.h"
#include "write_state.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriteState::WriteState(const Fragment* fragment, FragmentMetadata* bookkeeping)
    : metadata_(bookkeeping)
    , fragment_(fragment) {
  // For easy reference
  const ArraySchema* array_schema = fragment->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t coords_size = array_schema->coords_size();
  const Config* config = fragment_->array()->config();

  // Initialize the number of cells written in the current tile
  tile_cell_num_.resize(attribute_num + 1);
  for (int i = 0; i < attribute_num + 1; ++i)
    tile_cell_num_[i] = 0;

  // Initialize tiles and tile_io
  for (int i = 0; i < attribute_num; ++i) {
    const Attribute* attr = array_schema->Attributes()[i];
    bool var_size = array_schema->var_size(i);
    uint64_t cell_size =
        (var_size) ? array_schema->type_size(i) : array_schema->cell_size(i);
    tile_io_.emplace_back(new TileIO(config, fragment_->attr_uri(i)));
    tiles_.emplace_back(new Tile(
        attr->type(),
        attr->compressor(),
        attr->compression_level(),
        fragment_->tile_size(i),
        attr->cell_size(),
        var_size));
    if (var_size) {
      tiles_var_.emplace_back(new Tile(
          attr->type(),
          attr->compressor(),
          attr->compression_level(),
          fragment_->tile_size(i),
          cell_size));
      tile_io_var_.emplace_back(new TileIO(config, fragment_->attr_var_uri(i)));
    } else {
      tiles_var_.emplace_back(nullptr);
      tile_io_var_.emplace_back(nullptr);
    }
  }
  const Dimension* dim = array_schema->Dimensions()[0];
  tiles_.emplace_back(new Tile(
      dim->type(),
      dim->compressor(),
      dim->compression_level(),
      fragment_->tile_size(array_schema->attribute_num()),
      array_schema->coords_size()));
  tile_io_.emplace_back(new TileIO(config, fragment_->coords_uri()));

  // Initialize the current size of the variable attribute file
  buffer_var_offsets_.resize(attribute_num);
  for (int i = 0; i < attribute_num; ++i)
    buffer_var_offsets_[i] = 0;

  // Initialize current MBR
  mbr_ = malloc(2 * coords_size);

  // Initialize current bounding coordinates
  bounding_coords_ = malloc(2 * coords_size);
}

WriteState::~WriteState() {
  for (auto& tile : tiles_)
    delete tile;

  for (auto& tile_var : tiles_var_)
    delete tile_var;

  for (auto& tile_io : tile_io_)
    delete tile_io;

  for (auto& tile_io_var : tile_io_var_)
    delete tile_io_var;

  // Free current MBR
  if (mbr_ != nullptr)
    free(mbr_);

  // Free current bounding coordinates
  if (bounding_coords_ != nullptr)
    free(bounding_coords_);
}

/* ****************************** */
/*           MUTATORS             */
/* ****************************** */

Status WriteState::finalize() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Write last tile (applicable only to the sparse case)
  if (!tiles_[attribute_num]->empty())
    RETURN_NOT_OK(write_last_tile());

  // Sync all attributes
  RETURN_NOT_OK(sync());

  // Success
  return Status::Ok();
}

Status WriteState::sync() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::vector<int>& attribute_ids =
      fragment_->array()->query_->attribute_ids();
  IOMethod write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif
  std::string filename;

  // Sync all attributes
  for (int attribute_id : attribute_ids) {
    // For all attributes
    if (attribute_id == attribute_num)
      filename = fragment_->coords_uri().to_posix_path();
    else
      filename = fragment_->attr_uri(attribute_id).to_posix_path();
    if (write_method == IOMethod::WRITE) {
      RETURN_NOT_OK(vfs::sync(filename.c_str()));
    } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
      RETURN_NOT_OK(vfs::mpi_io_sync(mpi_comm, filename.c_str()));
#else
      // Error: MPI not supported
      return LOG_STATUS(
          Status::WriteStateError("Cannot sync; MPI not supported"));
#endif
    } else {
      assert(0);
    }

    // Only for variable-size attributes (they have an extra file)
    if (array_schema->var_size(attribute_id)) {
      filename = fragment_->attr_var_uri(attribute_id).to_posix_path();
      if (write_method == IOMethod::WRITE) {
        RETURN_NOT_OK(vfs::sync(filename.c_str()));
      } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
        RETURN_NOT_OK(vfs::mpi_io_sync(mpi_comm, filename.c_str()));
#else
        // Error: MPI not supported
        return LOG_STATUS(
            Status::WriteStateError("Cannot sync; MPI not supported"));
#endif
      } else {
        assert(0);
      }
    }
  }

  // Sync fragment directory
  filename = fragment_->fragment_uri().to_posix_path();
  if (write_method == IOMethod::WRITE) {
    RETURN_NOT_OK(vfs::sync(filename.c_str()));
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    RETURN_NOT_OK(vfs::mpi_io_sync(mpi_comm, filename.c_str()));
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::WriteStateError("Cannot sync; MPI not supported"));
#endif
  } else {
    assert(0);
  }

  // Success
  return Status::Ok();
}

Status WriteState::sync_attribute(const std::string& attribute) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  IOMethod write_method = fragment_->array()->config()->write_method();
#ifdef HAVE_MPI
  MPI_Comm* mpi_comm = fragment_->array()->config()->mpi_comm();
#endif

  int attribute_id;
  RETURN_NOT_OK(array_schema->attribute_id(attribute, &attribute_id));
  std::string filename;
  if (attribute_id == attribute_num)
    filename = fragment_->coords_uri().to_posix_path();
  else
    filename = fragment_->attr_uri(attribute_id).to_posix_path();
  if (write_method == IOMethod::WRITE) {
    RETURN_NOT_OK(vfs::sync(filename.c_str()));
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    RETURN_NOT_OK(vfs::mpi_io_sync(mpi_comm, filename.c_str()));
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::WriteStateError("Cannot sync attribute; MPI not supported"));
#endif
  } else {
    assert(0);
  }
  // Only for variable-size attributes (they have an extra file)
  if (array_schema->var_size(attribute_id)) {
    filename = fragment_->attr_var_uri(attribute_id).to_posix_path();
    if (write_method == IOMethod::WRITE) {
      RETURN_NOT_OK(vfs::sync(filename.c_str()));
    } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
      RETURN_NOT_OK(vfs::mpi_io_sync(mpi_comm, filename.c_str()));
#else
      // Error: MPI not supported
      return LOG_STATUS(
          Status::WriteStateError("Cannot sync attribute; MPI not supported"));
#endif
    } else {
      assert(0);
    }
  }
  // Sync fragment directory
  filename = fragment_->fragment_uri().to_posix_path();
  if (write_method == IOMethod::WRITE) {
    RETURN_NOT_OK(vfs::sync(filename.c_str()));
  } else if (write_method == IOMethod::MPI) {
#ifdef HAVE_MPI
    RETURN_NOT_OK(vfs::mpi_io_sync(mpi_comm, filename.c_str()));
#else
    // Error: MPI not supported
    return LOG_STATUS(
        Status::WriteStateError("Cannot sync attribute; MPI not supported"));
#endif
  } else {
    assert(0);
  }
  // Success
  return Status::Ok();
}

Status WriteState::write(const void** buffers, const size_t* buffer_sizes) {
  // Create fragment directory if it does not exist
  std::string fragment_name = fragment_->fragment_uri().to_posix_path();
  if (!vfs::is_dir(fragment_name))
    RETURN_NOT_OK(vfs::create_dir(fragment_name));

  QueryMode mode = fragment_->array()->query_->mode();

  // Dispatch the proper write command
  if (mode == QueryMode::WRITE || mode == QueryMode::WRITE_SORTED_COL ||
      mode == QueryMode::WRITE_SORTED_ROW) {  // SORTED
    // For easy reference
    const ArraySchema* array_schema = fragment_->array()->array_schema();
    const std::vector<int>& attribute_ids =
        fragment_->array()->query_->attribute_ids();
    int attribute_id_num = attribute_ids.size();

    // Write each attribute individually
    int buffer_i = 0;
    for (int i = 0; i < attribute_id_num; ++i) {
      if (!array_schema->var_size(attribute_ids[i])) {  // FIXED CELLS
        RETURN_NOT_OK(write_attr(
            attribute_ids[i], buffers[buffer_i], buffer_sizes[buffer_i]));
        ++buffer_i;
      } else {  // VARIABLE-SIZED CELLS
        RETURN_NOT_OK(write_attr_var(
            attribute_ids[i],
            buffers[buffer_i],  // offsets
            buffer_sizes[buffer_i],
            buffers[buffer_i + 1],  // actual cell values
            buffer_sizes[buffer_i + 1]));
        buffer_i += 2;
      }
    }

    // Success
    return Status::Ok();
  } else if (mode == QueryMode::WRITE_UNSORTED) {  // UNSORTED
    return write_sparse_unsorted(buffers, buffer_sizes);
  } else {
    return LOG_STATUS(
        Status::WriteStateError("Cannot write to fragment; Invalid mode"));
  }
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
void WriteState::expand_mbr(const T* coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  T* mbr = static_cast<T*>(mbr_);

  // Initialize MBR
  if (tile_cell_num_[attribute_num] == 0) {
    for (int i = 0; i < dim_num; ++i) {
      mbr[2 * i] = coords[i];
      mbr[2 * i + 1] = coords[i];
    }
  } else {  // Expand MBR
    utils::expand_mbr(mbr, coords, dim_num);
  }
}

void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Datatype coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    sort_cell_pos<int>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::INT64)
    sort_cell_pos<int64_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::FLOAT32)
    sort_cell_pos<float>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::FLOAT64)
    sort_cell_pos<double>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::INT8)
    sort_cell_pos<int8_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT8)
    sort_cell_pos<uint8_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::INT16)
    sort_cell_pos<int16_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT16)
    sort_cell_pos<uint16_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT32)
    sort_cell_pos<uint32_t>(buffer, buffer_size, cell_pos);
  else if (coords_type == Datatype::UINT64)
    sort_cell_pos<uint64_t>(buffer, buffer_size, cell_pos);
}

template <class T>
void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  Layout cell_order = array_schema->cell_order();
  auto buffer_T = static_cast<const T*>(buffer);

  // Populate cell_pos
  cell_pos.resize(buffer_cell_num);
  for (int i = 0; i < buffer_cell_num; ++i)
    cell_pos[i] = i;

  // Invoke the proper sort function, based on the cell order
  if (array_schema->tile_extents() == nullptr) {  // NO TILE GRID
    // Sort cell positions
    switch (cell_order) {
      case Layout::ROW_MAJOR:
        std::sort(
            cell_pos.begin(), cell_pos.end(), SmallerRow<T>(buffer_T, dim_num));
        break;
      case Layout::COL_MAJOR:
        std::sort(
            cell_pos.begin(), cell_pos.end(), SmallerCol<T>(buffer_T, dim_num));
        break;
    }
  } else {  // TILE GRID
    // Get tile ids
    std::vector<int64_t> ids;
    ids.resize(buffer_cell_num);
    for (int i = 0; i < buffer_cell_num; ++i)
      ids[i] = array_schema->tile_id<T>(&buffer_T[i * dim_num]);
    // Sort cell positions
    switch (cell_order) {
      case Layout::ROW_MAJOR:
        std::sort(
            cell_pos.begin(),
            cell_pos.end(),
            SmallerIdRow<T>(buffer_T, dim_num, ids));
        break;
      case Layout::COL_MAJOR:
        std::sort(
            cell_pos.begin(),
            cell_pos.end(),
            SmallerIdCol<T>(buffer_T, dim_num, ids));
        break;
    }
  }
}

void WriteState::update_bookkeeping(const void* buffer, size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  Datatype coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    update_bookkeeping<int>(buffer, buffer_size);
  else if (coords_type == Datatype::INT64)
    update_bookkeeping<int64_t>(buffer, buffer_size);
  else if (coords_type == Datatype::FLOAT32)
    update_bookkeeping<float>(buffer, buffer_size);
  else if (coords_type == Datatype::FLOAT64)
    update_bookkeeping<double>(buffer, buffer_size);
  else if (coords_type == Datatype::INT8)
    update_bookkeeping<int8_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT8)
    update_bookkeeping<uint8_t>(buffer, buffer_size);
  else if (coords_type == Datatype::INT16)
    update_bookkeeping<int16_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT16)
    update_bookkeeping<uint16_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT32)
    update_bookkeeping<uint32_t>(buffer, buffer_size);
  else if (coords_type == Datatype::UINT64)
    update_bookkeeping<uint64_t>(buffer, buffer_size);
}

template <class T>
void WriteState::update_bookkeeping(const void* buffer, size_t buffer_size) {
  // Trivial case
  if (buffer_size == 0)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  int64_t capacity = array_schema->capacity();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  auto buffer_T = static_cast<const T*>(buffer);
  int64_t& tile_cell_num = tile_cell_num_[attribute_num];

  // Update bounding coordinates and MBRs
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Set first bounding coordinates
    if (tile_cell_num == 0)
      memcpy(bounding_coords_, &buffer_T[i * dim_num], coords_size);

    // Expand MBR
    expand_mbr(&buffer_T[i * dim_num]);

    // Advance a cell
    ++tile_cell_num;

    // Set second bounding coordinates
    if (i == buffer_cell_num - 1 || tile_cell_num == capacity)
      memcpy(
          static_cast<char*>(bounding_coords_) + coords_size,
          &buffer_T[i * dim_num],
          coords_size);

    // Send MBR and bounding coordinates to metadata
    if (tile_cell_num == capacity) {
      metadata_->append_mbr(mbr_);
      metadata_->append_bounding_coords(bounding_coords_);
      tile_cell_num = 0;
    }
  }
}

Status WriteState::write_attr(
    int attribute_id, const void* buffer, size_t buffer_size) {
  // Trivial case
  if (buffer_size == 0)
    return Status::Ok();

  // Update metadata in the case of sparse fragment coordinates
  if (attribute_id == fragment_->array()->array_schema()->attribute_num())
    update_bookkeeping(buffer, buffer_size);

  // Preparation
  auto buf = new ConstBuffer(buffer, buffer_size);

  Tile* tile = tiles_[attribute_id];
  TileIO* tile_io = tile_io_[attribute_id];

  // Fill tiles and dispatch them for writing
  uint64_t bytes_written;
  do {
    RETURN_NOT_OK(tile->write(buf));
    if (tile->full()) {
      RETURN_NOT_OK(tile_io->write(tile, &bytes_written));
      metadata_->append_tile_offset(attribute_id, bytes_written);
      tile->reset_offset();
    }
  } while (!buf->end());

  // Clean up
  delete buf;

  return Status::Ok();
}

Status WriteState::write_attr_last(int attribute_id) {
  Tile* tile = tiles_[attribute_id];
  assert(!tile->empty());
  TileIO* tile_io = tile_io_[attribute_id];

  // Fill tiles and dispatch them for writing
  uint64_t bytes_written;
  RETURN_NOT_OK(tile_io->write(tile, &bytes_written));
  metadata_->append_tile_offset(attribute_id, bytes_written);
  tile->reset_offset();

  return Status::Ok();
}

Status WriteState::write_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size) {
  // Trivial case
  if (buffer_size == 0 || buffer_var_size == 0)
    return Status::Ok();

  auto buf = new ConstBuffer(buffer, buffer_size);
  auto buf_var = new ConstBuffer(buffer_var, buffer_var_size);

  size_t& buffer_var_offset = buffer_var_offsets_[attribute_id];

  Tile* tile = tiles_[attribute_id];
  Tile* tile_var = tiles_var_[attribute_id];
  TileIO* tile_io = tile_io_[attribute_id];
  TileIO* tile_io_var = tile_io_var_[attribute_id];

  // Fill tiles and dispatch them for writing
  uint64_t bytes_written, bytes_written_var, bytes_to_write_var;
  do {
    RETURN_NOT_OK(tile->write_with_shift(buf, buffer_var_offset));

    bytes_to_write_var =
        (buf->end()) ?
            buffer_var_offset + buffer_var_size - tile->value<uint64_t>(0) :
            buffer_var_offset + buf->value<uint64_t>() -
                tile->value<uint64_t>(0);

    RETURN_NOT_OK(tile_var->write(buf_var, bytes_to_write_var));

    if (tile->full()) {
      RETURN_NOT_OK(tile_io->write(tile, &bytes_written));
      RETURN_NOT_OK(tile_io_var->write(tile_var, &bytes_written_var));
      metadata_->append_tile_offset(attribute_id, bytes_written);
      metadata_->append_tile_var_offset(attribute_id, bytes_written_var);
      metadata_->append_tile_var_size(attribute_id, tile_var->offset());
      tile->reset_offset();
      tile_var->reset_offset();
    }
  } while (!buf->end());

  buffer_var_offset += buffer_var_size;

  // Clean up
  delete buf;
  delete buf_var;

  return Status::Ok();
}

Status WriteState::write_attr_var_last(int attribute_id) {
  Tile* tile = tiles_[attribute_id];
  Tile* tile_var = tiles_var_[attribute_id];
  TileIO* tile_io = tile_io_[attribute_id];
  TileIO* tile_io_var = tile_io_var_[attribute_id];

  // Fill tiles and dispatch them for writing
  uint64_t bytes_written, bytes_written_var;
  RETURN_NOT_OK(tile_io->write(tile, &bytes_written));
  RETURN_NOT_OK(tile_io_var->write(tile_var, &bytes_written_var));
  metadata_->append_tile_offset(attribute_id, bytes_written);
  metadata_->append_tile_var_offset(attribute_id, bytes_written_var);
  metadata_->append_tile_var_size(attribute_id, tile_var->offset());
  tile->reset_offset();
  tile_var->reset_offset();

  return Status::Ok();
}

Status WriteState::write_last_tile() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Send last MBR, bounding coordinates and tile cell number to metadata
  metadata_->append_mbr(mbr_);
  metadata_->append_bounding_coords(bounding_coords_);
  metadata_->set_last_tile_cell_num(tile_cell_num_[attribute_num]);

  // Flush the last tile for each compressed attribute (it is still in main
  // memory
  for (int i = 0; i < attribute_num + 1; ++i) {
    RETURN_NOT_OK(write_attr_last(i));
    if (array_schema->var_size(i))
      RETURN_NOT_OK(write_attr_var_last(i));
  }

  // Success
  return Status::Ok();
}

Status WriteState::write_sparse_unsorted(
    const void** buffers, const size_t* buffer_sizes) {
  // For easy reference
  const Array* array = fragment_->array();
  const ArraySchema* array_schema = array->array_schema();
  const std::vector<int>& attribute_ids = array->query_->attribute_ids();
  int attribute_id_num = (int)attribute_ids.size();

  // Find the coordinates buffer
  int coords_buffer_i = -1;
  RETURN_NOT_OK(array->query_->coords_buffer_i(&coords_buffer_i));

  // Sort cell positions
  std::vector<int64_t> cell_pos;
  sort_cell_pos(
      buffers[coords_buffer_i], buffer_sizes[coords_buffer_i], cell_pos);

  // Write each attribute individually
  int buffer_i = 0;
  for (int i = 0; i < attribute_id_num; ++i) {
    if (!array_schema->var_size(attribute_ids[i])) {  // FIXED CELLS
      RETURN_NOT_OK(write_sparse_unsorted_attr(
          attribute_ids[i],
          buffers[buffer_i],
          buffer_sizes[buffer_i],
          cell_pos));
      ++buffer_i;
    } else {  // VARIABLE-SIZED CELLS
      RETURN_NOT_OK(write_sparse_unsorted_attr_var(
          attribute_ids[i],
          buffers[buffer_i],  // offsets
          buffer_sizes[buffer_i],
          buffers[buffer_i + 1],  // actual values
          buffer_sizes[buffer_i + 1],
          cell_pos));
      buffer_i += 2;
    }
  }

  // Success
  return Status::Ok();
}

Status WriteState::write_sparse_unsorted_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if (buffer_cell_num != int64_t(cell_pos.size())) {
    return LOG_STATUS(Status::WriteStateError(
        std::string("Cannot write sparse unsorted; Invalid number of "
                    "cells in attribute '") +
        array_schema->attribute(attribute_id) + "'"));
  }

  // Allocate a local buffer to hold the sorted cells
  auto sorted_buf = new Buffer(constants::sorted_buffer_size);
  auto buffer_c = static_cast<const char*>(buffer);

  // Sort and write attribute values in batches
  Status st;
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Write batch
    if (sorted_buf->offset() + cell_size > constants::sorted_buffer_size) {
      RETURN_NOT_OK_ELSE(
          write_attr(attribute_id, sorted_buf->data(), sorted_buf->offset()),
          delete sorted_buf);
      sorted_buf->reset_offset();
    }

    // Keep on copying the cells in the sorted order in the sorted buffer
    RETURN_NOT_OK(
        sorted_buf->write(buffer_c + cell_pos[i] * cell_size, cell_size));
  }

  // Write final batch
  if (sorted_buf->offset() != 0) {
    RETURN_NOT_OK_ELSE(
        write_attr(attribute_id, sorted_buf->data(), sorted_buf->offset()),
        delete sorted_buf);
  }

  // Clean up
  delete sorted_buf;

  return Status::Ok();
}

Status WriteState::write_sparse_unsorted_attr_var(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const void* buffer_var,
    size_t buffer_var_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = constants::cell_var_offset_size;
  size_t cell_var_size;
  auto buffer_s = static_cast<const size_t*>(buffer);
  auto buffer_var_c = static_cast<const char*>(buffer_var);

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if (buffer_cell_num != int64_t(cell_pos.size())) {
    return LOG_STATUS(Status::WriteStateError(
        std::string("Cannot write sparse unsorted variable; "
                    "Invalid number of cells in attribute '") +
        array_schema->attribute(attribute_id) + "'"));
  }

  auto sorted_buf = new Buffer(constants::sorted_buffer_size);
  auto sorted_buf_var = new Buffer(constants::sorted_buffer_var_size);

  // Sort and write attribute values in batches
  Status st;
  uint64_t var_offset;
  for (int64_t i = 0; i < buffer_cell_num; ++i) {
    // Calculate variable cell size
    cell_var_size = (cell_pos[i] == buffer_cell_num - 1) ?
                        buffer_var_size - buffer_s[cell_pos[i]] :
                        buffer_s[cell_pos[i] + 1] - buffer_s[cell_pos[i]];

    // Write batch
    if (sorted_buf->offset() + cell_size > constants::sorted_buffer_size ||
        sorted_buf_var->offset() + cell_var_size >
            constants::sorted_buffer_var_size) {
      st = write_attr_var(
          attribute_id,
          sorted_buf->data(),
          sorted_buf->offset(),
          sorted_buf_var->data(),
          sorted_buf_var->offset());

      if (!st.ok()) {
        delete sorted_buf;
        delete sorted_buf_var;
        return st;
      }

      sorted_buf->reset_offset();
      sorted_buf_var->reset_offset();
    }

    // Keep on copying the cells in sorted order in the sorted buffers
    var_offset = sorted_buf_var->offset();
    RETURN_NOT_OK(sorted_buf->write(&var_offset, cell_size));
    RETURN_NOT_OK(sorted_buf_var->write(
        buffer_var_c + buffer_s[cell_pos[i]], cell_var_size));
  }

  // Write final batch
  if (sorted_buf->offset() != 0)
    st = write_attr_var(
        attribute_id,
        sorted_buf->data(),
        sorted_buf->offset(),
        sorted_buf_var->data(),
        sorted_buf_var->offset());

  // Clean up
  delete sorted_buf;
  delete sorted_buf_var;

  // Success
  return st;
}

}  // namespace tiledb
