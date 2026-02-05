/**
 * @file   column_fragment_writer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB, Inc.
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
 * This file implements class ColumnFragmentWriter.
 */

#include "tiledb/sm/fragment/column_fragment_writer.h"

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

using namespace tiledb::common;

namespace tiledb::sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

ColumnFragmentWriter::ColumnFragmentWriter(
    ContextResources* resources,
    shared_ptr<const ArraySchema> array_schema,
    const URI& fragment_uri,
    const NDRange& non_empty_domain,
    uint64_t tile_count)
    : resources_(resources)
    , array_schema_(array_schema)
    , fragment_uri_(fragment_uri)
    , dense_(array_schema->dense())
    , current_tile_idx_(0)
    , tile_num_(0)
    , first_field_closed_(false)
    , mbrs_set_(false) {
  // For dense arrays, compute tile count from domain.
  // For sparse arrays, tile_count is a capacity hint (upper bound).
  // The actual count is determined dynamically by the first field written.
  if (dense_) {
    tile_num_ = array_schema->domain().tile_num(non_empty_domain);
  }

  // Create fragment directory structure
  create_fragment_directory();

  // Derive timestamp range from fragment URI
  FragmentID frag_id{fragment_uri};
  auto timestamp_range = frag_id.timestamp_range();

  // Create memory tracker from resources
  auto memory_tracker = resources->create_memory_tracker();

  // Create fragment metadata
  frag_meta_ = make_shared<FragmentMetadata>(
      HERE(),
      resources_,
      array_schema_,
      fragment_uri_,
      timestamp_range,
      memory_tracker,
      dense_,
      false,   // has_timestamps
      false);  // has_delete_meta

  // Initialize metadata with domain
  frag_meta_->init(non_empty_domain);

  if (dense_) {
    frag_meta_->set_num_tiles(tile_num_);
  } else if (tile_count > 0) {
    // Reserve capacity for the upper bound; actual count determined
    // dynamically.
    frag_meta_->reserve_num_tiles(tile_count);
  }
}

/* ********************************* */
/*          FIELD OPERATIONS         */
/* ********************************* */

void ColumnFragmentWriter::open_field(const std::string& name) {
  if (!current_field_.empty()) {
    throw ColumnFragmentWriterException(
        "Cannot open field '" + name + "': field '" + current_field_ +
        "' is already open");
  }

  if (!array_schema_->is_field(name)) {
    throw ColumnFragmentWriterException(
        "Field '" + name + "' does not exist in array schema");
  }

  current_field_ = name;
  current_tile_idx_ = 0;
}

void ColumnFragmentWriter::write_tile(WriterTileTuple& tile) {
  if (current_field_.empty()) {
    throw ColumnFragmentWriterException(
        "Cannot write tile: no field is currently open");
  }

  if (!tile.filtered_size().has_value()) {
    throw ColumnFragmentWriterException(
        "Cannot write tile: tile is not filtered");
  }

  // For sparse arrays with dynamic growth (tile_num_=0 initially, first field),
  // grow metadata. After first field closes, tile_num_ is fixed.
  if (!dense_ && !first_field_closed_ && current_tile_idx_ >= tile_num_) {
    tile_num_++;
    frag_meta_->set_num_tiles(tile_num_);
  } else if (current_tile_idx_ >= tile_num_) {
    throw ColumnFragmentWriterException(
        "Cannot write tile: tile count limit (" + std::to_string(tile_num_) +
        ") reached");
  }

  const std::string& name = current_field_;
  const bool var_size = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);

  const auto type = array_schema_->type(name);
  const auto is_dim = array_schema_->is_dim(name);
  const auto cell_val_num = array_schema_->cell_val_num(name);
  const bool has_min_max_md = TileMetadataGenerator::has_min_max_metadata(
      type, is_dim, var_size, cell_val_num);
  const bool has_sum_md =
      TileMetadataGenerator::has_sum_metadata(type, var_size, cell_val_num);

  // Get URIs for tile files
  URI uri = frag_meta_->uri(name);
  URI var_uri = var_size ? frag_meta_->var_uri(name) : URI("");
  URI validity_uri = nullable ? frag_meta_->validity_uri(name) : URI("");

  // Write fixed/offset tile
  auto& t = var_size ? tile.offset_tile() : tile.fixed_tile();
  resources_->vfs().write(
      uri, t.filtered_buffer().data(), t.filtered_buffer().size());
  frag_meta_->set_tile_offset(
      name, current_tile_idx_, t.filtered_buffer().size());

  auto null_count = tile.null_count();
  auto cell_num = tile.cell_num();

  // Write var tile if var-size
  if (var_size) {
    auto& t_var = tile.var_tile();
    resources_->vfs().write(
        var_uri,
        t_var.filtered_buffer().data(),
        t_var.filtered_buffer().size());
    frag_meta_->set_tile_var_offset(
        name, current_tile_idx_, t_var.filtered_buffer().size());
    frag_meta_->set_tile_var_size(
        name, current_tile_idx_, tile.var_pre_filtered_size());

    if (has_min_max_md && null_count != cell_num) {
      frag_meta_->set_tile_min_var_size(
          name, current_tile_idx_, tile.min().size());
      frag_meta_->set_tile_max_var_size(
          name, current_tile_idx_, tile.max().size());
    }
  } else {
    if (has_min_max_md && null_count != cell_num && !tile.min().empty()) {
      frag_meta_->set_tile_min(name, current_tile_idx_, tile.min());
      frag_meta_->set_tile_max(name, current_tile_idx_, tile.max());
    }

    if (has_sum_md) {
      frag_meta_->set_tile_sum(name, current_tile_idx_, tile.sum());
    }
  }

  // Write validity tile if nullable
  if (nullable) {
    auto& t_val = tile.validity_tile();
    resources_->vfs().write(
        validity_uri,
        t_val.filtered_buffer().data(),
        t_val.filtered_buffer().size());
    frag_meta_->set_tile_validity_offset(
        name, current_tile_idx_, t_val.filtered_buffer().size());
    frag_meta_->set_tile_null_count(name, current_tile_idx_, null_count);
  }

  current_tile_idx_++;
}

void ColumnFragmentWriter::close_field() {
  if (current_field_.empty()) {
    throw ColumnFragmentWriterException(
        "Cannot close field: no field is currently open");
  }

  const std::string& name = current_field_;
  const bool var_size = array_schema_->var_size(name);
  const bool nullable = array_schema_->is_nullable(name);

  // Close the file URIs
  URI uri = frag_meta_->uri(name);
  throw_if_not_ok(resources_->vfs().close_file(uri));

  if (var_size) {
    URI var_uri = frag_meta_->var_uri(name);
    throw_if_not_ok(resources_->vfs().close_file(var_uri));

    // Convert min/max var sizes to offsets
    const auto type = array_schema_->type(name);
    const auto is_dim = array_schema_->is_dim(name);
    const auto cell_val_num = array_schema_->cell_val_num(name);
    if (TileMetadataGenerator::has_min_max_metadata(
            type, is_dim, var_size, cell_val_num)) {
      frag_meta_->convert_tile_min_max_var_sizes_to_offsets(name);
    }
  }

  if (nullable) {
    URI validity_uri = frag_meta_->validity_uri(name);
    throw_if_not_ok(resources_->vfs().close_file(validity_uri));
  }

  // For sparse with dynamic growth, first closed field determines tile count.
  // Resize to actual count (doubling may have over-allocated).
  if (!dense_ && !first_field_closed_) {
    tile_num_ = current_tile_idx_;
    frag_meta_->set_num_tiles(tile_num_);
    first_field_closed_ = true;
  } else if (current_tile_idx_ != tile_num_) {
    throw ColumnFragmentWriterException(
        "Field '" + name + "' has " + std::to_string(current_tile_idx_) +
        " tiles but expected " + std::to_string(tile_num_));
  }

  current_field_.clear();
  current_tile_idx_ = 0;
}

void ColumnFragmentWriter::set_mbrs(std::vector<NDRange>&& mbrs) {
  if (dense_) {
    throw ColumnFragmentWriterException("Dense arrays should not provide MBRs");
  }

  if (mbrs.size() != tile_num_) {
    throw ColumnFragmentWriterException(
        "Sparse array requires " + std::to_string(tile_num_) +
        " MBRs but got " + std::to_string(mbrs.size()));
  }

  mbrs_set_ = true;

  // Set MBRs in fragment metadata immediately so caller can free memory
  for (uint64_t i = 0; i < mbrs.size(); ++i) {
    frag_meta_->set_mbr(i, mbrs[i]);
  }
}

/* ********************************* */
/*       FRAGMENT-LEVEL OPERATIONS   */
/* ********************************* */

void ColumnFragmentWriter::finalize(const EncryptionKey& encryption_key) {
  if (!current_field_.empty()) {
    throw ColumnFragmentWriterException(
        "Cannot finalize: field '" + current_field_ + "' is still open");
  }

  if (!dense_ && !mbrs_set_) {
    throw ColumnFragmentWriterException(
        "Cannot finalize sparse array without MBRs. Call set_mbrs() first.");
  }

  finalize_internal(encryption_key);
}

void ColumnFragmentWriter::finalize_internal(
    const EncryptionKey& encryption_key) {
  frag_meta_->compute_fragment_min_max_sum_null_count();
  frag_meta_->store(encryption_key);

  // Create commit file
  FragmentID frag_id{fragment_uri_};
  URI commit_uri;
  if (frag_id.array_format_version() >= 12) {
    commit_uri = array_schema_->array_uri()
                     .join_path(constants::array_commits_dir_name)
                     .join_path(frag_id.name() + constants::write_file_suffix);
  } else {
    commit_uri = URI(fragment_uri_.to_string() + constants::ok_file_suffix);
  }
  resources_->vfs().touch(commit_uri);
}

/* ********************************* */
/*             ACCESSORS             */
/* ********************************* */

const URI& ColumnFragmentWriter::fragment_uri() const {
  return fragment_uri_;
}

shared_ptr<FragmentMetadata> ColumnFragmentWriter::fragment_metadata() const {
  return frag_meta_;
}

/* ********************************* */
/*         PRIVATE METHODS           */
/* ********************************* */

void ColumnFragmentWriter::create_fragment_directory() {
  const URI& array_uri = array_schema_->array_uri();

  resources_->vfs().create_dir(
      array_uri.join_path(constants::array_fragments_dir_name));
  resources_->vfs().create_dir(fragment_uri_);
  resources_->vfs().create_dir(
      array_uri.join_path(constants::array_commits_dir_name));
}

}  // namespace tiledb::sm
